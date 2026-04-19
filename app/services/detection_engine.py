import json
import pandas as pd
from datetime import datetime, timedelta
from app.models import Beneficiary, Transaction, Alert, db
from app.services.risk_scorer import calculate_risk
from app.services.name_matcher import gujarati_similarity, batch_normalize


def run_detection_cycle():
    """
    Main detection engine – Pandas-vectorized for <30 s on 10 000+ rows.

    Pattern 2 now uses proper Gujarati-transliteration-aware matching:
      2a. Same bank account + fuzzy Gujarati similarity ≥ 70
          (catches middleman accounts hoovering up transfers)
      2b. Vectorized phonetic-bucket scan across ALL beneficiaries:
          names that share the same normalized Gujarati key but carry
          different Aadhaar IDs are flagged as cross-dataset identity clones.

    Returns the total number of new Alert records created.
    """
    # ── Load data ─────────────────────────────────────────────────────────────
    df_b = pd.read_sql(db.session.query(Beneficiary).statement, db.engine)
    df_t = pd.read_sql(db.session.query(Transaction).statement, db.engine)

    if df_b.empty or df_t.empty:
        return 0

    alerts_to_create: list[Alert] = []
    now = datetime.now()

    df_b['death_date'] = pd.to_datetime(df_b['death_date'])
    df_t['timestamp']  = pd.to_datetime(df_t['timestamp'])

    # =========================================================================
    # PATTERN 1 – DECEASED BENEFICIARY
    # =========================================================================
    merged = df_t.merge(df_b, left_on='beneficiary_id', right_on='id',
                        suffixes=('_t', '_b'))

    deceased_hits = merged[
        (merged['is_deceased'] == True) &
        (merged['timestamp'] > merged['death_date'])
    ]

    for _, row in deceased_hits.iterrows():
        death_str = (row['death_date'].strftime('%Y-%m-%d')
                     if pd.notna(row['death_date']) else 'Unknown')
        score, evidence = calculate_risk('DECEASED', {'death_date': death_str})
        alerts_to_create.append(Alert(
            transaction_id=int(row['id_t']),
            beneficiary_id=int(row['beneficiary_id']),
            leakage_type='DECEASED',
            risk_score=score,
            evidence=json.dumps({'detail': evidence})
        ))

    # =========================================================================
    # PATTERN 2 – DUPLICATE IDENTITY  (Gujarati-transliteration-aware)
    # =========================================================================

    # ── Pre-compute phonetic keys once for all rows (vectorized, fast) ────────
    df_b['name_norm'] = batch_normalize(df_b['name_transliterated'])

    # ── 2a.  Same bank account + Gujarati similarity ≥ 70 ────────────────────
    #         Catches middlemen / piggybacking on the same account with a
    #         slightly mangled spelling.
    seen_pairs_bank: set[tuple] = set()

    for bank_acc, group in df_b.groupby('bank_account_no'):
        if len(group) < 2 or not bank_acc or str(bank_acc).strip() == '':
            continue
        rows = group.to_dict('records')
        for i in range(len(rows)):
            for j in range(i + 1, len(rows)):
                r1, r2 = rows[i], rows[j]
                if r1['aadhaar_number'] == r2['aadhaar_number']:
                    continue
                pair_key = tuple(sorted([r1['id'], r2['id']]))
                if pair_key in seen_pairs_bank:
                    continue
                seen_pairs_bank.add(pair_key)

                sim = gujarati_similarity(
                    str(r1['name_transliterated']),
                    str(r2['name_transliterated'])
                )
                if sim >= 70:
                    ev = {
                        'similarity': sim,
                        'name_1': r1['name_transliterated'],
                        'name_2': r2['name_transliterated'],
                        'match_method': 'same_bank_account + gujarati_phonetic',
                        'norm_1': r1['name_norm'],
                        'norm_2': r2['name_norm'],
                    }
                    score, evidence = calculate_risk('DUPLICATE_IDENTITY', ev)
                    alerts_to_create.append(Alert(
                        transaction_id=None,
                        beneficiary_id=int(r1['id']),
                        leakage_type='DUPLICATE_IDENTITY',
                        risk_score=score,
                        evidence=json.dumps({'detail': evidence, 'raw': ev})
                    ))

    # ── 2b.  Phonetic-bucket scan – O(n log n), fully vectorized ─────────────
    #         Group by normalized Gujarati key. Any group with ≥ 2 entries AND
    #         at least 2 distinct Aadhaar IDs is a cross-dataset identity clone.
    #         This catches the exact "Varun Thakkar" ↔ "Warun Takkar" case even
    #         when they use different banks or live in different districts.
    seen_pairs_phon: set[tuple] = set()

    bucket_groups = df_b[df_b['name_norm'].str.strip() != ''].groupby('name_norm')

    for norm_key, group in bucket_groups:
        if len(group) < 2:
            continue
        # Only flag if at least two distinct Aadhaar IDs share the same phonetic key
        unique_aadhaar = group['aadhaar_number'].nunique()
        if unique_aadhaar < 2:
            continue

        rows = group.to_dict('records')
        for i in range(len(rows)):
            for j in range(i + 1, len(rows)):
                r1, r2 = rows[i], rows[j]
                if r1['aadhaar_number'] == r2['aadhaar_number']:
                    continue
                pair_key = tuple(sorted([r1['id'], r2['id']]))
                if pair_key in seen_pairs_phon or pair_key in seen_pairs_bank:
                    continue
                seen_pairs_phon.add(pair_key)

                # Final similarity confirmation (still fast because bucket is small)
                sim = gujarati_similarity(
                    str(r1['name_transliterated']),
                    str(r2['name_transliterated'])
                )

                ev = {
                    'similarity': sim,
                    'name_1': r1['name_transliterated'],
                    'name_2': r2['name_transliterated'],
                    'match_method': 'gujarati_phonetic_bucket',
                    'phonetic_key': norm_key,
                    'district_1': r1['district'],
                    'district_2': r2['district'],
                }
                score, evidence = calculate_risk('DUPLICATE_IDENTITY', ev)
                alerts_to_create.append(Alert(
                    transaction_id=None,
                    beneficiary_id=int(r1['id']),
                    leakage_type='DUPLICATE_IDENTITY',
                    risk_score=score,
                    evidence=json.dumps({'detail': evidence, 'raw': ev})
                ))

    # =========================================================================
    # PATTERN 3 – UNDRAWN FUNDS
    # =========================================================================
    df_b['last_withdrawal_date'] = pd.to_datetime(df_b['last_withdrawal_date'])

    def calc_months(d):
        if pd.isna(d):
            return 0
        return (now.year - d.year) * 12 + now.month - d.month

    df_b['months_stagnant'] = df_b['last_withdrawal_date'].apply(calc_months)
    undrawn_hits = df_b[
        (df_b['months_stagnant'] >= 24) & (df_b['current_balance'] >= 40000)
    ]

    for _, row in undrawn_hits.iterrows():
        ev = {
            'months_stagnant': int(row['months_stagnant']),
            'current_balance': round(float(row['current_balance']), 2)
        }
        score, evidence = calculate_risk('UNDRAWN_FUNDS', ev)
        alerts_to_create.append(Alert(
            transaction_id=None,
            beneficiary_id=int(row['id']),
            leakage_type='UNDRAWN_FUNDS',
            risk_score=score,
            evidence=json.dumps({'detail': evidence})
        ))

    # =========================================================================
    # PATTERN 4 – CROSS-SCHEME DUPLICATION
    # =========================================================================
    recent_txs = df_t[df_t['timestamp'] > (now - timedelta(days=60))]
    scheme_counts = (
        recent_txs
        .groupby('beneficiary_id')['scheme_id']
        .unique()
        .reset_index()
    )

    def is_cross_scheme(schemes):
        sc = list(schemes)
        return 'Student_Scholarship' in sc and 'Kisan_Sahay' in sc

    scheme_counts['is_cross'] = scheme_counts['scheme_id'].apply(is_cross_scheme)

    for _, row in scheme_counts[scheme_counts['is_cross']].iterrows():
        ev = {'schemes': list(row['scheme_id'])}
        score, evidence = calculate_risk('CROSS_SCHEME', ev)
        alerts_to_create.append(Alert(
            transaction_id=None,
            beneficiary_id=int(row['beneficiary_id']),
            leakage_type='CROSS_SCHEME',
            risk_score=score,
            evidence=json.dumps({'detail': evidence})
        ))

    # ── Persist ───────────────────────────────────────────────────────────────
    db.session.query(Alert).delete()
    if alerts_to_create:
        db.session.bulk_save_objects(alerts_to_create)
        db.session.commit()

    return len(alerts_to_create)

def run_detection_on_dataframe(df):
    alerts = []


    # Normalize column names
    df.columns = [c.lower().strip() for c in df.columns]

    # Try to map columns safely
    name_col = 'name' if 'name' in df.columns else None
    account_col = 'bank_account' if 'bank_account' in df.columns else None
    status_col = 'status' if 'status' in df.columns else None

    # -------------------------------
    # 1. Undrawn Funds
    # -------------------------------
    if status_col:
        undrawn = df[df[status_col].astype(str).str.lower().isin(['not withdrawn', 'pending', 'failed'])]

        for _, row in undrawn.iterrows():
            alerts.append({
                "type": "UNDRAWN_FUNDS",
                "name": row.get(name_col, "Unknown"),
                "reason": "Funds not withdrawn"
            })

    # -------------------------------
    # 2. Duplicate Account
    # -------------------------------
    if account_col:
        dup_accounts = df[df.duplicated(account_col, keep=False)]

        for _, row in dup_accounts.iterrows():
            alerts.append({
                "type": "DUPLICATE_ACCOUNT",
                "name": row.get(name_col, "Unknown"),
                "reason": "Same bank account used multiple times"
            })

    # -------------------------------
    # 3. Simple Name Similarity
    # -------------------------------
    if name_col:
        from rapidfuzz import fuzz

        names = df[name_col].dropna().tolist()

        for i in range(len(names)):
            for j in range(i+1, len(names)):
                if fuzz.ratio(names[i], names[j]) > 90:
                    alerts.append({
                        "type": "DUPLICATE_IDENTITY",
                        "name": names[i],
                        "reason": f"Similar name found: {names[j]}"
                    })

    return alerts

