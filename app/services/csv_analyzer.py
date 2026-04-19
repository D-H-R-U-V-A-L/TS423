"""
csv_analyzer.py  –  OPTIMIZED v3
==================================
In-memory anomaly detection for DFO-uploaded CSV files.

Performance targets (50 000 rows):
  Pattern 1 – DECEASED          : O(n)   vectorized
  Pattern 2 – DUPLICATE IDENTITY: O(n log n)  groupby; no nested pair-loops
  Pattern 3 – UNDRAWN FUNDS     : O(n)   vectorized
  Pattern 4 – CROSS-SCHEME      : O(n log n)  groupby

Key design choices:
  • Duplicate-identity via phonetic bucket: only buckets with cluster_size
    ≤ MAX_PHONETIC_CLUSTER are emitted (large clusters = common name, not fraud).
  • Same-bank-account: capped at MAX_PAIRS_PER_GROUP comparisons.
  • Hard row-cap: ROW_LIMIT rows analysed; excess rows noted in errors[].
  • Undrawn-funds fallback: works on withdrawn==0 col with amount > 0.
  • Total anomaly result cap: MAX_ANOMALIES to keep JSON payload small.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any

import pandas as pd

from app.services.name_matcher import batch_normalize
from app.services.risk_scorer import calculate_risk


# ── Tunable constants ─────────────────────────────────────────────────────────
ROW_LIMIT            = 50_000   # max rows analysed
MAX_PAIRS_PER_GROUP  = 10       # max pair comparisons per bank-account bucket
MAX_PHONETIC_CLUSTER = 20       # buckets larger than this = common name → skip
MAX_ANOMALIES        = 5_000    # cap the result list to avoid huge JSON


# ── Column alias mapping (case-insensitive) ────────────────────────────────────
_ALIASES: dict[str, list[str]] = {
    "name":               ["name", "beneficiary_name", "full_name", "naam",
                            "applicant_name", "holder_name"],
    "aadhaar":            ["aadhaar", "aadhaar_number", "aadhar", "aadhar_number",
                            "uid", "aadhaar_no", "aadhar_no"],
    "bank_account_no":    ["bank_account_no", "account_no", "acc_no",
                            "account_number", "bank_acc", "bank_account"],
    "death_date":         ["death_date", "date_of_death", "dod", "death_dt"],
    "last_withdrawal_date": ["last_withdrawal_date", "last_withdrawal",
                              "last_txn_date", "last_transaction_date", "last_wd_date"],
    "current_balance":    ["current_balance", "balance", "account_balance", "bal"],
    "amount":             ["amount", "transfer_amount", "disbursement"],
    "scheme_id":          ["scheme_id", "scheme", "scheme_name", "scheme_code", "program"],
    "district":           ["district", "taluka", "block", "mandal", "zilla"],
    "is_deceased":        ["is_deceased", "deceased", "death_flag", "is_dead", "dead"],
    "withdrawn":          ["withdrawn", "is_withdrawn", "withdrawal_flag", "withdrawal_status"],
    "status":             ["status", "txn_status", "transaction_status"],
    "transaction_date":   ["transaction_date", "date", "txn_date", "transfer_date"],
}


def _detect_columns(df: pd.DataFrame) -> dict[str, str | None]:
    lower_cols = {c.strip().lower(): c for c in df.columns}
    mapping: dict[str, str | None] = {}
    for canonical, aliases in _ALIASES.items():
        found = None
        for alias in aliases:
            if alias in lower_cols:
                found = lower_cols[alias]
                break
        mapping[canonical] = found
    return mapping


def _display_row(row: dict, cols: dict) -> tuple[str, str, str]:
    name     = str(row.get(cols["name"],     "—") if cols["name"]     else "—")
    aadhaar  = str(row.get(cols["aadhaar"],  "—") if cols["aadhaar"]  else "—")
    district = str(row.get(cols["district"], "—") if cols["district"] else "—")
    return name, aadhaar, district


# ── Public entry point ────────────────────────────────────────────────────────

def analyze_csv(df: pd.DataFrame) -> dict[str, Any]:
    """
    Run all anomaly patterns on *df* in-memory (no DB access).
    Returns a JSON-serialisable dict.
    """
    cols          = _detect_columns(df)
    anomalies:    list[dict] = []
    errors:       list[str]  = []
    patterns_run: list[str]  = []
    trimmed       = False
    now           = datetime.now()

    total_rows_original = len(df)
    if len(df) > ROW_LIMIT:
        df      = df.iloc[:ROW_LIMIT].copy()
        trimmed = True
        errors.append(
            f"File has {total_rows_original:,} rows — analysis limited to "
            f"the first {ROW_LIMIT:,} rows for performance."
        )
    else:
        df = df.copy()

    df.columns = [c.strip() for c in df.columns]
    df["_row"] = range(1, len(df) + 1)   # 1-based row index for reporting

    def _add(a: dict) -> bool:
        """Add anomaly if under cap. Returns False when cap reached."""
        if len(anomalies) >= MAX_ANOMALIES:
            return False
        anomalies.append(a)
        return True

    # =========================================================================
    # PATTERN 1 – DECEASED BENEFICIARY   O(n) vectorized
    # =========================================================================
    dod_col      = cols["death_date"]
    deceased_col = cols["is_deceased"]

    if dod_col or deceased_col:
        patterns_run.append("DECEASED")
        try:
            if dod_col:
                dod       = pd.to_datetime(df[dod_col], errors="coerce")
                hits      = df[dod.notna()].copy()
                hits_dod  = dod[dod.notna()]
                for idx, row in hits.iterrows():
                    row    = row.to_dict()
                    name, aadhaar, district = _display_row(row, cols)
                    dod_str = hits_dod[idx].strftime("%Y-%m-%d")
                    score, evidence = calculate_risk("DECEASED", {"death_date": dod_str})
                    if not _add({"row_index": int(row["_row"]), "anomaly_type": "DECEASED",
                                 "risk_score": score, "name": name, "aadhaar": aadhaar,
                                 "district": district, "evidence": evidence}):
                        break
            else:
                dead_mask = (
                    df[deceased_col].astype(str).str.strip().str.lower()
                    .isin(["true", "1", "yes", "y"])
                )
                for _, row in df[dead_mask].iterrows():
                    row = row.to_dict()
                    name, aadhaar, district = _display_row(row, cols)
                    score, evidence = calculate_risk("DECEASED", {"death_date": "flagged"})
                    if not _add({"row_index": int(row["_row"]), "anomaly_type": "DECEASED",
                                 "risk_score": score, "name": name, "aadhaar": aadhaar,
                                 "district": district, "evidence": evidence}):
                        break
        except Exception as exc:
            errors.append(f"DECEASED pattern error: {exc}")

    # =========================================================================
    # PATTERN 2 – DUPLICATE IDENTITY   O(n log n) groupby
    # =========================================================================
    name_col    = cols["name"]
    aadhaar_col = cols["aadhaar"]
    bank_col    = cols["bank_account_no"]

    if name_col and (aadhaar_col or bank_col):
        patterns_run.append("DUPLICATE_IDENTITY")
        try:
            # One-shot vectorized phonetic normalisation
            df["_name_norm"] = batch_normalize(df[name_col].fillna("").astype(str))
            seen_row_pairs: set[tuple] = set()

            # ── 2a. Same bank account (capped nested loop, small groups only) ─
            if bank_col:
                valid_bank = df[df[bank_col].notna() & (df[bank_col].astype(str).str.strip() != "")]
                bank_sizes = valid_bank.groupby(bank_col)["_row"].count()
                # Only 2–20 per account (>20 is likely a shared paypoint, not fraud)
                multi_banks = bank_sizes[(bank_sizes >= 2) & (bank_sizes <= 20)].index

                for bank_acc in multi_banks:
                    if len(anomalies) >= MAX_ANOMALIES:
                        break
                    grp  = df[df[bank_col] == bank_acc]
                    rows = grp.to_dict("records")
                    pairs_added = 0
                    for i in range(len(rows)):
                        if pairs_added >= MAX_PAIRS_PER_GROUP:
                            break
                        for j in range(i + 1, len(rows)):
                            if pairs_added >= MAX_PAIRS_PER_GROUP:
                                break
                            r1, r2 = rows[i], rows[j]
                            if aadhaar_col and (
                                str(r1.get(aadhaar_col, "X")) == str(r2.get(aadhaar_col, "Y"))
                            ):
                                continue
                            pair_key = tuple(sorted([r1["_row"], r2["_row"]]))
                            if pair_key in seen_row_pairs:
                                continue
                            seen_row_pairs.add(pair_key)
                            pairs_added += 1

                            name1 = str(r1.get(name_col, ""))
                            name2 = str(r2.get(name_col, ""))
                            n, aad, dist = _display_row(r1, cols)
                            ev = {
                                "name_1":       name1,
                                "name_2":       name2,
                                "match_method": "same_bank_account",
                                "bank_account": str(bank_acc),
                                "similarity":   95,
                            }
                            score, evidence = calculate_risk("DUPLICATE_IDENTITY", ev)
                            _add({"row_index": int(r1["_row"]), "anomaly_type": "DUPLICATE_IDENTITY",
                                  "risk_score": score, "name": n, "aadhaar": aad,
                                  "district": dist, "evidence": evidence})

            # ── 2b. Phonetic-bucket scan: flag buckets that are SMALL ────────
            #   • Small bucket (2–MAX_PHONETIC_CLUSTER unique Aadhaar) = suspicious clone
            #   • Large bucket (> threshold) = common name variant in population = skip
            #   • One alert per unique Aadhaar in the bucket (NOT one per pair)
            if aadhaar_col:
                norm_valid = df[df["_name_norm"].str.strip() != ""]
                for norm_key, grp in norm_valid.groupby("_name_norm", sort=False):
                    if len(anomalies) >= MAX_ANOMALIES:
                        break
                    if len(grp) < 2:
                        continue
                    unique_aadh_count = grp[aadhaar_col].astype(str).nunique(dropna=False)
                    if unique_aadh_count < 2:
                        continue
                    # Skip very large clusters — these are common names, not fraud
                    if unique_aadh_count > MAX_PHONETIC_CLUSTER:
                        continue

                    all_names = grp[name_col].dropna().unique().tolist()
                    first_per_aadh = (
                        grp.groupby(grp[aadhaar_col].astype(str), sort=False)
                        .first()
                        .reset_index(drop=True)
                    )

                    for _, row in first_per_aadh.iterrows():
                        if len(anomalies) >= MAX_ANOMALIES:
                            break
                        row = row.to_dict()
                        name, aadhaar, district = _display_row(row, cols)
                        ev = {
                            "match_method":  "gujarati_phonetic_bucket",
                            "phonetic_key":  str(norm_key),
                            "similar_names": [str(n) for n in all_names[:5]],
                            "cluster_size":  int(unique_aadh_count),
                            "similarity":    95,
                        }
                        score, evidence = calculate_risk("DUPLICATE_IDENTITY", ev)
                        _add({"row_index": int(row["_row"]), "anomaly_type": "DUPLICATE_IDENTITY",
                              "risk_score": score, "name": name, "aadhaar": aadhaar,
                              "district": district, "evidence": evidence})

        except Exception as exc:
            errors.append(f"DUPLICATE_IDENTITY pattern error: {exc}")

    # =========================================================================
    # PATTERN 3 – UNDRAWN FUNDS   O(n) vectorized
    # =========================================================================
    lwd_col       = cols["last_withdrawal_date"]
    bal_col       = cols["current_balance"]
    withdrawn_col = cols["withdrawn"]
    amount_col    = cols["amount"]

    if lwd_col and bal_col:
        # Full undrawn funds: explicit last-withdrawal date + balance
        patterns_run.append("UNDRAWN_FUNDS")
        try:
            lwd = pd.to_datetime(df[lwd_col], errors="coerce")
            bal = pd.to_numeric(df[bal_col], errors="coerce").fillna(0)

            def months_ago(d):
                if pd.isna(d):
                    return 0
                return (now.year - d.year) * 12 + now.month - d.month

            months_stagnant = lwd.apply(months_ago)
            mask = (months_stagnant >= 24) & (bal >= 40_000)
            for idx in df[mask].index:
                row = df.loc[idx].to_dict()
                name, aadhaar, district = _display_row(row, cols)
                ev = {
                    "months_stagnant": int(months_stagnant[idx]),
                    "current_balance": round(float(bal[idx]), 2),
                }
                score, evidence = calculate_risk("UNDRAWN_FUNDS", ev)
                if not _add({"row_index": int(row["_row"]), "anomaly_type": "UNDRAWN_FUNDS",
                              "risk_score": score, "name": name, "aadhaar": aadhaar,
                              "district": district, "evidence": evidence}):
                    break
        except Exception as exc:
            errors.append(f"UNDRAWN_FUNDS pattern error: {exc}")

    elif withdrawn_col:
        # Fallback: withdrawn==0 — flag any undrawn transfer with amount > ₹500
        # Only flag SUCCESS transactions (FAILED = never credited, not a ghost-funds issue).
        patterns_run.append("UNDRAWN_FUNDS")
        try:
            wdr_raw = df[withdrawn_col]
            if pd.api.types.is_numeric_dtype(wdr_raw):
                not_withdrawn = wdr_raw == 0
            else:
                wdr_str = wdr_raw.astype(str).str.strip().str.lower()
                not_withdrawn = wdr_str.isin(["0", "false", "no", "n"])

            # Restrict to successful (credited) transactions only
            status_col_detected = cols.get("status")
            if status_col_detected:
                success_mask = df[status_col_detected].astype(str).str.strip().str.upper() == "SUCCESS"
                not_withdrawn = not_withdrawn & success_mask

            use_amount_col = amount_col or bal_col
            if use_amount_col:
                amt  = pd.to_numeric(df[use_amount_col], errors="coerce").fillna(0)
                # Flag any undrawn transfer > ₹500 (covers DBT amounts of ₹1k–₹5k)
                mask = not_withdrawn & (amt > 500)
            else:
                amt  = pd.Series([0] * len(df), index=df.index)
                mask = not_withdrawn

            for idx in df[mask].index:
                row = df.loc[idx].to_dict()
                name, aadhaar, district = _display_row(row, cols)
                bal_val = float(amt[idx])
                ev = {
                    "months_stagnant": 0,
                    "current_balance": round(bal_val, 2),
                    "note": "withdrawn=0; funds credited (SUCCESS) but not withdrawn",
                }
                score, evidence = calculate_risk("UNDRAWN_FUNDS", ev)
                if not _add({"row_index": int(row["_row"]), "anomaly_type": "UNDRAWN_FUNDS",
                              "risk_score": score, "name": name, "aadhaar": aadhaar,
                              "district": district, "evidence": evidence}):
                    break
        except Exception as exc:
            errors.append(f"UNDRAWN_FUNDS (withdrawn flag) pattern error: {exc}")



    # =========================================================================
    # PATTERN 4 – CROSS-SCHEME DUPLICATION   O(n log n) groupby
    # =========================================================================
    scheme_col = cols["scheme_id"]

    if scheme_col and aadhaar_col:
        patterns_run.append("CROSS_SCHEME")
        try:
            scheme_sets = (
                df.groupby(df[aadhaar_col].astype(str))[scheme_col]
                .agg(lambda x: set(x.astype(str).str.strip()))
                .reset_index()
            )
            scheme_sets.columns = ["_aadh", "_schemes"]

            # Scheme-agnostic: flag any Aadhaar appearing in >= 2 *distinct* schemes.
            # This works with any scheme naming (PM-KISAN, Pension, Scholarship, etc.)
            # as opposed to hardcoded scheme names that may not match the uploaded file.
            cross_aadh = set(
                scheme_sets.loc[
                    scheme_sets["_schemes"].apply(lambda s: len(s) >= 2), "_aadh"
                ]
            )

            if cross_aadh:
                hit_df = df[df[aadhaar_col].astype(str).isin(cross_aadh)]
                first_hits = (
                    hit_df.groupby(hit_df[aadhaar_col].astype(str), sort=False)
                    .first()
                    .reset_index(drop=True)
                )

                for _, row in first_hits.iterrows():
                    row = row.to_dict()
                    name, aadhaar, district = _display_row(row, cols)
                    schemes_for_aadh = list(
                        df[df[aadhaar_col].astype(str) == aadhaar][scheme_col]
                        .astype(str).unique()
                    )
                    ev = {"schemes": schemes_for_aadh}
                    score, evidence = calculate_risk("CROSS_SCHEME", ev)
                    if not _add({"row_index": int(row["_row"]), "anomaly_type": "CROSS_SCHEME",
                                  "risk_score": score, "name": name, "aadhaar": aadhaar,
                                  "district": district, "evidence": evidence}):
                        break
        except Exception as exc:
            errors.append(f"CROSS_SCHEME pattern error: {exc}")

    # ── Summary ───────────────────────────────────────────────────────────────
    summary: dict[str, int] = {}
    for a in anomalies:
        summary[a["anomaly_type"]] = summary.get(a["anomaly_type"], 0) + 1

    anomalies.sort(key=lambda x: x["risk_score"], reverse=True)

    if len(anomalies) >= MAX_ANOMALIES:
        errors.append(
            f"Result capped at {MAX_ANOMALIES:,} anomalies. "
            "Export to CSV for the full list."
        )

    return {
        "total_rows":            len(df),
        "total_rows_original":   total_rows_original,
        "trimmed":               trimmed,
        "anomalies":             anomalies,
        "summary":               summary,
        "columns_detected":      cols,
        "patterns_run":          patterns_run,
        "errors":                errors,
    }
