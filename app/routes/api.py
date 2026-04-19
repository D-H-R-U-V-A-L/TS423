from flask import Blueprint, jsonify, request
from flask_login import login_required, current_user
from ..models import Alert, Verification, db, ROLE_DFO, ROLE_VERIFIER, ROLE_ADMIN, ROLE_AUDIT
from ..services.detection_engine import run_detection_cycle

bp = Blueprint('api', __name__)


def api_role_required(*roles):
    """Returns a 403 JSON error if the current user's role is not allowed."""
    from functools import wraps
    def decorator(f):
        @wraps(f)
        @login_required
        def wrapped(*args, **kwargs):
            if current_user.role not in roles:
                return jsonify({"error": "Forbidden – insufficient role"}), 403
            return f(*args, **kwargs)
        return wrapped
    return decorator


# ── Alert list (DFO + Audit can read) ────────────────────────────────────────
@bp.route('/alerts', methods=['GET'])
@api_role_required(ROLE_DFO, ROLE_ADMIN, ROLE_AUDIT)
def get_alerts():
    alerts = Alert.query.order_by(Alert.risk_score.desc()).all()
    return jsonify([a.to_dict() for a in alerts])


# ── Run detection engine (Admin only) ────────────────────────────────────────
@bp.route('/run_detection', methods=['POST'])
@api_role_required(ROLE_DFO, ROLE_ADMIN)
def run_detection():
    import time
    start = time.time()
    num_alerts = run_detection_cycle()
    elapsed = round(time.time() - start, 4)
    return jsonify({
        "status": "success",
        "alerts_generated": num_alerts,
        "time_taken_seconds": elapsed
    })


# ── Field verification submit (Verifier only) ─────────────────────────────────
@bp.route('/alerts/<int:alert_id>/verify', methods=['POST'])
@api_role_required(ROLE_VERIFIER)
def verify_alert(alert_id):
    data = request.json
    alert = Alert.query.get_or_404(alert_id)

    ver = Verification(
        alert_id=alert.id,
        verifier_id=str(current_user.id),
        latitude=data.get('latitude'),
        longitude=data.get('longitude'),
        comments=data.get('comments', '')
    )
    alert.status = data.get('status', 'VERIFIED')
    db.session.add(ver)
    db.session.commit()

    return jsonify({"status": "success", "message": "Verification submitted successfully."})


# ── Assign alert to a verifier (DFO only) ────────────────────────────────────
@bp.route('/alerts/<int:alert_id>/assign', methods=['POST'])
@api_role_required(ROLE_DFO)
def assign_alert(alert_id):
    alert = Alert.query.get_or_404(alert_id)
    alert.status = 'ASSIGNED'
    db.session.commit()
    return jsonify({"status": "success", "message": "Case assigned."})


# ── Gujarati name-match live demo ─────────────────────────────────────────────
@bp.route('/name-match', methods=['POST'])
@api_role_required(ROLE_DFO, ROLE_ADMIN, ROLE_AUDIT)
def name_match():
    """
    POST { "name_a": "Varun Thakkar", "name_b": "Warun Takkar" }
    Returns similarity score, normalized forms, and a human-readable verdict.
    """
    from ..services.name_matcher import gujarati_similarity, normalize
    data = request.json or {}
    name_a = str(data.get('name_a', '')).strip()
    name_b = str(data.get('name_b', '')).strip()

    if not name_a or not name_b:
        return jsonify({"error": "Both name_a and name_b are required."}), 400

    score   = gujarati_similarity(name_a, name_b)
    norm_a  = normalize(name_a)
    norm_b  = normalize(name_b)
    exact   = (norm_a == norm_b)

    if score == 100 and exact:
        verdict = "EXACT PHONETIC MATCH — same person with transliteration variant"
    elif score >= 85:
        verdict = "HIGH CONFIDENCE MATCH — very likely same person"
    elif score >= 70:
        verdict = "PROBABLE MATCH — warrants manual review"
    elif score >= 50:
        verdict = "WEAK MATCH — possibly same person, needs investigation"
    else:
        verdict = "NO MATCH"

    return jsonify({
        "name_a":         name_a,
        "name_b":         name_b,
        "normalized_a":   norm_a,
        "normalized_b":   norm_b,
        "exact_phonetic": exact,
        "score":          score,
        "verdict":        verdict,
    })


@bp.route('/chart-data', methods=['GET'])
@api_role_required(ROLE_DFO, ROLE_ADMIN, ROLE_AUDIT)
def chart_data():
    """
    Returns live aggregated data for all dashboard charts:
      - leakage_type_counts   → { label: count, … }
      - district_counts       → { district: count, … }
      - risk_tier_counts      → { critical, high, medium, low }
      - daily_trend           → last 7 days alert count per day
      - total_alerts          → int
    """
    from collections import defaultdict
    from datetime import datetime, timedelta
    from sqlalchemy import func

    alerts = Alert.query.all()

    # ── Leakage type breakdown ─────────────────────────────────────────────────
    type_counts = defaultdict(int)
    for a in alerts:
        label = (a.leakage_type or 'UNKNOWN').replace('_', ' ').title()
        type_counts[label] += 1

    # ── Per-district flagged cases ─────────────────────────────────────────────
    from ..models import Beneficiary
    district_counts = defaultdict(int)
    for a in alerts:
        b = a.beneficiary
        district = (b.district if b else 'Unknown') or 'Unknown'
        district_counts[district] += 1

    # ── Risk tier counts ───────────────────────────────────────────────────────
    risk_tiers = {'Critical (90-100)': 0, 'High (75-89)': 0, 'Medium (50-74)': 0, 'Low (<50)': 0}
    for a in alerts:
        if a.risk_score >= 90:
            risk_tiers['Critical (90-100)'] += 1
        elif a.risk_score >= 75:
            risk_tiers['High (75-89)'] += 1
        elif a.risk_score >= 50:
            risk_tiers['Medium (50-74)'] += 1
        else:
            risk_tiers['Low (<50)'] += 1

    # ── 7-day daily trend ──────────────────────────────────────────────────────
    today = datetime.utcnow().date()
    daily_trend = {}
    for i in range(6, -1, -1):
        day = today - timedelta(days=i)
        daily_trend[day.strftime('%b %d')] = 0
    for a in alerts:
        if a.created_at:
            day_key = a.created_at.date().strftime('%b %d')
            if day_key in daily_trend:
                daily_trend[day_key] += 1

    return jsonify({
        'total_alerts':       len(alerts),
        'leakage_type_counts': dict(type_counts),
        'district_counts':     dict(district_counts),
        'risk_tier_counts':    risk_tiers,
        'daily_trend':         daily_trend,
    })



# ── DFO CSV Upload & In-Memory Anomaly Analysis ───────────────────────────────
@bp.route('/analyze-csv', methods=['POST'])
@api_role_required(ROLE_DFO, ROLE_ADMIN)
def analyze_csv():
    """
    POST multipart/form-data  { file: <csv_file> }

    Accepts a CSV (max 10 MB), runs all anomaly patterns in-memory
    (no DB writes), and returns the full anomaly report as JSON.
    """
    import io
    import pandas as pd
    from ..services.csv_analyzer import analyze_csv as _analyze

    if 'file' not in request.files:
        return jsonify({"error": "No file uploaded. Send `file` as multipart form-data."}), 400

    f = request.files['file']
    if not f.filename:
        return jsonify({"error": "Empty filename."}), 400

    # Allow .csv and plain text (some tools export without extension)
    allowed = {'.csv', '.txt', '.tsv'}
    import os
    ext = os.path.splitext(f.filename)[1].lower()
    if ext not in allowed and f.mimetype not in ('text/csv', 'text/plain', 'application/csv'):
        return jsonify({"error": f"Unsupported file type '{ext}'. Upload a .csv file."}), 400

    # 10 MB guard
    raw = f.read()
    if len(raw) > 10 * 1024 * 1024:
        return jsonify({"error": "File too large. Maximum size is 10 MB."}), 413

    try:
        sep = '\t' if ext == '.tsv' else ','
        df = pd.read_csv(io.BytesIO(raw), sep=sep, low_memory=False)
    except Exception as exc:
        return jsonify({"error": f"Could not parse CSV: {exc}"}), 422

    if df.empty:
        return jsonify({"error": "The uploaded CSV has no rows."}), 422

    result = _analyze(df)
    return jsonify(result)


# ── Import CSV persons into the investigation queue ───────────────────────────
@bp.route('/import-csv-persons', methods=['POST'])
@api_role_required(ROLE_DFO, ROLE_ADMIN)
def import_csv_persons():
    """
    POST JSON body: { persons: [ ... ] }
    Each person: {
        name, aadhaar, district,
        anomaly_types: [...],
        evidences: [ { type, text } ],
        risk_score: int,
        row_indices: [...]
    }

    For each person:
      1. Finds or creates a Beneficiary (upsert by aadhaar_number).
      2. Creates one Alert per anomaly_type.

    Returns { status, imported, alert_ids }.
    """
    import json as _json
    from ..models import Beneficiary, Alert, db

    body    = request.json or {}
    persons = body.get('persons', [])

    if not persons:
        return jsonify({"error": "No persons provided."}), 400
    if len(persons) > 5000:
        return jsonify({"error": "Too many persons — max 5 000 per import."}), 400

    imported  = 0
    alert_ids = []

    try:
        for p in persons:
            aadhaar  = str(p.get('aadhaar', '') or '').strip()
            name     = str(p.get('name',    '') or '').strip() or 'Unknown'
            district = str(p.get('district','') or '').strip() or None

            # ── Upsert Beneficiary ────────────────────────────────────────────
            ben = None
            if aadhaar and aadhaar not in ('—', 'nan', 'None'):
                ben = Beneficiary.query.filter_by(aadhaar_number=aadhaar).first()

            if not ben:
                ben = Beneficiary(
                    aadhaar_number       = aadhaar or None,
                    name_english         = name,
                    name_transliterated  = name,
                    district             = district,
                    bank_account_no      = None,
                    ifsc_code            = None,
                    is_deceased          = False,
                    death_date           = None,
                    last_withdrawal_date = None,
                    current_balance      = 0.0,
                )
                db.session.add(ben)
                db.session.flush()   # get ben.id

            # ── Create one Alert per anomaly type ─────────────────────────────
            anomaly_types = p.get('anomaly_types') or []
            evidences     = p.get('evidences')     or []
            risk_score    = int(p.get('risk_score', 50))
            row_indices   = p.get('row_indices')   or []

            ev_map = {}
            for e in evidences:
                if isinstance(e, dict):
                    ev_map[e.get('type', '')] = e.get('text', '')

            for atype in anomaly_types:
                ev_text = ev_map.get(atype) or f"Imported from CSV upload. Row(s): {row_indices}"
                alert = Alert(
                    transaction_id = None,
                    beneficiary_id = ben.id,
                    leakage_type   = atype,
                    risk_score     = risk_score,
                    evidence       = _json.dumps({
                        'detail':      ev_text,
                        'source':      'csv_upload',
                        'row_indices': row_indices,
                    }),
                    status         = 'PENDING',
                )
                db.session.add(alert)
                db.session.flush()
                alert_ids.append(alert.id)

            imported += 1

        db.session.commit()
    except Exception as exc:
        db.session.rollback()
        return jsonify({"error": f"Import failed: {exc}"}), 500

    return jsonify({
        "status":    "success",
        "imported":  imported,
        "alert_ids": alert_ids,
    })


@bp.route('/name-match/demo', methods=['GET'])
@api_role_required(ROLE_DFO, ROLE_ADMIN, ROLE_AUDIT)
def name_match_demo():
    """
    Returns pre-scored results for all DEMO_PAIRS — instant hackathon showcase.
    """
    from ..services.name_matcher import gujarati_similarity, normalize, DEMO_PAIRS
    results = []
    for name_a, name_b, rule in DEMO_PAIRS:
        score  = gujarati_similarity(name_a, name_b)
        norm_a = normalize(name_a)
        norm_b = normalize(name_b)
        results.append({
            "name_a":         name_a,
            "name_b":         name_b,
            "rule_demo":      rule,
            "normalized_a":   norm_a,
            "normalized_b":   norm_b,
            "exact_phonetic": norm_a == norm_b,
            "score":          score,
        })
    return jsonify(results)

from flask import request
import pandas as pd

@bp.route('/upload_csv', methods=['POST'])
def upload_csv():
    try:
        if 'file' not in request.files:
            return {"error": "No file uploaded"}, 400


        file = request.files['file']

        df = pd.read_csv(file)

        # 🔥 IMPORTANT: normalize column names
        df.columns = [col.lower().strip() for col in df.columns]

        # Store temporarily (global or session)
        from app.services.detection_engine import run_detection_on_dataframe

        alerts = run_detection_on_dataframe(df)

        return {
            "status": "success",
            "alerts_generated": len(alerts),
            "alerts": alerts[:50]  # limit
        }

    except Exception as e:
        return {"error": str(e)}, 500

