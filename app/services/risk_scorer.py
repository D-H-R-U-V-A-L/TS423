def calculate_risk(leakage_type, specific_evidence_dict):
    """
    Calculates a risk score (0-100) based on the leakage type and evidence.
    """
    score = 0
    evidence_str = ""

    if leakage_type == 'DECEASED':
        score = 100
        death_date = specific_evidence_dict.get('death_date', 'Unknown')
        evidence_str = f"CRITICAL: Beneficiary ID matched deceased registry. Recorded death: {death_date}."
    
    elif leakage_type == 'DUPLICATE_IDENTITY':
        similarity = specific_evidence_dict.get('similarity', 0)
        score = min(95, max(60, int(similarity)))
        name_1 = specific_evidence_dict.get('name_1', '')
        name_2 = specific_evidence_dict.get('name_2', '')
        evidence_str = f"Fuzzy Name Match ({similarity}%): '{name_1}' & '{name_2}' share same Bank Account but different Aadhaar IDs."
    
    elif leakage_type == 'UNDRAWN_FUNDS':
        balance = specific_evidence_dict.get('current_balance', 0)
        months_stagnant = specific_evidence_dict.get('months_stagnant', 0)
        # Score increases with balance and stagnation
        score = min(90, 50 + int(months_stagnant * 0.5) + int(balance / 5000))
        evidence_str = f"Account stagnant for {months_stagnant} months with high accumulated balance of ₹{balance:.2f}."
    
    elif leakage_type == 'CROSS_SCHEME':
        schemes = specific_evidence_dict.get('schemes', [])
        score = 85
        evidence_str = f"Beneficiary received mutually exclusive schemes or anomalous volume: {', '.join(schemes)}."
    
    else:
        score = 50
        evidence_str = "Suspicious activity detected."

    return score, evidence_str
