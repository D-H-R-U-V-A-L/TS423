# DBT Transaction Monitoring System 🛡️

**Team ID / Name:** TS423 - Eternal Champions

## Problem Statement
Gujarat disburses welfare benefits through Direct Benefit Transfer across multiple schemes. Known failure modes include:
- Funds reaching ineligible or deceased beneficiaries
- Undrawn funds (beneficiary never notified)
- Duplicate payments under slightly different names or across schemes

## Features
This system processes 50,000+ simulated DBT transactions and generates a structured audit report. It includes:
1. **4 Leakage Pattern Detectors:** Deceased beneficiary, duplicate identity, undrawn funds, and cross-scheme duplication.
2. **Fuzzy Name Matching:** Handles Gujarati transliteration variations.
3. **Risk Scoring:** Assigns explainable risk scores and specific evidence for each anomaly.
4. **Prioritized Queue:** District Finance Officers get an interactive queue instead of a raw data dump.
5. **High-Speed Processing:** Powered by Polars, processing 10,000+ transactions in under a second.
6. **Multi-Role Portals:** Streamlit UI supporting DFOs, Scheme Verifiers, Audit Team, and State Admins.

## Requirements
- Python 3.9+
- Dependencies: `polars`, `rapidfuzz`, `plotly`, `streamlit`

## How to Run
```bash
pip install polars rapidfuzz plotly streamlit
streamlit run app.py
```
