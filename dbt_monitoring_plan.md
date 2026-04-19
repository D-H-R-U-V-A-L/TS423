# Implementation Plan: DBT Transaction Monitoring System

## Background & Motivation
Gujarat requires a high-speed anomaly detection system for Direct Benefit Transfer (DBT) disbursements. The system must flag deceased beneficiaries, undrawn funds, duplicate identities, and cross-scheme duplications while providing a structured audit report for District Finance Officers.

## Scope & Impact
- Ingest 50,000+ simulated DBT transactions and civil death registry records.
- Achieve sub-30 second processing time (targeting <5s using Polars).
- Provide an interactive, visually compelling Streamlit dashboard for data exploration and prioritized queues.

## Proposed Architecture
- **Data Engine:** Polars (High-speed, multi-threaded CPU processing in Python) ensuring robustness on Windows without WSL2 overhead.
- **Fuzzy Matching:** RapidFuzz (Optimized C++ string matching for accurate Gujarati-to-English transliteration variations).
- **Graph Analytics:** NetworkX (Connecting Aadhaar numbers to distinct fuzzy names across schemes).
- **Frontend UI:** Streamlit with Plotly visualizations.

## Implementation Steps

### Phase 1: Environment & Data Ingestion
1. Initialize Python environment and install core dependencies (`polars`, `rapidfuzz`, `networkx`, `streamlit`, `plotly`).
2. Load `TS-PS4-1.csv` and `TS-PS4-2.csv` via Polars.
3. Standardize schema data types (dates, floats, strings) to prepare for vectorized operations.

### Phase 2: Core Anomaly Detection Engine
1. **Pattern 1: Deceased Beneficiary Disbursal**
   - Perform a vectorized left join between DBT transactions and the Death Registry on the `aadhaar` key.
   - Flag transactions where the `transaction_date` occurs strictly > 30 days after the `death_date`.
   - **Evidence String:** "Beneficiary matched death register; transaction initiated X days post-mortem."

2. **Pattern 2: Undrawn Funds / Ghost Accounts**
   - Filter transactions where `status == 'SUCCESS'` and `withdrawn == 0`.
   - Calculate the timedelta against the current date; flag accounts dormant > 180 days.
   - **Evidence String:** "Funds successfully credited but unwithdrawn for X consecutive days."

3. **Pattern 3: Duplicate Identity (Fuzzy Matching)**
   - Group records by scheme.
   - Utilize `RapidFuzz` to run similarity comparisons on the `name` column. Flag pairs with >85% phonetic/transliteration match.
   - **Evidence String:** "85%+ phonetic/transliteration match with existing beneficiary in the same scheme."

4. **Pattern 4: Cross-Scheme Duplication (Aadhaar Graph Pivot)**
   - Build a NetworkX graph mapping `Aadhaar` nodes to distinct `Name` variations across mutually exclusive or distinct schemes.
   - Flag high-degree nodes where one Aadhaar is linked to multiple conflicting fuzzy identities or overlapping schemes.
   - **Evidence String:** "Aadhaar linked to multiple distinct fuzzy identities across 3 different schemes."

### Phase 3: Risk Scoring & Evidence Aggregation
1. Merge all flags back into a consolidated Polars DataFrame.
2. Assign weighted Risk Scores (e.g., Deceased = 100, Undrawn = 70).
3. Generate the structured plain-text explainable evidence column for each flagged row.

### Phase 4: Streamlit Dashboard Construction (The Pitch UI)
1. **Macro View:** Render a state-level heatmap using Plotly to show district-wise risk concentrations, plus a pie chart breaking down leakage types.
2. **Micro View:** Display a prioritized investigation queue (sorted strictly descending by Risk Score).
3. **Interactive Evidence:** Embed Streamlit expanders to reveal the plain-text evidence strings for high-risk flags cleanly.

## Verification & Testing
- Profile the pipeline to guarantee end-to-end processing well under the 30-second benchmark.
- Visually inspect the generated dashboard and interact with the prioritized queue.
- Rehearse the 3-minute pitch emphasizing the performance scale (Polars) and algorithmic elegance.