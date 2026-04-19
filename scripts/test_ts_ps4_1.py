import sys; sys.path.insert(0, '.')
import pandas as pd
df = pd.read_csv('data/TS-PS4-1.csv')
from app.services.csv_analyzer import analyze_csv
result = analyze_csv(df)
print('Patterns run:', result['patterns_run'])
print('Total anomalies:', len(result['anomalies']))
print('Summary:', result['summary'])
print('Errors:', result['errors'])
print()
if result['anomalies']:
    print('Sample anomalies (first 5):')
    for a in result['anomalies'][:5]:
        print(f" - [{a['anomaly_type']}] {a['name']} | Aadhaar:{a['aadhaar']} | District:{a['district']} | Score:{a['risk_score']}")
