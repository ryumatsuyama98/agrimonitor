import requests
import pandas as pd
import time
import json

ncms = ["12011000", "12019000"]
todos = []

for ncm in ncms:
    payload = {
        "flow": "export",
        "monthDetail": True,
        "yearStart": 2025,
        "monthStart": 1,
        "yearEnd": 2026,
        "monthEnd": 2,
        "filters": [
            {"filter": "ncm", "values": [ncm]}
        ],
        "details": ["ncm"],
        "metrics": ["metricFOB", "metricKG"]
    }
    
    r = requests.post(
        "https://api-comexstat.mdic.gov.br/general",
        json=payload,
        headers={"Content-Type": "application/json"}
    )
    data = r.json()
    print(f"\n--- NCM {ncm} ---")
    print(json.dumps(data, indent=2, ensure_ascii=False))
    time.sleep(12)
