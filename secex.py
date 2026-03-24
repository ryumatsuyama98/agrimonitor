import requests
import pandas as pd

url = "https://api-comexstat.mdic.gov.br/general"
params = {
    "flow": "export",
    "monthDetail": "true",
    "yearStart": "2025",
    "monthStart": "01",
    "yearEnd": "2026",
    "monthEnd": "02",
}

ncms = ["12011000", "12019000"]
todos = []

import time
import json

for ncm in ncms:
    r = requests.get(url, params={**params, "ncm[]": ncm})
    data = r.json()
    print(f"\n--- NCM {ncm} ---")
    print(json.dumps(data, indent=2, ensure_ascii=False))
    time.sleep(12)
