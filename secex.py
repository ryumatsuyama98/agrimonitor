import requests
import json
import time

# Testa diferentes endpoints e formatos
testes = [
    ("GET /general/year/month", "GET", "https://api-comexstat.mdic.gov.br/general/2025/1", {}),
    ("GET com city", "GET", "https://api-comexstat.mdic.gov.br/general", {
        "flow": "export",
        "monthDetail": "true",
        "yearStart": "2025",
        "monthStart": "01",
        "yearEnd": "2025",
        "monthEnd": "03",
        "ncm": "12019000"
    }),
    ("GET ncm sem colchetes", "GET", "https://api-comexstat.mdic.gov.br/general", {
        "flow": "export",
        "monthDetail": "true",
        "yearStart": "2025",
        "monthStart": "01",
        "yearEnd": "2025",
        "monthEnd": "03",
        "typeForm": "ncm",
        "filter": "12019000"
    }),
]

for nome, metodo, url, params in testes:
    print(f"\n=== {nome} ===")
    if metodo == "GET":
        r = requests.get(url, params=params)
    else:
        r = requests.post(url, json=params)
    print(f"URL final: {r.url}")
    data = r.json()
    print(json.dumps(data, indent=2, ensure_ascii=False)[:500])
    time.sleep(13)
