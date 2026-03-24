import requests
import json

# Busca a documentação da API para entender os endpoints corretos
r = requests.get("https://api-comexstat.mdic.gov.br/openapi.json")
print(json.dumps(r.json(), indent=2, ensure_ascii=False)[:3000])
