import requests
import pandas as pd
import io

# Base bruta do MDIC - arquivos CSV públicos sem autenticação
# Exportações 2025
url_2025 = "https://balanca.economia.gov.br/balanca/bd/comexstat-bd/ncm/EXP_2025.csv"
url_2026 = "https://balanca.economia.gov.br/balanca/bd/comexstat-bd/ncm/EXP_2026.csv"

ncms = [12011000, 12019000]
dfs = []

for url, ano in [(url_2025, 2025), (url_2026, 2026)]:
    print(f"Baixando {ano}...")
    r = requests.get(url, stream=True)
    df = pd.read_csv(io.StringIO(r.content.decode("latin1")), sep=";")
    df = df[df["CO_NCM"].isin(ncms)]
    dfs.append(df)
    print(f"{ano}: {len(df)} linhas encontradas")

resultado = pd.concat(dfs)
print(resultado.head(10).to_string())
