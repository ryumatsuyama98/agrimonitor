import requests
import pandas as pd
import sqlite3
import io
import os
from datetime import datetime

# ── Configurações ──────────────────────────────────────────────────────────────
DB_PATH = "conab/conab.db"

URLS = {
    "graos": "https://portaldeinformacoes.conab.gov.br/downloads/arquivos/LevantamentoGraos.txt",
    "cana":  "https://portaldeinformacoes.conab.gov.br/downloads/arquivos/LevantamentoCana.txt",
}

# Culturas que queremos guardar — nome exato como aparece na coluna PRODUTO do arquivo
CULTURAS_CONFIG = [
    {"produto": "SOJA",                          "cultura": "Soja"},
    {"produto": "MILHO TOTAL (1ª+2ª SAFRAS)",   "cultura": "Milho Total"},
    {"produto": "ALGODÃO EM PLUMA",              "cultura": "Algodão em Pluma"},
]

# Cana vem de arquivo separado — produto fixo
PRODUTO_CANA = "CANA-DE-AÇÚCAR"

# ── Inicializa banco ───────────────────────────────────────────────────────────
os.makedirs("conab", exist_ok=True)

conn = sqlite3.connect(DB_PATH)
conn.execute("""
    CREATE TABLE IF NOT EXISTS safra (
        produto        TEXT,
        cultura        TEXT,
        safra          TEXT,
        levantamento   INTEGER,
        area_mil_ha    REAL,
        produtividade  REAL,
        producao_mil_t REAL,
        updated_at     TEXT,
        PRIMARY KEY (produto, safra, levantamento)
    )
""")
conn.commit()

# ── Função auxiliar de download ────────────────────────────────────────────────
def baixa_txt(url):
    print(f"Baixando {url}...")
    r = requests.get(url, timeout=120, verify=False)
    r.raise_for_status()
    df = pd.read_csv(io.StringIO(r.content.decode("latin1")), sep=";", dtype=str)
    df.columns = [c.strip() for c in df.columns]
    print(f"  → {len(df)} linhas, colunas: {list(df.columns)}")
    return df

def parse_float(val):
    try:
        return float(str(val).replace(".", "").replace(",", ".").strip())
    except (ValueError, AttributeError):
        return None

# ── Grãos (Soja, Milho, Algodão) ──────────────────────────────────────────────
produtos_alvo   = [c["produto"]  for c in CULTURAS_CONFIG]
produto_cultura = {c["produto"]: c["cultura"] for c in CULTURAS_CONFIG}

try:
    df_graos = baixa_txt(URLS["graos"])

    # Filtra só as culturas desejadas e apenas a linha de total Brasil
    # (sem coluna de estado, ou estado vazio/nulo = linha de consolidado nacional)
    col_produto = next(c for c in df_graos.columns if "PRODUTO" in c.upper())
    col_safra   = next(c for c in df_graos.columns if "SAFRA"   in c.upper())
    col_lev     = next(c for c in df_graos.columns if "LEVANTAMENTO" in c.upper())
    col_area    = next(c for c in df_graos.columns if "AREA"    in c.upper())
    col_prod    = next(c for c in df_graos.columns if "PRODUTIVIDADE" in c.upper())
    col_prodc   = next(c for c in df_graos.columns if "PRODUCAO" in c.upper() or "PRODUÇÃO" in c.upper())

    # Linha de total Brasil = sem estado ou estado == "BRASIL"
    col_estado = next((c for c in df_graos.columns if c.upper() in ("ESTADO", "UF")), None)
    if col_estado:
        df_graos = df_graos[
            df_graos[col_estado].isna() |
            df_graos[col_estado].str.strip().str.upper().isin(["", "BRASIL"])
        ]

    df_graos = df_graos[df_graos[col_produto].str.strip().isin(produtos_alvo)].copy()

    df_graos["produto"]        = df_graos[col_produto].str.strip()
    df_graos["cultura"]        = df_graos["produto"].map(produto_cultura)
    df_graos["safra"]          = df_graos[col_safra].str.strip()
    df_graos["levantamento"]   = pd.to_numeric(df_graos[col_lev], errors="coerce")
    df_graos["area_mil_ha"]    = df_graos[col_area].apply(parse_float)
    df_graos["produtividade"]  = df_graos[col_prod].apply(parse_float)
    df_graos["producao_mil_t"] = df_graos[col_prodc].apply(parse_float)
    df_graos["updated_at"]     = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    df_graos = df_graos[["produto","cultura","safra","levantamento",
                          "area_mil_ha","produtividade","producao_mil_t","updated_at"]]

    df_graos.to_sql("safra", conn, if_exists="append", index=False, method="multi")
    conn.execute("""
        DELETE FROM safra WHERE rowid NOT IN (
            SELECT MAX(rowid) FROM safra GROUP BY produto, safra, levantamento
        )
    """)
    conn.commit()
    print(f"  ✅ {len(df_graos)} registros de grãos inseridos/atualizados.")

except Exception as e:
    print(f"  ❌ Erro ao processar grãos: {e}")

# ── Cana-de-Açúcar ─────────────────────────────────────────────────────────────
try:
    df_cana = baixa_txt(URLS["cana"])

    col_safra  = next(c for c in df_cana.columns if "SAFRA"        in c.upper())
    col_lev    = next(c for c in df_cana.columns if "LEVANTAMENTO" in c.upper())
    col_area   = next(c for c in df_cana.columns if "AREA"         in c.upper())
    col_prod   = next(c for c in df_cana.columns if "PRODUTIVIDADE" in c.upper())
    col_prodc  = next(c for c in df_cana.columns if "PRODUCAO" in c.upper() or "PRODUÇÃO" in c.upper())

    # Linha de total Brasil
    col_estado = next((c for c in df_cana.columns if c.upper() in ("ESTADO", "UF")), None)
    if col_estado:
        df_cana = df_cana[
            df_cana[col_estado].isna() |
            df_cana[col_estado].str.strip().str.upper().isin(["", "BRASIL"])
        ]

    df_cana["produto"]        = PRODUTO_CANA
    df_cana["cultura"]        = "Cana-de-Açúcar"
    df_cana["safra"]          = df_cana[col_safra].str.strip()
    df_cana["levantamento"]   = pd.to_numeric(df_cana[col_lev], errors="coerce")
    df_cana["area_mil_ha"]    = df_cana[col_area].apply(parse_float)
    df_cana["produtividade"]  = df_cana[col_prod].apply(parse_float)
    df_cana["producao_mil_t"] = df_cana[col_prodc].apply(parse_float)
    df_cana["updated_at"]     = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    df_cana = df_cana[["produto","cultura","safra","levantamento",
                        "area_mil_ha","produtividade","producao_mil_t","updated_at"]]

    df_cana.to_sql("safra", conn, if_exists="append", index=False, method="multi")
    conn.execute("""
        DELETE FROM safra WHERE rowid NOT IN (
            SELECT MAX(rowid) FROM safra GROUP BY produto, safra, levantamento
        )
    """)
    conn.commit()
    print(f"  ✅ {len(df_cana)} registros de cana inseridos/atualizados.")

except Exception as e:
    print(f"  ❌ Erro ao processar cana: {e}")

conn.close()
print("\nConcluído.")
