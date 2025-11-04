import pandas as pd
from pathlib import Path

# --- Caminhos dos CSVs ---
CSV_PATH = "/Users/augusto/Library/Mobile Documents/com~apple~CloudDocs/git/avalensurance-bia/data"

paths = {
    "costs": f"{CSV_PATH}/costs_cleaned.csv",
    "clinical": f"{CSV_PATH}/clinical_cleaned.csv",
    "policy": f"{CSV_PATH}/policy_cleaned.csv",
    "demographics": f"{CSV_PATH}/demographics_cleaned.csv",
    "lifestyle": f"{CSV_PATH}/lifestyle_cleaned.csv",
    "utilization": f"{CSV_PATH}/utilization_cleaned.csv",
}

# --- Carrega os DataFrames ---
dfs = {name: pd.read_csv(path) for name, path in paths.items()}

# --- Função para unir com chaves detectadas automaticamente ---
CANDIDATE_KEYS = ["person_id", "policy_id", "record_id", "visit_id"]

def smart_left_merge(base: pd.DataFrame, other: pd.DataFrame, other_name: str):
    keys = [k for k in CANDIDATE_KEYS if k in base.columns and k in other.columns]
    if not keys:
        print(f"⚠️ Nenhuma chave comum encontrada com {other_name}.")
        return base
    print(f"🔗 Mesclando {other_name} pelas chaves: {keys}")
    return base.merge(other, how="left", on=keys, suffixes=("", f"_{other_name}"))

# --- Pipeline de junção ---
merged = dfs["costs"].copy()
for name in ["clinical", "policy", "demographics", "lifestyle", "utilization"]:
    merged = smart_left_merge(merged, dfs[name], name)

# Remove colunas duplicadas
merged = merged.loc[:, ~merged.columns.duplicated()]

# --- Exporta resultado ---
out_path = Path(f"{CSV_PATH}/full_warehouse_merged.csv")
merged.to_csv(out_path, index=False)

print(f"\n✅ Merge concluído com sucesso!")
print(f"Shape final: {merged.shape}")
print(f"Arquivo salvo em: {out_path}")
