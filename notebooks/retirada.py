import pandas as pd
from pathlib import Path

# --- Caminho base ---
CSV_PATH = "/Users/augusto/Library/Mobile Documents/com~apple~CloudDocs/git/avalensurance-bia/data"

# --- Arquivo completo (gerado anteriormente) ---
file_path = f"{CSV_PATH}/full_warehouse_merged.csv"

# --- Ler o CSV ---
df = pd.read_csv(file_path)

# --- Filtrar linhas ---
# Remove linhas que tenham -1 (numérico ou string) ou 'no_data' em qualquer coluna
mask_valid = ~df.isin([-1, "-1", "no_data", "NAO"])
df_clean = df[mask_valid.all(axis=1)].copy()

# --- Exportar resultado ---
out_path = Path(f"{CSV_PATH}/onlyfulldata.csv")
df_clean.to_csv(out_path, index=False)

# --- Mostrar resumo ---
print("✅ Nova tabela criada com sucesso!")
print(f"Linhas originais: {df.shape[0]}")
print(f"Linhas após limpeza: {df_clean.shape[0]}")
print(f"Arquivo salvo em: {out_path}")
