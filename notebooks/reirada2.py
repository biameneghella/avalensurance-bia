import pandas as pd
from pathlib import Path

# --- Caminho base ---
CSV_PATH = "/Users/augusto/Library/Mobile Documents/com~apple~CloudDocs/git/avalensurance-bia/data"

# --- Arquivo completo ---
file_path = f"{CSV_PATH}/full_warehouse_method_B_sem_sklearn.csv"

# --- Ler o CSV ---
df = pd.read_csv(file_path)

# --- Lista de variáveis onde queremos retirar outliers ---
cols_outliers = [
    "monthly_premium",
    "avg_claim_amount",
    "risk_score",
    "chronic_count",
    "is_high_risk",
    "visits_last_year",
    "days_hospitalized_last_3yrs",
    "hospitalizations_last_3yrs",
    "hypertension",
    "systolic_bp",
    "age",
    "mental_health",
    "had_major_procedure",
    "claims_count",
    "diastolic_bp",
    "arthritis",
    "medication_count"
]

# --- Filtrar linhas inválidas (mesma sua regra anterior) ---
mask_valid = ~df.isin([-1, "-1", "no_data", "NAO"])
df_clean = df[mask_valid.all(axis=1)].copy()

# --- Remover outliers SOMENTE das variáveis especificadas ---
for col in cols_outliers:
    if col in df_clean.columns:
        Q1 = df_clean[col].quantile(0.08)
        Q3 = df_clean[col].quantile(0.92)
        IQR = Q3 - Q1
        
        lower = Q1 - 1.5 * IQR
        upper = Q3 + 1.5 * IQR

        df_clean = df_clean[(df_clean[col] >= lower) & (df_clean[col] <= upper)]

# --- Exportar resultado ---
out_path = Path(f"{CSV_PATH}/onlyfulldata_sem_outliers_cols_especificas.csv")
df_clean.to_csv(out_path, index=False)

# --- Mostrar resumo ---
print("✅ Nova tabela criada com sucesso!")
print(f"Linhas originais: {df.shape[0]}")
print(f"Linhas após remover inválidos: {mask_valid.all(axis=1).sum()}")
print(f"Linhas após remover outliers nas colunas selecionadas: {df_clean.shape[0]}")
print(f"Arquivo salvo em: {out_path}")
