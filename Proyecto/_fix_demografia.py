import os, sys
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from extraction.demografiaciudades import fetch_population_by_municipality
from extraction.storage import save_df_to_theme

BASE_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "data", "raw")
print("Descargando demografia municipal (30 años)...")
df = fetch_population_by_municipality(years=30)
if df is not None and not df.empty:
    save_df_to_theme(df, "demografia", "demografia_poblacion_municipios.csv", base_outdir=BASE_DIR)
    print(f"OK: {len(df)} filas")
    print(df.head(3).to_string())
else:
    print("ERROR: df vacío")
