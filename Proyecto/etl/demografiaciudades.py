# demografiaciudades.py
import requests
import pandas as pd
import time
from pathlib import Path

# --- Configuración ---
INE_API_URL = "https://servicios.ine.es/wstempus/js/ES/DATOS_TABLA/29005"

try:
    from extraction.config import OUTDIR as CONFIG_OUTDIR
except Exception:
    CONFIG_OUTDIR = "data"

OUTDIR = Path(CONFIG_OUTDIR)
# La ruta correcta que definimos antes
OUTPUT_FILE = OUTDIR / "demografia" / "demografia_poblacion_municipios.csv"

def fetch_population_by_municipality(years: int | None = 1) -> pd.DataFrame:
    params = {"nult": years} if years else {}
    max_retries = 3
    data = None

    for intento in range(max_retries):
        try:
            print(f"[INE] Descargando datos de población municipal (Intento {intento + 1}/{max_retries})...")
            r = requests.get(INE_API_URL, params=params, timeout=120)
            r.raise_for_status()
            
            # Intentamos parsear el JSON ignorando caracteres invisibles
            data = r.json(strict=False) 
            
            # Si llegamos a esta línea, la descarga fue un éxito completo. Rompemos el bucle.
            break 
            
        except Exception as e:
            print(f" ⚠️ Error: El servidor del INE cortó la conexión o envió datos corruptos ({e})")
            if intento < max_retries - 1:
                print(" ⏳ Reintentando en 5 segundos...")
                time.sleep(5)
            else:
                print(" ❌ Se agotaron los reintentos. El servidor del INE está fallando hoy.")
                return pd.DataFrame()

    if not data:
        print("⚠️ No se recibieron datos de la API del INE.")
        return pd.DataFrame()

    rows = []
    for entry in data:
        municipio = entry.get("Nombre", "Desconocido")
        cod_prov = entry.get("CODPROV", "")
        cod_muni = entry.get("CODMUNI", "")
        for dato in entry.get("Data", []):
            year = dato.get("Anyo")
            poblacion = dato.get("Valor")
            if poblacion is None:
                continue
            rows.append({
                "cod_prov": cod_prov,
                "cod_muni": cod_muni,
                "municipio": municipio,
                "year": year,
                "population": poblacion
            })

    df = pd.DataFrame(rows)
    print(f"✅ Se descargaron {len(df)} registros de población municipal (INE)")
    return df

def save_population_data(df: pd.DataFrame):
    if df.empty:
        print(" No hay datos para guardar.")
        return
    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)  
    df.to_csv(OUTPUT_FILE, index=False, encoding="utf-8")
    print(f"✅ Datos guardados en: {OUTPUT_FILE}")

if __name__ == "__main__":
    df = fetch_population_by_municipality(None)  
    save_population_data(df)
    if not df.empty:
        print(df.head())