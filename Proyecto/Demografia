# demografia.py
import requests
import pandas as pd
from pathlib import Path

# --- Configuración ---
EUROSTAT_API_URL = "https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data/demo_r_pjanaggr3"

# Carpeta de salida (como en los otros módulos del proyecto)
OUTDIR = Path(__file__).resolve().parent.parent / "neo_lumina_output"
OUTDIR.mkdir(parents=True, exist_ok=True)
OUTPUT_FILE = OUTDIR / "demografia_poblacion.csv"


def fetch_population_total_nuts3():
    """
    Descarga población total por región NUTS3 (provincias) en España desde Eurostat.
    Devuelve un DataFrame con columnas: region_code, region_name, year, population
    """
    params = {
        "sex": "T",       # Total (sin desagregar por sexo)
        "age": "TOTAL",   # Todas las edades
        "format": "JSON"
    }

    try:
        print("[Eurostat] Descargando datos de población...")
        r = requests.get(EUROSTAT_API_URL, params=params, timeout=60)
        r.raise_for_status()
        data = r.json()
    except requests.exceptions.RequestException as e:
        print("❌ Error al consultar Eurostat API:", e)
        return pd.DataFrame()

    # --- Extraer dimensiones ---
    dim_geo = data["dimension"]["geo"]["category"]["label"]
    dim_time = data["dimension"]["time"]["category"]["label"]
    values = data["value"]

    # --- Reconstruir las combinaciones ---
    rows = []
    n_geo = len(dim_geo)
    for key, val in values.items():
        key = int(key)
        time_pos = key // n_geo
        geo_pos = key % n_geo

        year = list(dim_time.keys())[time_pos]
        region_code = list(dim_geo.keys())[geo_pos]
        region_name = dim_geo[region_code]

        # Solo regiones españolas (empiezan con 'ES')
        if not region_code.startswith("ES"):
            continue

        rows.append({
            "region_code": region_code,
            "region_name": region_name,
            "year": int(year),
            "population": val
        })

    df = pd.DataFrame(rows)
    print(f"✅ Se descargaron {len(df)} registros de población total (Eurostat)")
    return df


def save_population_data(df: pd.DataFrame):
    """Guarda los datos en un archivo CSV"""
    if df.empty:
        print("⚠️ No hay datos para guardar.")
        return
    df.to_csv(OUTPUT_FILE, index=False)
    print(f"💾 Datos guardados en: {OUTPUT_FILE}")


if __name__ == "__main__":
    df = fetch_population_total_nuts3()
    save_population_data(df)
    print(df.head())
