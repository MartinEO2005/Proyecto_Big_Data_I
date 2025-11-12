# demografiaProvincias.py
import requests
import pandas as pd
from pathlib import Path
from storage import save_df_to_theme  # ✅ integración con storage.py

__all__ = ["fetch_population_total_nuts3", "fetch_population_and_save"]

# --- Configuración por defecto ---
EUROSTAT_API_URL = "https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data/demo_r_pjanaggr3"


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


def fetch_population_and_save(base_outdir="outputs/data", filename="demografia_poblacion_provincias.csv"):
    """
    Función principal para el main.py.
    Descarga los datos y los guarda en CSV dentro de la carpeta temática 'demografia'.
    Devuelve la ruta del archivo guardado.
    """
    df = fetch_population_total_nuts3()
    if df is None or df.empty:
        print("⚠️ No hay datos demográficos para guardar.")
        return None

    # ✅ Usa el sistema de carpetas de storage.py
    path = save_df_to_theme(df, theme="demografia", filename=filename, base_outdir=base_outdir)
    print(f"💾 Datos demográficos guardados en: {path}")
    return path


if __name__ == "__main__":
    fetch_population_and_save()
