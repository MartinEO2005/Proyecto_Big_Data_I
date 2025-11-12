# demografiaProvincias.py
import requests
import pandas as pd
from pathlib import Path

__all__ = ["fetch_population_total_nuts3", "save_population_data", "fetch_population_and_save"]

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


def save_population_data(df: pd.DataFrame, outdir: Path = None, filename="demografia_poblacion.csv"):
    """Guarda los datos en un archivo CSV"""
    if df.empty:
        print("⚠️ No hay datos para guardar.")
        return None
    if outdir is None:
        outdir = Path(__file__).resolve().parent / "data"
    outdir.mkdir(parents=True, exist_ok=True)
    output_file = outdir / filename
    df.to_csv(output_file, index=False)
    print(f"💾 Datos guardados en: {output_file}")
    return output_file


def fetch_population_and_save(base_outdir="data", filename="demografia_poblacion.csv"):
    """
    Función principal para el main.py.
    Descarga los datos y los guarda en CSV en `base_outdir`.
    Devuelve la ruta del archivo guardado.
    """
    df = fetch_population_total_nuts3()
    path = save_population_data(df, outdir=Path(base_outdir), filename=filename)
    return path


if __name__ == "__main__":
    fetch_population_and_save()
