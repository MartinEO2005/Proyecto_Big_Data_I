# demografiaciudades.py
import requests
import pandas as pd
from pathlib import Path

# --- Configuración ---
INE_API_URL = "https://servicios.ine.es/wstempus/js/ES/DATOS_TABLA/29005"


try:
    from extraction.config import OUTDIR as CONFIG_OUTDIR
except Exception:
    CONFIG_OUTDIR = "data"

OUTDIR = Path(CONFIG_OUTDIR)
OUTPUT_FILE = OUTDIR / "demografia_poblacion_municipios.csv"


def fetch_population_by_municipality(years: int | None = 1) -> pd.DataFrame:
    """
    Descarga la población por municipio de España desde la API del INE 
    :param years: número de últimos años (por ejemplo 5) o None para todos
    :return: DataFrame con columnas: cod_prov, cod_muni, municipio, year, population
    """
    params = {"nult": years} if years else {}



    try:
        print(f"[INE] Descargando datos de población municipal ({'todos los años' if not years else f'últimos {years}'})...")
        r = requests.get(INE_API_URL, params=params, timeout=120)
        r.raise_for_status()
        data = r.json()
    except requests.exceptions.RequestException as e:
        print(" Error al consultar API del INE:", e)
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
    """Guarda los datos en un archivo CSV"""
    if df.empty:
        print(" No hay datos para guardar.")
        return
    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)  
    df.to_csv(OUTPUT_FILE, index=False, encoding="utf-8")
    print(f"Datos guardados en: {OUTPUT_FILE}")


if __name__ == "__main__":
    df = fetch_population_by_municipality(None)  
    save_population_data(df)
    print(df.head())
