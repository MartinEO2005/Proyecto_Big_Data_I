# viirs.py
import ee
import geemap
import pandas as pd
import geopandas as gpd
import os
import time
from datetime import datetime
from tqdm import tqdm
from extraction.storage import save_df_to_theme
import warnings

warnings.filterwarnings("ignore")

# --- 🔹 Configuración y Autenticación ---
def init_ee(project="bubbly-reducer-477312-d0"):
    """Inicialización profesional con Service Account o credenciales locales."""
    json_path = 'google_credentials.json'
    EE_SCOPES = ['https://www.googleapis.com/auth/earthengine', 'https://www.googleapis.com/auth/cloud-platform']
    
    try:
        if os.path.exists(json_path):
            from google.oauth2 import service_account
            credentials = service_account.Credentials.from_service_account_file(
                json_path, scopes=EE_SCOPES
            )
            ee.Initialize(credentials=credentials, project=project)
            print(f"✅ Earth Engine conectado con Service Account.")
        else:
            ee.Initialize(project=project)
            print("✅ Earth Engine inicializado con credenciales locales.")
    except Exception as e:
        print(f"❌ Error crítico de autenticación: {e}")
        raise SystemExit(1)

# --- 🔹 Lógica de Actualización Incremental ---
def get_last_downloaded_date(base_outdir):
    """
    Busca el archivo final para determinar desde qué fecha retomar la descarga.
    Si no existe, devuelve el inicio de la serie histórica (Abril 2012).
    """
    final_path = os.path.join(base_outdir, "luz_nocturna", "viirs_luz_nocturna.csv")
    
    if os.path.exists(final_path):
        try:
            df = pd.read_csv(final_path)
            if not df.empty and 'date' in df.columns:
                last_date = pd.to_datetime(df['date']).max()
                # Retornamos el primer día del mes siguiente
                next_date = last_date + pd.DateOffset(months=1)
                return next_date.year, next_date.month
        except Exception as e:
            print(f"⚠️ No se pudo leer el historial ({e}). Iniciando desde 2012.")
    
    return 2012, 4  # Inicio oficial de VIIRS Monthly

# --- 🔹 Procesamiento de Imágenes ---
def viirs_mes(fecha_iso):
    """Devuelve la imagen VIIRS mensual para la fecha dada si existe."""
    collection = (
        ee.ImageCollection("NOAA/VIIRS/DNB/MONTHLY_V1/VCMCFG")
        .filterDate(fecha_iso, ee.Date(fecha_iso).advance(1, "month"))
        .select("avg_rad")
    )
    
    # Verificamos si hay imágenes disponibles para ese mes
    count = collection.size().getInfo()
    return collection.first() if count > 0 else None

def zonal_stats(img, municipios, fecha_iso):
    """Calcula estadísticas zonales sobre los municipios para una fecha."""
    reducer = (
        ee.Reducer.mean()
        .combine(ee.Reducer.min(), sharedInputs=True)
        .combine(ee.Reducer.max(), sharedInputs=True)
        .combine(ee.Reducer.stdDev(), sharedInputs=True)
    )

    stats = img.reduceRegions(
        collection=municipios,
        reducer=reducer,
        scale=1000,
        tileScale=16,
    ).map(lambda f: f.set("date", ee.Date(fecha_iso).format("YYYY-MM")))

    return stats

# --- 🔹 Gestión de Descarga por Bloques ---
def descargar_rango_temporal(municipios, start_year, start_month, end_year, end_month, bloque_id, outdir):
    """Bucle de descarga mensual para un grupo de municipios."""
    os.makedirs(outdir, exist_ok=True)
    
    start_date = f"{start_year}-{start_month:02d}-01"
    end_date = f"{end_year}-{end_month:02d}-01"
    
    meses = pd.date_range(start_date, end_date, freq="MS")
    dfs = []

    for fecha in tqdm(meses, desc=f"Bloque {bloque_id} ({start_year}-{end_year})", leave=False):
        fecha_str = str(fecha.date())
        img = viirs_mes(fecha_str)
        
        if img is None:
            # Esto ocurrirá si pides un mes que aún no se ha procesado (ej. marzo 2026)
            continue

        try:
            stats = zonal_stats(img, municipios, fecha_str)
            df_mes = geemap.ee_to_df(stats)
            if not df_mes.empty:
                dfs.append(df_mes)
        except Exception as e:
            print(f"⚠️ Error en {fecha.strftime('%Y-%m')}: {e}")

    if dfs:
        df_bloque = pd.concat(dfs, ignore_index=True)
        out_path = os.path.join(outdir, f"viirs_bloque_{bloque_id}.csv")
        df_bloque.to_csv(out_path, index=False)
        return df_bloque
    return None

# --- 🔹 Orquestador Principal ---
def fetch_viirs_and_save(geojson_path="municipios_es.geojson", base_outdir="outputs/data"):
    """
    Punto de entrada para el pipeline de GeoLúmica.
    Gestiona autenticación, incrementalidad y guardado final.
    """
    print("\n🌙 --- Módulo VIIRS (Nighttime Lights) ---")
    init_ee()

    # 1. Determinar rango temporal
    y_start, m_start = get_last_downloaded_date(base_outdir)
    now = datetime.now()
    y_end, m_end = now.year, now.month

    if y_start > y_end or (y_start == y_end and m_start > m_end):
        print("✅ Los datos ya están actualizados hasta el mes actual.")
        return os.path.join(base_outdir, "luz_nocturna", "viirs_luz_nocturna.csv")

    print(f"📅 Rango de actualización: {y_start}-{m_start:02d} hasta {y_end}-{m_end:02d}")

    # 2. Preparar directorios y datos geográficos
    tmp_outdir = os.path.join(base_outdir, "luz_nocturna/tmp")
    os.makedirs(tmp_outdir, exist_ok=True)
    
    gdf = gpd.read_file(geojson_path)
    block_size = 1000
    n_blocks = (len(gdf) // block_size) + 1
    
    all_data = []

    # 3. Procesar por bloques de municipios para evitar Timeouts de GEE
    for i in range(n_blocks):
        sub = gdf.iloc[i * block_size : (i + 1) * block_size]
        if sub.empty: continue
        
        sub_path = os.path.join(tmp_outdir, f"tmp_muni_{i}.geojson")
        sub.to_file(sub_path, driver="GeoJSON")
        
        print(f"📦 Procesando bloque {i+1}/{n_blocks} ({len(sub)} municipios)")
        municipios_ee = geemap.geojson_to_ee(sub_path)
        
        df_b = descargar_rango_temporal(municipios_ee, y_start, m_start, y_end, m_end, i, tmp_outdir)
        if df_b is not None:
            all_data.append(df_b)
        
        # Pausa de cortesía para la API
        time.sleep(5)

    # 4. Consolidar y Guardar
    if not all_data:
        print("⚠️ No se encontraron nuevos datos en Earth Engine.")
        return None

    df_new = pd.concat(all_data, ignore_index=True)
    
    # Si ya existía el archivo, lo unimos con lo nuevo
    final_path = os.path.join(base_outdir, "luz_nocturna", "viirs_luz_nocturna.csv")
    if os.path.exists(final_path):
        df_old = pd.read_csv(final_path)
        df_final = pd.concat([df_old, df_new], ignore_index=True).drop_duplicates(subset=['LAU_ID', 'date'])
    else:
        df_final = df_new

    # Guardado definitivo
    path = save_df_to_theme(
        df_final,
        theme="luz_nocturna",
        filename="viirs_luz_nocturna.csv",
        base_outdir=base_outdir
    )
    
    print(f"💾 Proceso finalizado. Datos guardados en: {path}")
    return path

if __name__ == "__main__":
    fetch_viirs_and_save()