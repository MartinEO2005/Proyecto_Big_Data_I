# viirs_provincias_gaul.py
import ee
import geemap
import pandas as pd
from tqdm import tqdm
import os
import time
from datetime import datetime

# --- 🔹 Configuración Global ---
DEFAULT_OUTDIR = 'data/luz_nocturna/provincias'
SCALE = 1000
TILE_SCALE = 16
SIMPLIFY_TOL = 1000
DEFAULT_PROJECT = "bubbly-reducer-477312-d0"

def init_ee(project=DEFAULT_PROJECT):
    """Inicialización de Earth Engine con soporte para Service Account o local."""
    json_path = 'google_credentials.json'
    EE_SCOPES = ['https://www.googleapis.com/auth/earthengine', 'https://www.googleapis.com/auth/cloud-platform']
    
    try:
        if os.path.exists(json_path):
            from google.oauth2 import service_account
            credentials = service_account.Credentials.from_service_account_file(
                json_path, scopes=EE_SCOPES
            )
            ee.Initialize(credentials=credentials, project=project)
            print(f"✅ Earth Engine conectado (Provincias).")
        else:
            ee.Initialize(project=project)
            print("✅ Earth Engine inicializado con credenciales locales.")
    except Exception as e:
        print(f"❌ Error de autenticación en Provincias: {e}")
        raise SystemExit(1)

# --- 🔹 Construcción de Geometría ---
def build_provinces_from_gaul_adm2(simplify_tol=SIMPLIFY_TOL):
    """Extrae y disuelve las provincias de España desde la base de datos GAUL."""
    gaul_lvl2 = ee.FeatureCollection("FAO/GAUL/2015/level2").filter(ee.Filter.eq('ADM0_NAME', 'Spain'))
    
    def annotate(ft):
        return ft.set({'PROV_CODE': ft.get('ADM2_CODE'), 'PROV_NAME': ft.get('ADM2_NAME')})
    
    gaul_lvl2 = gaul_lvl2.map(annotate)
    prov_keys = gaul_lvl2.aggregate_array('PROV_CODE').distinct()

    def dissolve_one(code):
        subset = gaul_lvl2.filter(ee.Filter.eq('PROV_CODE', code))
        geom = subset.geometry().simplify(simplify_tol)
        name = ee.String(ee.Feature(subset.first()).get('PROV_NAME'))
        return ee.Feature(geom).set({'PROV_CODE': code, 'PROV_NAME': name})
    
    return ee.FeatureCollection(prov_keys.map(dissolve_one))

# --- 🔹 Procesamiento Mensual ---
def viirs_mes(fecha_iso):
    """Obtiene la primera imagen válida del mes en la colección NOAA VIIRS."""
    img_coll = (ee.ImageCollection("NOAA/VIIRS/DNB/MONTHLY_V1/VCMCFG")
                .filterDate(fecha_iso, ee.Date(fecha_iso).advance(1, 'month'))
                .select('avg_rad'))
    
    return img_coll.first() if img_coll.size().getInfo() > 0 else None

def get_reducer():
    """Define el conjunto de estadísticas a calcular."""
    return (ee.Reducer.mean()
            .combine(ee.Reducer.min(), sharedInputs=True)
            .combine(ee.Reducer.max(), sharedInputs=True)
            .combine(ee.Reducer.stdDev(), sharedInputs=True))

def zonal_stats_provinces(img, provinces_fc, fecha_iso):
    """Ejecuta la reducción estadística por región provincial."""
    reducer = get_reducer()
    return img.reduceRegions(
        collection=provinces_fc,
        reducer=reducer,
        scale=SCALE,
        tileScale=TILE_SCALE,
    ).map(lambda f: f.set('date', ee.Date(fecha_iso).format('YYYY-MM')))

# --- 🔹 Gestión Anual Incremental ---
def procesar_anio_provincias(prov_fc, anio, outdir):
    """
    Procesa un año completo. Si el CSV del año ya existe y no es el año actual, 
    salta la descarga para ahorrar tiempo[cite: 14].
    """
    tmp_outdir = os.path.join(outdir, "tmp_provincias")
    os.makedirs(tmp_outdir, exist_ok=True)
    out_path = os.path.join(tmp_outdir, f"viirs_provincias_{anio}.csv")

    # Si ya tenemos el año y no es el año en curso, saltar
    if os.path.exists(out_path) and anio < datetime.now().year:
        print(f"⏭️ Año {anio} ya procesado. Saltando...")
        return out_path

    meses = pd.date_range(f"{anio}-01-01", f"{anio+1}-01-01", freq="MS", inclusive="left")
    dfs = []
    
    print(f"\n🗓️ Procesando año {anio}")
    for fecha in tqdm(meses, desc=f"Provincias {anio}"):
        img = viirs_mes(str(fecha.date()))
        if img is None: continue
        
        try:
            stats = zonal_stats_provinces(img, prov_fc, str(fecha.date()))
            df_mes = geemap.ee_to_df(stats)
            
            # Limpieza de nombres de columnas (EE suele añadir prefijos)
            rename_map = {col: col.split('_')[-1] for col in df_mes.columns if any(x in col for x in ['mean', 'min', 'max', 'stdDev'])}
            df_mes = df_mes.rename(columns=rename_map)
            
            cols_keep = ['PROV_CODE', 'PROV_NAME', 'date', 'mean', 'min', 'max', 'stdDev']
            dfs.append(df_mes[[c for c in cols_keep if c in df_mes.columns]])
        except Exception as e:
            print(f"⚠️ Error en mes {fecha.strftime('%Y-%m')}: {e}")

    if dfs:
        df_anio = pd.concat(dfs, ignore_index=True)
        df_anio.to_csv(out_path, index=False)
        print(f"✅ Año {anio} guardado.")
        return out_path
    return None

def concatenar_csvs(outdir):
    """Une todos los archivos anuales en el CSV maestro final[cite: 14]."""
    tmp_outdir = os.path.join(outdir, "tmp_provincias")
    files = sorted([f for f in os.listdir(tmp_outdir) if f.startswith("viirs_provincias_") and f.endswith(".csv")])
    
    if not files: return None
    
    dfs = [pd.read_csv(os.path.join(tmp_outdir, f)) for f in files]
    df_final = pd.concat(dfs, ignore_index=True)
    
    # Nombre dinámico según el rango detectado
    min_y = df_final['date'].str[:4].min()
    max_y = df_final['date'].str[:4].max()
    final_name = f"viirs_provincias_{min_y}_{max_y}.csv"
    
    final_path = os.path.join(outdir, final_name)
    df_final.to_csv(final_path, index=False)
    print(f"\n✅ CSV Provincial Maestro creado: {final_path}")
    return final_path

# --- 🔹 Función Principal ---
def main(outdir=DEFAULT_OUTDIR, project=DEFAULT_PROJECT):
    """Orquestador del flujo provincial de GeoLúmica[cite: 14]."""
    os.makedirs(outdir, exist_ok=True)
    init_ee(project=project)

    start_time = time.time()
    provinces_fc = build_provinces_from_gaul_adm2()

    # Rango dinámico desde 2012 hasta hoy
    current_year = datetime.now().year
    years_to_process = range(2012, current_year + 1)

    processed_paths = []
    for anio in years_to_process:
        path = procesar_anio_provincias(provinces_fc, anio, outdir)
        if path:
            processed_paths.append(path)

    final_csv = concatenar_csvs(outdir)
    
    elapsed = (time.time() - start_time) / 60
    print(f"\n⏱️ Tiempo total provincias: {elapsed:.2f} min")
    return final_csv

if __name__ == "__main__":
    main()