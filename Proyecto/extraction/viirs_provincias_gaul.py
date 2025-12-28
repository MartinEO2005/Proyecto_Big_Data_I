# viirs_provincias_gaul.py
import ee
import geemap
import pandas as pd
from tqdm import tqdm
import os
import time

# Valores por defecto (se pueden sobrescribir pasando outdir a main)
DEFAULT_OUTDIR = 'data/luz_nocturna/provincias'
YEARS = range(2018, 2023)  # 2018-2022 inclusive
SCALE = 1000
TILE_SCALE = 16
SIMPLIFY_TOL = 1000
DEFAULT_PROJECT = "bubbly-reducer-477312-d0"

def init_ee(project="bubbly-reducer-477312-d0"):
    """Inicialización profesional con Service Account y Scopes definidos."""
    import os
    import ee
    from google.oauth2 import service_account
    
    json_path = 'google_credentials.json'
    # Definimos el permiso específico para Earth Engine
    EE_SCOPES = ['https://www.googleapis.com/auth/earthengine', 'https://www.googleapis.com/auth/cloud-platform']
    
    try:
        if os.path.exists(json_path):
            # Cargamos las credenciales añadiendo los SCOPES
            credentials = service_account.Credentials.from_service_account_file(
                json_path, scopes=EE_SCOPES
            )
            ee.Initialize(credentials=credentials, project=project)
            print(f"✅ Earth Engine conectado con Service Account: {credentials.service_account_email}")
        else:
            # Fallback para ejecución local fuera de Docker
            ee.Initialize(project=project)
            print("✅ Earth Engine inicializado con credenciales locales.")
    except Exception as e:
        print(f"❌ Error crítico de autenticación: {e}")
        raise SystemExit(1)

def build_provinces_from_gaul_adm2(simplify_tol=SIMPLIFY_TOL):
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
    provinces_fc = ee.FeatureCollection(prov_keys.map(dissolve_one))
    try:
        print("📦 Provincias GAUL construidas:", provinces_fc.size().getInfo())
    except Exception:
        print("📦 Provincias construidas (conteo no mostrado).")
    return provinces_fc

def viirs_mes(fecha_iso):
    return (ee.ImageCollection("NOAA/VIIRS/DNB/MONTHLY_V1/VCMCFG")
            .filterDate(fecha_iso, ee.Date(fecha_iso).advance(1, 'month'))
            .select('avg_rad').first())

def get_reducer():
    return (ee.Reducer.mean()
            .combine(ee.Reducer.min(), sharedInputs=True)
            .combine(ee.Reducer.max(), sharedInputs=True)
            .combine(ee.Reducer.stdDev(), sharedInputs=True))

def zonal_stats_provinces(img, provinces_fc, fecha_iso):
    reducer = get_reducer()
    stats = img.reduceRegions(
        collection=provinces_fc,
        reducer=reducer,
        scale=SCALE,
        tileScale=TILE_SCALE,
    ).map(lambda f: f.set('date', ee.Date(fecha_iso).format('YYYY-MM')))
    return stats

def procesar_anio_provincias(prov_fc, anio, outdir):
    # asegurarse de usar tmp_provincias para los CSVs anuales
    tmp_outdir = os.path.join(outdir, "tmp_provincias")
    os.makedirs(tmp_outdir, exist_ok=True)

    meses = pd.date_range(f"{anio}-01-01", f"{anio+1}-01-01", freq="MS", inclusive="left")
    dfs = []
    print(f"\n🗓️ Procesando año {anio} (provincias)")
    for fecha in tqdm(meses, desc=f"Provincias {anio}"):
        img = viirs_mes(str(fecha.date()))
        if img is None:
            continue
        stats = zonal_stats_provinces(img, prov_fc, str(fecha.date()))
        try:
            df_mes = geemap.ee_to_df(stats)
            rename_map = {}
            for col in list(df_mes.columns):
                if col.endswith("_mean") and "mean" not in df_mes.columns:
                    rename_map[col] = "mean"
                if col.endswith("_min") and "min" not in df_mes.columns:
                    rename_map[col] = "min"
                if col.endswith("_max") and "max" not in df_mes.columns:
                    rename_map[col] = "max"
                if col.endswith("_stdDev") and "stdDev" not in df_mes.columns:
                    rename_map[col] = "stdDev"
            if rename_map:
                df_mes = df_mes.rename(columns=rename_map)
            for col in ['PROV_CODE', 'PROV_NAME']:
                if col not in df_mes.columns:
                    df_mes[col] = None
            cols_keep = ['PROV_CODE', 'PROV_NAME', 'date', 'mean', 'min', 'max', 'stdDev']
            for c in cols_keep:
                if c not in df_mes.columns:
                    df_mes[c] = pd.NA
            dfs.append(df_mes[cols_keep])
        except Exception as e:
            print(f"⚠️ Error {fecha.strftime('%Y-%m')}: {e}")
    if not dfs:
        print(f"⚠️ Sin datos para {anio}")
        return None
    df_anio = pd.concat(dfs, ignore_index=True)
    # GUARDA EN tmp_provincias (corrección)
    os.makedirs(tmp_outdir, exist_ok=True)
    out_path = os.path.join(tmp_outdir, f"viirs_provincias_{anio}.csv")
    df_anio.to_csv(out_path, index=False)
    print(f"✅ Guardado: {out_path} ({len(df_anio)} filas)")
    return out_path

def concatenar_csvs(outdir, final_name="viirs_provincias_2018_2022.csv"):
    tmp_outdir = os.path.join(outdir, "tmp_provincias")
    if not os.path.isdir(tmp_outdir):
        print("⚠️ No existe tmp_provincias; no hay CSVs para concatenar.")
        return None
    files = sorted([f for f in os.listdir(tmp_outdir) if f.startswith("viirs_provincias_") and f.endswith(".csv")])
    if not files:
        print("⚠️ No hay CSVs para concatenar.")
        return None
    dfs = [pd.read_csv(os.path.join(tmp_outdir, f)) for f in files]
    df_final = pd.concat(dfs, ignore_index=True)
    final_path = os.path.join(outdir, final_name)
    df_final.to_csv(final_path, index=False)
    print(f"\n✅ CSV final combinado: {final_path} ({len(df_final)} filas)")
    return final_path


def main(outdir: str | None = None, project: str | None = DEFAULT_PROJECT):
    """
    Ejecuta el flujo provincial.
    - outdir: carpeta donde escribir resultados (crea outdir/tmp_provincias para intermedios).
    - project: proyecto GEE a usar; si None se intentará fallback.
    Devuelve la ruta absoluta del CSV final o None.
    """
    if outdir is None:
        outdir = DEFAULT_OUTDIR
    os.makedirs(outdir, exist_ok=True)

    init_ee(project=project)

    start = time.time()
    provinces_fc = build_provinces_from_gaul_adm2(SIMPLIFY_TOL)

    year_paths = []
    for anio in YEARS:
        p = procesar_anio_provincias(provinces_fc, anio, outdir=outdir)
        if p:
            year_paths.append(p)

    final = concatenar_csvs(outdir) if year_paths else None

    elapsed = time.time() - start
    print(f"\n⏱️ Tiempo total: {elapsed/60:.2f} min")
    if final:
        return os.path.abspath(final)
    return None

if __name__ == "__main__":
    main()
