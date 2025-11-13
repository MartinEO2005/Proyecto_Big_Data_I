# viirs.py
import ee
import geemap
import pandas as pd
import geopandas as gpd
import os
import time
from tqdm import tqdm
from storage import save_df_to_theme
import warnings

warnings.filterwarnings("ignore")


# --- 🔹 Inicialización de Earth Engine ---
def init_ee(project="bubbly-reducer-477312-d0"):
    """Inicializa Earth Engine de forma segura."""
    try:
        ee.Initialize(project=project)
        print("✅ Earth Engine inicializado correctamente.")
    except Exception:
        print("🔑 Autenticando con Earth Engine...")
        ee.Authenticate()
        ee.Initialize(project=project)
        print("✅ Earth Engine autenticado e inicializado.")


# --- 🔹 Obtener imagen VIIRS mensual ---
def viirs_mes(fecha_iso):
    """Devuelve la imagen VIIRS mensual (avg_rad) para la fecha dada."""
    return (
        ee.ImageCollection("NOAA/VIIRS/DNB/MONTHLY_V1/VCMCFG")
        .filterDate(fecha_iso, ee.Date(fecha_iso).advance(1, "month"))
        .select("avg_rad")
        .first()
    )


# --- 🔹 Estadísticas zonales ---
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


# --- 🔹 Procesar un bloque de años ---
def descargar_historico(municipios, anio_ini, anio_fin, bloque_id=0, outdir="salida_viirs"):
    """Procesa imágenes VIIRS mensuales dentro de un bloque temporal."""
    os.makedirs(outdir, exist_ok=True)
    meses = pd.date_range(f"{anio_ini}-01-01", f"{anio_fin+1}-01-01", freq="MS", inclusive="left")
    dfs = []

    print(f"\n🚀 Procesando bloque {bloque_id} ({anio_ini}-{anio_fin}) con {municipios.size().getInfo()} municipios")

    for fecha in tqdm(meses, desc=f"Bloque {bloque_id} ({anio_ini}-{anio_fin})"):
        img = viirs_mes(str(fecha.date()))
        if img is None:
            continue

        stats = zonal_stats(img, municipios, str(fecha.date()))
        try:
            df_mes = geemap.ee_to_df(stats)
            dfs.append(df_mes)
        except Exception as e:
            print(f"⚠️ Error en {fecha.strftime('%Y-%m')}: {e}")

    if dfs:
        df = pd.concat(dfs, ignore_index=True)
        out_path = os.path.join(outdir, f"viirs_bloque_{bloque_id}.csv")
        df.to_csv(out_path, index=False)
        print(f"✅ Guardado {out_path} ({len(df)} filas)")
        return df

    print(f"⚠️ No se generaron datos para el bloque {bloque_id}")
    return None


# --- 🔹 Descargar todos los bloques ---
def descargar_historico_por_bloques(geojson_path, anio_ini, anio_fin, outdir="salida_viirs", block_size=800):
    """Divide los municipios en bloques y descarga VIIRS por cada uno."""
    os.makedirs(outdir, exist_ok=True)
    gdf = gpd.read_file(geojson_path)
    n_blocks = (len(gdf) // block_size) + 1
    print(f"📦 Total municipios: {len(gdf)} → {n_blocks} bloques de ~{block_size}")

    for i in range(n_blocks):
        sub = gdf.iloc[i * block_size:(i + 1) * block_size]
        if sub.empty:
            continue
        sub_path = os.path.join(outdir, f"tmp_municipios_{i}.geojson")
        sub.to_file(sub_path, driver="GeoJSON")

        print(f"\n🚀 Procesando bloque {i+1}/{n_blocks} con {len(sub)} municipios")
        municipios = geemap.geojson_to_ee(sub_path)
        descargar_historico(municipios, anio_ini, anio_fin, bloque_id=i, outdir=outdir)
        time.sleep(60)  # evitar rate limit entre bloques


# --- 🔹 Combinar CSVs ---
def combinar_csvs(outdir="salida_viirs", final_name="viirs_municipios_final.csv"):
    """Combina todos los CSVs generados en un archivo final."""
    files = [f for f in os.listdir(outdir) if f.startswith("viirs_bloque_") and f.endswith(".csv")]
    if not files:
        print("⚠️ No hay CSVs para combinar.")
        return None

    dfs = [pd.read_csv(os.path.join(outdir, f)) for f in files]
    df_final = pd.concat(dfs, ignore_index=True)
    out_path = os.path.join(outdir, final_name)
    df_final.to_csv(out_path, index=False)
    print(f"✅ Archivo final combinado: {out_path} ({len(df_final)} filas)")
    return df_final


# --- 🔹 Función principal (para main.py) ---
def fetch_viirs_and_save(geojson_path="municipios_es.geojson", anio_ini=2018, anio_fin=2022, base_outdir="outputs/data"):
    """Ejecuta el flujo completo de VIIRS y guarda el resultado final."""
    print("-> Inicializando Earth Engine para VIIRS...")
    init_ee()

    tmp_outdir = os.path.join(base_outdir, "luz_nocturna/tmp")
    os.makedirs(tmp_outdir, exist_ok=True)

    start_time = time.time()
    print("\n🌙 Iniciando descarga de datos VIIRS (NOAA)...")

    descargar_historico_por_bloques(
        geojson_path=geojson_path,
        anio_ini=anio_ini,
        anio_fin=anio_fin,
        outdir=tmp_outdir,
        block_size=1000,
    )

    df_final = combinar_csvs(tmp_outdir, final_name="viirs_luz_nocturna.csv")

    elapsed = time.time() - start_time
    print(f"\n⏱️ Tiempo total de descarga VIIRS: {elapsed/60:.2f} minutos")

    if df_final is not None and not df_final.empty:
        path = save_df_to_theme(
            df_final,
            theme="luz_nocturna",
            filename="viirs_luz_nocturna.csv",
            base_outdir=base_outdir,
        )
        print("💾 Datos VIIRS guardados en:", path)
        return path

    print("⚠️ No se generó el DataFrame final VIIRS.")
    return None


if __name__ == "__main__":
    fetch_viirs_and_save()
