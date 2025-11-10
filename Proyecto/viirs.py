import ee
import geemap
import pandas as pd
from tqdm import tqdm
import os
import geopandas as gpd
import time

# 1️⃣ Autenticación con tu proyecto
ee.Authenticate()
ee.Initialize(project='bubbly-reducer-477312-d0')

# 2️⃣ Función: obtener imagen mensual de VIIRS
def viirs_mes(fecha_iso):
    """Devuelve la imagen VIIRS mensual (avg_rad) para la fecha dada."""
    return (
        ee.ImageCollection("NOAA/VIIRS/DNB/MONTHLY_V1/VCMCFG")
        .filterDate(fecha_iso, ee.Date(fecha_iso).advance(1, 'month'))
        .select("avg_rad")
        .first()
    )

# 3️⃣ Reducer combinado (media, min, max, desviación estándar)
reducer = (
    ee.Reducer.mean()
    .combine(ee.Reducer.min(), sharedInputs=True)
    .combine(ee.Reducer.max(), sharedInputs=True)
    .combine(ee.Reducer.stdDev(), sharedInputs=True)
)

# 4️⃣ Estadísticas zonales
def zonal_stats(img, municipios, fecha_iso):
    """Calcula estadísticas zonales de una imagen VIIRS para un conjunto de municipios."""
    stats = img.reduceRegions(
        collection=municipios,
        reducer=reducer,
        scale=1000,    # ajustar según precisión
        tileScale=16   # más tiles = menos riesgo de timeout
    ).map(lambda f: f.set("date", ee.Date(fecha_iso).format("YYYY-MM")))
    return stats

# 5️⃣ Descargar un rango histórico completo para un bloque
def descargar_historico(municipios, anio_ini, anio_fin, bloque_id=0, outdir="salida_viirs"):
    """Descarga datos VIIRS mensuales por bloque de municipios."""
    os.makedirs(outdir, exist_ok=True)
    meses = pd.date_range(f"{anio_ini}-01-01", f"{anio_fin+1}-01-01", freq="MS", inclusive="left")
    dfs = []

    print(f"🚀 Procesando bloque {bloque_id} ({anio_ini}-{anio_fin}) con {municipios.size().getInfo()} municipios")

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
    return None

# 6️⃣ Procesar por bloques (para evitar timeouts)
def descargar_historico_por_bloques(geojson_path, anio_ini, anio_fin, outdir="salida_viirs", block_size=800):
    """Divide municipios en bloques y descarga VIIRS para cada uno."""
    os.makedirs(outdir, exist_ok=True)

    gdf = gpd.read_file(geojson_path)
    n_blocks = (len(gdf) // block_size) + 1
    print(f"📦 Total municipios: {len(gdf)} → {n_blocks} bloques de ~{block_size}")

    for i in range(n_blocks):
        sub = gdf.iloc[i * block_size:(i + 1) * block_size]
        if sub.empty:
            continue

        # ✅ Guardar el geojson temporal dentro del mismo directorio
        sub_path = os.path.join(outdir, f"tmp_municipios_{i}.geojson")
        sub.to_file(sub_path, driver="GeoJSON")

        print(f"\n🚀 Procesando bloque {i+1}/{n_blocks} con {len(sub)} municipios")
        municipios = geemap.geojson_to_ee(sub_path)
        descargar_historico(municipios, anio_ini, anio_fin, bloque_id=i, outdir=outdir)
        time.sleep(60)  # espera 1 minuto antes del siguiente bloque

        # 🧹 Borrar temporal después de usarlo (opcional)
        # os.remove(sub_path)

# 7️⃣ Fusionar todos los CSVs en uno solo
def combinar_csvs(outdir="salida_viirs", final_name="viirs_municipios_final.csv"):
    """Combina todos los CSV de bloques en un único archivo."""
    files = [f for f in os.listdir(outdir) if f.startswith("viirs_bloque_") and f.endswith(".csv")]
    if not files:
        print("⚠️ No hay CSVs de bloques para combinar.")
        return

    dfs = [pd.read_csv(os.path.join(outdir, f)) for f in files]
    df_final = pd.concat(dfs, ignore_index=True)
    out_path = os.path.join(outdir, final_name)
    df_final.to_csv(out_path, index=False)
    print(f"✅ Archivo final combinado: {out_path} ({len(df_final)} filas)")

# 8️⃣ Ejecución principal
if __name__ == "__main__":
    geojson_municipios = "municipios_es.geojson"
    anio_ini, anio_fin = 2018, 2022
    outdir = "salida_viirs"

    descargar_historico_por_bloques(geojson_municipios, anio_ini, anio_fin, outdir=outdir, block_size=1000)
    combinar_csvs(outdir)
