import ee
import geemap
import pandas as pd
from tqdm import tqdm

ee.Authenticate()
ee.Initialize(project='bubbly-reducer-477312-d0')

# 1) Municipios españoles (asset oficial)
municipios_raw = ee.FeatureCollection(
    "projects/bubbly-reducer-477312-d0/assets/LAU_RG_01M_2024_3035"
).filter(ee.Filter.eq('CNTR_CODE', 'ES'))

# 2) Disolver geometrías por municipio
def disolver_por_municipio(f):
    gid = f.get('GISCO_ID')
    nombre = f.get('LAU_NAME')
    geom = municipios_raw.filter(ee.Filter.eq('GISCO_ID', gid)).geometry().dissolve()
    return ee.Feature(geom).set({'GISCO_ID': gid, 'LAU_NAME': nombre})

municipios_unicos = municipios_raw.distinct('GISCO_ID').map(disolver_por_municipio)

# 3) VIIRS mensual
def viirs_mes(fecha_iso):
    return ee.ImageCollection("NOAA/VIIRS/DNB/MONTHLY_V1/VCMCFG") \
        .filterDate(fecha_iso, ee.Date(fecha_iso).advance(1, 'month')) \
        .select('avg_rad') \
        .first()

reducer = ee.Reducer.mean() \
    .combine(ee.Reducer.min(), sharedInputs=True) \
    .combine(ee.Reducer.max(), sharedInputs=True) \
    .combine(ee.Reducer.stdDev(), sharedInputs=True)

def zonal_stats(img, fecha_iso):
    return img.reduceRegions(
        collection=municipios_unicos,
        reducer=reducer,
        scale=500,
        tileScale=8
    ).map(lambda f: f.set('date', ee.Date(fecha_iso).format('YYYY-MM')))

# 4) Rango de meses
meses = pd.date_range("2020-01-01", "2020-02-01", freq="MS")
dfs = []

for fecha in tqdm(meses, desc="Descargando meses"):
    img = viirs_mes(str(fecha.date()))
    if img is None:
        continue
    stats = zonal_stats(img, str(fecha.date()))
    df_mes = geemap.ee_to_df(stats)
    dfs.append(df_mes)

# 5) Concatenar y guardar
df = pd.concat(dfs, ignore_index=True)
df.to_csv("viirs_municipios.csv", index=False)
print("✅ CSV único guardado: viirs_municipios.csv")
print("Filas por mes:", len(df) // len(meses))
