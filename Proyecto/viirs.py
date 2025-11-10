import ee
import geemap
import pandas as pd
from tqdm import tqdm

# -------------------------------
# 1. Autenticación Earth Engine
# -------------------------------
ee.Authenticate()
ee.Initialize(project='bubbly-reducer-477312-d0')

# Municipios españoles
municipios = ee.FeatureCollection(
    "projects/bubbly-reducer-477312-d0/assets/LAU_RG_01M_2024_3035"
).filter(ee.Filter.eq('CNTR_CODE', 'ES'))

# Disolver multipolígonos por municipio
def disolver_por_municipio(f):
    lau_name = f.get('LAU_NAME')
    gisco_id = f.get('GISCO_ID')
    geom = municipios.filter(ee.Filter.eq('GISCO_ID', gisco_id)).geometry().dissolve()
    return ee.Feature(geom).set({'GISCO_ID': gisco_id, 'LAU_NAME': lau_name})

municipios_unicos = municipios.distinct('GISCO_ID').map(disolver_por_municipio)

# VIIRS mensual (enero y febrero 2020)
viirs = ee.ImageCollection("NOAA/VIIRS/DNB/MONTHLY_V1/VCMCFG") \
            .filterDate('2020-01-01', '2020-03-01') \
            .select('avg_rad')

# Reducer combinado
reducer = ee.Reducer.mean() \
            .combine(ee.Reducer.min(), sharedInputs=True) \
            .combine(ee.Reducer.max(), sharedInputs=True) \
            .combine(ee.Reducer.stdDev(), sharedInputs=True)

# Función zonal stats
def zonal_stats(img):
    stats = img.reduceRegions(
        collection=municipios_unicos,
        reducer=reducer,
        scale=500,
        tileScale=4
    )
    return stats.map(lambda f: f.set('date', img.date().format('YYYY-MM')))

# Aplicar a las imágenes seleccionadas
resultados = viirs.map(zonal_stats).flatten()

# -------------------------------
# 2. Descargar directamente a CSV con barra de progreso
# -------------------------------
csv_file = "viirs_municipios_2meses.csv"

# geemap.ee_to_csv devuelve un CSV completo; para mostrar progreso,
# lo convertimos primero a DataFrame y luego escribimos con tqdm.
df = geemap.ee_to_df(resultados)

with open(csv_file, "w", encoding="utf-8") as f:
    # Escribir cabecera
    f.write(",".join(df.columns) + "\n")
    # Escribir filas con barra de progreso
    for _, row in tqdm(df.iterrows(), total=len(df), desc="Descargando CSV"):
        f.write(",".join(map(str, row.values)) + "\n")

print(f"✅ Datos descargados en {csv_file}")

# -------------------------------
# 3. Limpiar duplicados y renombrar
# -------------------------------
rename_map = {
    'LAU_NAME': 'municipio',
    'GISCO_ID': 'id',
    'avg_rad_mean': 'mean',
    'avg_rad_min': 'min',
    'avg_rad_max': 'max',
    'avg_rad_stdDev': 'stdDev'
}
df = df.rename(columns=rename_map)

# Quitar duplicados
df = df.drop_duplicates(subset=['id','date'])

# Guardar limpio
df.to_csv("luminosidad_municipios_2meses.csv", index=False)
print("✅ CSV final guardado en local: luminosidad_municipios_2meses.csv")
