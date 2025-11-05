import ee
import geemap
import pandas as pd

ee.Authenticate()
ee.Initialize(project='bubbly-reducer-477312-d0')

# 1. Municipios de España (GAUL nivel 2)
municipios = ee.FeatureCollection("FAO/GAUL/2015/level2") \
                 .filter(ee.Filter.eq('ADM0_NAME', 'Spain'))

# 2. VIIRS mensual últimos 5 años
viirs = ee.ImageCollection("NOAA/VIIRS/DNB/MONTHLY_V1/VCMCFG") \
            .filterDate('2018-01-01', '2023-12-31') \
            .select('avg_rad')

# 3. Reducer combinado: media, min, max, std
reducer = ee.Reducer.mean() \
            .combine(ee.Reducer.min(), sharedInputs=True) \
            .combine(ee.Reducer.max(), sharedInputs=True) \
            .combine(ee.Reducer.stdDev(), sharedInputs=True)

# 4. Función para calcular estadísticas por municipio
def zonal_stats(img):
    stats = img.reduceRegions(
        collection=municipios,
        reducer=reducer,
        scale=500
    )
    return stats.map(lambda f: f.set('date', img.date().format('YYYY-MM')))

# 5. Aplicar función a cada imagen
resultados = viirs.map(zonal_stats).flatten()

# 6. Convertir a DataFrame local
df = geemap.ee_to_df(resultados)

# 7. Guardar en CSV
df.to_csv("luminosidad_municipios.csv", index=False)

print("✅ CSV guardado en local: luminosidad_municipios.csv")
print(df.head())
