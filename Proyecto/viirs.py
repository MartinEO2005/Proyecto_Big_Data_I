import ee
import geopandas as gpd
from datetime import datetime

# Inicializar y autenticar (previamente: earthengine authenticate)
ee.Initialize()

# ------------- Parámetros -------------
# Intervalo de fechas (YYYY-MM-DD)
fecha_inicio = '2023-01-01'
fecha_fin   = '2023-12-31'

# Definir área: bbox [minLon, minLat, maxLon, maxLat] para España peninsular + Baleares + Canarias aproximado
bbox = [-10.7, 27.6, 4.6, 44.2]
geometry = ee.Geometry.Rectangle(bbox)

# Producto VIIRS (ejemplo: VIIRS monthly VCMCFG producto; cambia si usas otra colección)
collection_id = 'NOAA/VIIRS/DNB/MONTHLY_V1/VCMCFG'

# Estadística deseada por pixel o por muestreo puntual
statistic = 'mean'  # 'mean', 'median', 'sum', etc.

# Muestreo: rejilla de puntos (se puede cambiar)
pixel_spacing_km = 5  # separación en km para muestreo de puntos
# -------------------------------------

# Convertir fechas a objetos ee
start = ee.Date(fecha_inicio)
end   = ee.Date(fecha_fin)

# Cargar colección y filtrar
col = ee.ImageCollection(collection_id).filterDate(start, end).filterBounds(geometry)

# Reducir la colección (composite temporal) o calcular por fecha
# Aquí: calcular la media temporal por pixel en el intervalo seleccionado
image = col.mean().clip(geometry)

# Crear una rejilla de puntos para muestreo (simple)
def create_point_grid(rect, spacing_km):
    # convertir spacing km a grados aproximados para latitud/longitud
    spacing_deg = spacing_km / 111.32
    coords = rect.bounds().getInfo()['coordinates'][0]
    minx = coords[0][0]; miny = coords[0][1]; maxx = coords[2][0]; maxy = coords[2][1]
    xs = []
    ys = []
    x = minx
    while x <= maxx:
        xs.append(x)
        x += spacing_deg
    y = miny
    while y <= maxy:
        ys.append(y)
        y += spacing_deg
    pts = []
    for xi in xs:
        for yi in ys:
            pts.append(ee.Feature(ee.Geometry.Point([xi, yi])))
    return ee.FeatureCollection(pts)

points_fc = create_point_grid(ee.Geometry.Rectangle(bbox), pixel_spacing_km)

# Muestrear la imagen en los puntos
# Cambia el nombre de la banda según la colección (por ejemplo 'avg_rad' o 'radiance'; inspecciona col.first())
band_names = image.bandNames().getInfo()
print("BANDAS:", band_names)

sample = image.sampleRegions(collection=points_fc, scale=500, geometries=True)

# Exportar la tabla a Drive (CSV)
task = ee.batch.Export.table.toDrive(
    collection=sample,
    description='VIIRS_Spain_sample_export',
    folder='GEE_exports',  # carpeta en Drive
    fileNamePrefix=f'viirs_spain_{fecha_inicio}_{fecha_fin}',
    fileFormat='CSV'
)
task.start()
print("Export started. Check your Earth Engine Tasks or Drive for the CSV export.")
