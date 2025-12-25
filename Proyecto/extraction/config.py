# config.py
import os
from datetime import datetime

# Credenciales S3 / Copernicus
COP_S3_KEY = os.getenv("COP_S3_KEY")
COP_S3_SECRET = os.getenv("COP_S3_SECRET")
COP_S3_ENDPOINT = os.getenv("COP_S3_ENDPOINT", "https://eodata.dataspace.copernicus.eu")
COP_S3_BUCKET = os.getenv("COP_S3_BUCKET", "eodata")

# Usuario CDSE
CDSE_USER = os.getenv("CDSE_USER")
CDSE_PASS = os.getenv("CDSE_PASS")

# Colecciones
COLLECTION_S2 = os.getenv("COLLECTION_S2", "SENTINEL-2")
COLLECTION_S1 = os.getenv("COLLECTION_S1", "SENTINEL-1")


# Área de interés y fechas
# 旧的：
# AOI_WKT = "POLYGON((-9.5 36.0, -9.5 43.8, 3.3 43.8, 3.3 36.0, -9.5 36.0))"
# DATE_FROM = ...
# DATE_TO = ...

# 改成马德里附近（小 AOI，更集中在陆地）
AOI_WKT = "POLYGON((-4.2 40.2, -4.2 40.8, -3.2 40.8, -3.2 40.2, -4.2 40.2))"

# 改成：2024 年 6–8 月（夏季）
DATE_FROM = "2024-06-01"
DATE_TO   = "2024-08-31"


# Filtros
MAX_CLOUD = os.getenv("MAX_CLOUD", "40")
TOP = int(os.getenv("TOP", "500"))

# Carpeta de salida
OUTDIR = os.getenv("OUTDIR", "data")

# Plantilla opcional para descargar VIIRS desde un mirror público.
# Debe incluir los placeholders {year} y {month:02d}, por ejemplo:
#   https://my-mirror/viirs/vcmcfg_{year}{month:02d}.tif
# La variable se puede configurar en el entorno con: setx VIIRS_URL_TEMPLATE "..."
VIIRS_URL_TEMPLATE = os.getenv("VIIRS_URL_TEMPLATE")

