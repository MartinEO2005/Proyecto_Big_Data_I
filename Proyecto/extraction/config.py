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

AOI_WKT = "POLYGON((-4.2 40.2, -4.2 40.8, -3.2 40.8, -3.2 40.2, -4.2 40.2))"

DATE_FROM = "2024-06-01"
DATE_TO   = "2024-08-31"


# Filtros
MAX_CLOUD = os.getenv("MAX_CLOUD", "40")
TOP = int(os.getenv("TOP", "500"))

# Carpeta de salida
OUTDIR = os.getenv("OUTDIR", "data")

VIIRS_URL_TEMPLATE = os.getenv("VIIRS_URL_TEMPLATE")

