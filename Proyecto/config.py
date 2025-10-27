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
AOI_WKT = "POLYGON((-9.5 36.0, -9.5 43.8, 3.3 43.8, 3.3 36.0, -9.5 36.0))"
DATE_FROM = os.getenv("DATE_FROM", (datetime.utcnow().date().replace(year=datetime.utcnow().year-1)).isoformat())
DATE_TO = os.getenv("DATE_TO", datetime.utcnow().date().isoformat())

# Filtros
MAX_CLOUD = os.getenv("MAX_CLOUD", "40")
TOP = int(os.getenv("TOP", "500"))

# Carpeta de salida
OUTDIR = os.getenv("OUTDIR", "data")