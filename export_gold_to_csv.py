import os
import shlex
import shutil
import pandas as pd
import pyarrow.parquet as pq
import pyarrow as pa
import subprocess

BASE_DIR = os.path.dirname(os.path.abspath(__file__))


def to_wsl_path(path):
    normalized = os.path.abspath(path).replace("\\", "/")
    drive, rest = os.path.splitdrive(normalized)
    if drive:
        return f"/mnt/{drive[0].lower()}{rest}"
    return normalized

# Ruta HDFS y destino local
HDFS_PARQUET = "/geolumica/gold/df_maestro.parquet"
LOCAL_PARQUET = os.path.join(BASE_DIR, "data", "gold", "df_maestro.parquet")
CSV_OUT = os.path.join(BASE_DIR, "data", "gold", "df_maestro.csv")

# 1. Descargar Parquet desde HDFS
os.makedirs(os.path.dirname(CSV_OUT), exist_ok=True)
if os.path.isdir(LOCAL_PARQUET):
    shutil.rmtree(LOCAL_PARQUET)
elif os.path.exists(LOCAL_PARQUET):
    os.remove(LOCAL_PARQUET)

print("Descargando Parquet de HDFS...")
wsl_local_parquet = to_wsl_path(LOCAL_PARQUET)
get_cmd = (
    f"$HOME/hadoop-3.3.6/bin/hdfs dfs -get -f {HDFS_PARQUET} {shlex.quote(wsl_local_parquet)}"
)
subprocess.run([
    "wsl", "-d", "Ubuntu", "--", "bash", "-lc", get_cmd
], check=True)

# 2. Leer Parquet y exportar a CSV
print("Leyendo Parquet y exportando a CSV...")
table = pq.ParquetDataset(LOCAL_PARQUET).read()
df = table.to_pandas()
df.to_csv(CSV_OUT, index=False)
print(f"CSV exportado en: {CSV_OUT}")