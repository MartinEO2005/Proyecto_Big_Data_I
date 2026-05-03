"""
GeoLumica - HDFS (local WSL) -> MariaDB (universidad)
======================================================
Lee Silver + Gold desde HDFS local (hdfs://localhost:9000)
y los vuelca a la base de datos MariaDB de la universidad.

Ejecucion desde PowerShell:
  wsl -d Ubuntu -- /home/fernaferna/spark_env/bin/python \
    /mnt/c/Users/ferna/Documents/GitHub/Proyecto_Big_Data_I/Proyecto_Big_Data_I/hdfs_to_mariadb.py
"""

import os
import sys
import time

from pyspark.sql import SparkSession
from sqlalchemy import create_engine, text

# ---------------------------------------------------------------------------
# 1. CONFIGURACION
# ---------------------------------------------------------------------------
DB_USER = "bd_rvm_gelumica"
DB_PASS = "Rio45Abc"
DB_HOST = "10.151.30.2"
DB_PORT = "3306"
DB_NAME = "bd_rvm_gelumica"

HDFS_BASE   = "hdfs://localhost:9000/geolumica"
SILVER_BASE = f"{HDFS_BASE}/silver/fact"
GOLD_PATH   = f"{HDFS_BASE}/gold/df_maestro.parquet"

# Tablas Silver a cargar  (nombre_hdfs -> nombre_tabla_mariadb)
SILVER_TABLES = {
    "fact_conectividad":       "fact_conectividad",
    "fact_demografia":         "fact_demografia",
    "fact_empresas_transporte":"fact_empresas_transporte",
    "fact_energia":            "fact_energia",
    "fact_migracion_neta":     "fact_migracion_neta",
    "fact_osm_logistica":      "fact_osm_logistica",
    "fact_renta":              "fact_renta",
    "fact_satelital":          "fact_satelital",
    "fact_viirs":              "fact_viirs",
}

# ---------------------------------------------------------------------------
# 2. SPARK SESSION (apuntando al HDFS local de WSL)
# ---------------------------------------------------------------------------
def create_spark():
    os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")
    os.environ.setdefault("SPARK_LOCAL_HOSTNAME", "localhost")
    spark = (
        SparkSession.builder
        .appName("GeoLumica_HDFS_to_MariaDB")
        .config("spark.hadoop.fs.defaultFS", "hdfs://localhost:9000")
        .config("spark.sql.parquet.compression.codec", "snappy")
        .config("spark.driver.memory", "2g")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


# ---------------------------------------------------------------------------
# 3. CARGA
# ---------------------------------------------------------------------------
def cargar_datos():
    t0 = time.time()

    print(f"\n🔌 Conectando a MariaDB en {DB_HOST}:{DB_PORT} / {DB_NAME} ...")
    engine = create_engine(
        f"mysql+pymysql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}",
        connect_args={"connect_timeout": 10},
    )
    # Verificar conexion
    with engine.connect() as conn:
        conn.execute(text("SELECT 1"))
    print("   Conexion OK ✅")

    print("\n✨ Iniciando Spark ...")
    spark = create_spark()
    print("   Spark OK ✅")

    tablas_creadas = []

    # --- CAPA SILVER ---
    print("\n🥈 CARGANDO CAPA SILVER ...")
    for hdfs_name, tabla in SILVER_TABLES.items():
        path = f"{SILVER_BASE}/{hdfs_name}.parquet"
        print(f"   [{hdfs_name}] Leyendo ...", end="", flush=True)
        try:
            df_spark = spark.read.parquet(path)
            df_pd    = df_spark.toPandas()
            print(f" {len(df_pd):,} filas -> subiendo a MariaDB ...", end="", flush=True)
            df_pd.to_sql(tabla, engine, if_exists="replace", index=False,
                         chunksize=5000, method="multi")
            tablas_creadas.append(tabla)
            print(" OK ✅")
        except Exception as exc:
            print(f" ❌ Error: {exc}")

    # --- CAPA GOLD ---
    print("\n🥇 CARGANDO CAPA GOLD ...")
    print("   [df_maestro] Leyendo 130k filas ...", end="", flush=True)
    try:
        df_gold = spark.read.parquet(GOLD_PATH)
        df_pd   = df_gold.toPandas()
        print(f" {len(df_pd):,} filas, {len(df_pd.columns)} cols -> subiendo ...", end="", flush=True)
        df_pd.to_sql("fact_master_gold", engine, if_exists="replace", index=False,
                     chunksize=5000, method="multi")
        tablas_creadas.append("fact_master_gold")
        print(" OK ✅")
    except Exception as exc:
        print(f" ❌ Error: {exc}")

    # --- INDICES ---
    print("\n⚡ Creando indices (lau_id / muni_id) ...")
    with engine.begin() as conn:
        for tabla in tablas_creadas:
            for col_cand in ("lau_id", "muni_id"):
                try:
                    conn.execute(text(f"ALTER TABLE `{tabla}` MODIFY `{col_cand}` VARCHAR(10)"))
                    conn.execute(text(f"CREATE INDEX idx_{tabla}_{col_cand} ON `{tabla}`(`{col_cand}`)"))
                    print(f"   - {tabla}.{col_cand} ✅")
                    break
                except Exception:
                    pass

    elapsed = round((time.time() - t0) / 60, 2)
    print(f"\n🚀 ¡Volcado completo en {elapsed} minutos!")
    print(f"   Tablas cargadas: {', '.join(tablas_creadas)}")
    spark.stop()


if __name__ == "__main__":
    cargar_datos()
