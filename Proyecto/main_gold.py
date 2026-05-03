"""
Orquestador Gold: Silver Parquet -> Gold (Spark)

Replica el patron exacto del referente (gold_feature_store.py):
  - Lee los Silver Parquet ya generados por main_silver.py.
  - Ancla: fact_conectividad (muni_id, year, indice_conectividad, Vehiculos_Oficial).
  - LEFT JOINs en orden: osm -> consumo -> demografia -> migracion -> renta -> empresas -> viirs_anual.
  - Sin seleccion de columnas -> exactamente 77 columnas.
  - Filas = filas en fact_conectividad (= filas de conectividad_final_limpio.csv).
"""

import logging
import os
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

logger = logging.getLogger(__name__)

def configure_local_spark_env():
    os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")
    os.environ.setdefault("SPARK_LOCAL_HOSTNAME", "localhost")

def setup_logger(log_file="logs/main_gold.log"):
    os.makedirs("logs", exist_ok=True)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler(),
        ],
    )


def ensure_output_dir(path):
    if path.startswith("hdfs://"):
        logger.info(f"Salida HDFS detectada, no se crea directorio local: {path}")
        return
    os.makedirs(path, exist_ok=True)


def read_silver(spark, fact_base_path):
    """Lee los Silver Parquet generados por main_silver.py."""
    logger.info("Leyendo Silver Parquet...")

    def _read(name, optional=False):
        path = f"{fact_base_path}/{name}.parquet"
        try:
            df = spark.read.parquet(path)
            logger.info(f"  {name}: {df.count()} filas, {len(df.columns)} cols -- {df.columns}")
            return df
        except Exception as e:
            if optional:
                logger.warning(f"  {name}: no encontrado (opcional) -- {e}")
                return None
            raise

    return {
        "conectividad": _read("fact_conectividad"),
        "osm":          _read("fact_osm_logistica"),
        "consumo":      _read("fact_energia"),
        "demografia":   _read("fact_demografia"),
        "migracion":    _read("fact_migracion_neta"),
        "renta":        _read("fact_renta"),
        "empresas":     _read("fact_empresas_transporte"),
        "viirs":        _read("fact_viirs", optional=True),
    }


def build_gold_dataframe(spark, silver_dfs):
    """Construye el dataframe maestro desnormalizado.

    Patron exacto del referente:
      - Ancla: fact_conectividad.
      - LEFT JOINs sin filtro de columnas -> 77 columnas.
      - Filas = filas de fact_conectividad.
    """
    logger.info("Construyendo Gold (patron referente: ancla conectividad + LEFT JOINs)...")

    df_con   = silver_dfs["conectividad"]
    df_osm   = silver_dfs["osm"]
    df_elec  = silver_dfs["consumo"]
    df_dem   = silver_dfs["demografia"]
    df_mig   = silver_dfs["migracion"]
    df_ren   = silver_dfs["renta"]
    df_emp   = silver_dfs["empresas"]
    df_viirs = silver_dfs["viirs"]

    if df_viirs is not None:
        df_viirs_anual = df_viirs.drop("fecha").groupBy("muni_id", "year").mean()
        for c in df_viirs_anual.columns:
            if c.startswith("avg("):
                original = c[4:-1]
                if original.lower() in ["muni_id", "year"]:
                    df_viirs_anual = df_viirs_anual.drop(c)
                else:
                    df_viirs_anual = df_viirs_anual.withColumnRenamed(c, original)
    else:
        df_viirs_anual = None

    df_master = (
        df_con
        .join(df_osm,  on=["muni_id"],          how="left")
        .join(df_elec, on=["muni_id"],          how="left")
        .join(df_dem,  on=["muni_id", "year"],  how="left")
        .join(df_mig,  on=["muni_id", "year"],  how="left")
        .join(df_ren,  on=["muni_id", "year"],  how="left")
        .join(df_emp,  on=["muni_id", "year"],  how="left")
    )

    if df_viirs_anual is not None:
        df_master = df_master.join(df_viirs_anual, on=["muni_id", "year"], how="left")

    df_master = df_master.fillna({
        "poblacion_muni":  0.0,
        "intensidad_luz":  0.0,
        "max":             0.0,
        "min":             0.0,
    })

    count = df_master.count()
    logger.info(f"Gold construido: {count} filas, {len(df_master.columns)} columnas")
    return df_master


def main(
    fact_base_path="data/silver/fact",
    gold_base_path="data/gold",
):
    setup_logger()

    logger.info("=" * 70)
    logger.info("INICIANDO PIPELINE SILVER -> GOLD (GeoLumica)")
    logger.info(f"Timestamp: {datetime.now().isoformat()}")
    logger.info("=" * 70)

    ensure_output_dir(gold_base_path)

    logger.info("Iniciando sesion Spark...")
    configure_local_spark_env()
    spark = (
        SparkSession.builder
        .appName("GeoLumica-Gold")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.parquet.compression.codec", "snappy")
        .config("spark.driver.memory", "4g")
        .config("spark.default.parallelism", "4")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.local.ip", "127.0.0.1")
        .config("spark.hadoop.fs.defaultFS", "hdfs://localhost:9000")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    try:
        silver_dfs = read_silver(spark, fact_base_path)
        gold = build_gold_dataframe(spark, silver_dfs)

        out_path = f"{gold_base_path}/df_maestro.parquet"
        logger.info(f"Escribiendo Gold en {out_path}...")
        gold.coalesce(4).write.mode("overwrite").parquet(out_path)

        final_count = gold.count()
        logger.info(f"Gold escrita: {final_count} filas, {len(gold.columns)} columnas")
        logger.info("=" * 70)
        logger.info("PIPELINE COMPLETADO EXITOSAMENTE")
        logger.info("=" * 70)
        return True

    except Exception as e:
        logger.error(f"ERROR EN PIPELINE: {e}", exc_info=True)
        return False

    finally:
        spark.stop()
        logger.info("Sesion Spark cerrada")


if __name__ == "__main__":
    import argparse
    import sys

    parser = argparse.ArgumentParser(description="Pipeline Silver -> Gold para GeoLumica")
    parser.add_argument("--dim", default=None, help="Compatibilidad con run_pipeline.ps1; no se usa en este Gold")
    parser.add_argument("--fact", default="data/silver/fact", help="Facts Silver (ruta local o HDFS)")
    parser.add_argument("--gold", default="data/gold",        help="Salida Gold (ruta local o HDFS)")
    args = parser.parse_args()

    success = main(
        fact_base_path=args.fact,
        gold_base_path=args.gold,
    )
    sys.exit(0 if success else 1)
