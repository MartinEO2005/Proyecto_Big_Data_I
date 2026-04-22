"""
Generación de fact_satelital desde el catálogo de productos Sentinel-2 (Copernicus).

Fuente: data/raw/satelital/sentinel2_products.csv
Salida: fact_satelital.parquet

Esquema:
  - product_id   : str  — ID único del producto (columna Id del catálogo)
  - product_name : str  — Nombre del fichero .SAFE
  - tile_code    : str  — Código MGRS del tile (ej: T30TUK)
  - level        : str  — Nivel de procesamiento (L1C o L2A)
  - date         : str  — Fecha de adquisición (YYYY-MM-DD)
  - size_mb      : float — Tamaño del producto en MB
  - online       : bool — Si el producto está disponible online
"""

import logging
import os as _os
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, BooleanType

logger = logging.getLogger(__name__)


def _local(path):
    """Prefija file:// para que Spark no resuelva el path como HDFS."""
    return f"file://{_os.path.abspath(path)}"


def create_fact_satelital(spark, raw_path, output_path):
    """
    Lee sentinel2_products.csv y genera fact_satelital.parquet.

    Columnas del CSV origen usadas:
      Id, Name, ContentLength, ContentDate.Start, Online
    """
    logger.info("Generando fact_satelital...")

    catalog_csv = _os.path.join(raw_path, "satelital", "sentinel2_products.csv")
    if not _os.path.exists(catalog_csv):
        raise FileNotFoundError(f"Catálogo Sentinel-2 no encontrado: {catalog_csv}")

    try:
        df = spark.read.csv(
            _local(catalog_csv),
            header=True,
            inferSchema=False
        )

        # Extraer tile_code de Name: patrón T\d{2}[A-Z]{3} (ej: T30TUK)
        # Extraer level: MSIL1C → L1C, MSIL2A → L2A
        df_clean = (
            df
            .withColumn("product_id",
                F.col("Id").cast(StringType()))
            .withColumn("product_name",
                F.col("Name").cast(StringType()))
            .withColumn("tile_code",
                F.regexp_extract(F.col("Name"), r"_(T\d{2}[A-Z]{3})_", 1))
            .withColumn("level",
                F.when(F.col("Name").contains("MSIL1C"), F.lit("L1C"))
                 .when(F.col("Name").contains("MSIL2A"), F.lit("L2A"))
                 .otherwise(F.lit("UNKNOWN")))
            .withColumn("date",
                F.to_date(F.substring(F.col("`ContentDate.Start`"), 1, 19), "yyyy-MM-dd'T'HH:mm:ss").cast(StringType()))
            .withColumn("size_mb",
                (F.col("ContentLength").cast(DoubleType()) / 1_048_576).cast(DoubleType()))
            .withColumn("online",
                F.when(F.upper(F.col("Online")) == "TRUE", F.lit(True))
                 .otherwise(F.lit(False)))
            .select(
                "product_id",
                "product_name",
                "tile_code",
                "level",
                "date",
                "size_mb",
                "online"
            )
            .filter(F.col("product_id").isNotNull())
            .dropDuplicates(["product_id"])
        )

        df_clean.write.mode("overwrite").parquet(output_path)
        count = df_clean.count()
        logger.info(f"fact_satelital: {count} productos escritos → {output_path}")
        return df_clean

    except Exception as e:
        logger.error(f"Error en fact_satelital: {e}")
        raise
