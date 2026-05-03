"""
Generación de dimensiones para Silver layer.
Produce: dim_municipio, dim_provincia, dim_fecha_anual
"""

import logging
import os
import json
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType
)

logger = logging.getLogger(__name__)


def create_dim_municipio(spark, geojson_path, output_path):
    """Genera dim_municipio desde el GeoJSON de municipios españoles."""
    logger.info(f"Generando dim_municipio desde {geojson_path}")

    with open(geojson_path, encoding="utf-8") as f:
        gj = json.load(f)

    rows = []
    for feat in gj["features"]:
        p = feat.get("properties", {})
        geom = feat.get("geometry", {})
        coords = geom.get("coordinates", []) if geom else []

        # Centroide aproximado del primer anillo
        lat, lon = None, None
        try:
            if geom["type"] == "Polygon":
                ring = coords[0]
            elif geom["type"] == "MultiPolygon":
                ring = coords[0][0]
            else:
                ring = []
            if ring:
                lon = sum(c[0] for c in ring) / len(ring)
                lat = sum(c[1] for c in ring) / len(ring)
        except Exception:
            pass

        muni_id   = str(p.get("LAU_ID", p.get("muni_id", ""))).zfill(5)
        prov_id   = muni_id[:2] if len(muni_id) >= 2 else ""
        muni_name = p.get("LAU_NAME", p.get("muni_name", ""))
        area_km2  = float(p.get("AREA_KM2", p.get("area_km2", 0) or 0))

        rows.append((muni_id, muni_name, prov_id, lat, lon, area_km2))

    schema = StructType([
        StructField("muni_id",   StringType(),  False),
        StructField("muni_name", StringType(),  True),
        StructField("prov_id",   StringType(),  True),
        StructField("latitude",  DoubleType(),  True),
        StructField("longitude", DoubleType(),  True),
        StructField("area_km2",  DoubleType(),  True),
    ])

    df = spark.createDataFrame(rows, schema) \
              .filter(F.col("muni_id") != "") \
              .dropDuplicates(["muni_id"])

    df.write.mode("overwrite").parquet(output_path)
    logger.info(f"dim_municipio: {df.count()} registros → {output_path}")
    return df


def create_dim_provincia(spark, geojson_path, output_path):
    """Genera dim_provincia desde el GeoJSON (agrupando por prov_id)."""
    logger.info("Generando dim_provincia...")

    with open(geojson_path, encoding="utf-8") as f:
        gj = json.load(f)

    INE_PROV = {
        "01":"Álava","02":"Albacete","03":"Alicante","04":"Almería","05":"Ávila",
        "06":"Badajoz","07":"Islas Baleares","08":"Barcelona","09":"Burgos","10":"Cáceres",
        "11":"Cádiz","12":"Castellón","13":"Ciudad Real","14":"Córdoba","15":"A Coruña",
        "16":"Cuenca","17":"Girona","18":"Granada","19":"Guadalajara","20":"Gipuzkoa",
        "21":"Huelva","22":"Huesca","23":"Jaén","24":"León","25":"Lleida",
        "26":"La Rioja","27":"Lugo","28":"Madrid","29":"Málaga","30":"Murcia",
        "31":"Navarra","32":"Ourense","33":"Asturias","34":"Palencia","35":"Las Palmas",
        "36":"Pontevedra","37":"Salamanca","38":"Santa Cruz de Tenerife","39":"Cantabria",
        "40":"Segovia","41":"Sevilla","42":"Soria","43":"Tarragona","44":"Teruel",
        "45":"Toledo","46":"Valencia","47":"Valladolid","48":"Bizkaia","49":"Zamora",
        "50":"Zaragoza","51":"Ceuta","52":"Melilla",
    }

    seen = set()
    rows = []
    for feat in gj["features"]:
        p = feat.get("properties", {})
        muni_id = str(p.get("LAU_ID", "")).zfill(5)
        prov_id = muni_id[:2]
        if prov_id and prov_id not in seen:
            seen.add(prov_id)
            prov_name = INE_PROV.get(prov_id, p.get("PROV_NAME", ""))
            rows.append((prov_id, prov_name))

    schema = StructType([
        StructField("prov_id",   StringType(), False),
        StructField("prov_name", StringType(), True),
    ])
    df = spark.createDataFrame(rows, schema)
    df.write.mode("overwrite").parquet(output_path)
    logger.info(f"dim_provincia: {df.count()} registros → {output_path}")
    return df


def create_dim_fecha_anual(spark, output_path, year_start=1995, year_end=2025):
    """Genera dim_fecha_anual con un rango de años."""
    logger.info("Generando dim_fecha_anual...")
    rows = [(y,) for y in range(year_start, year_end + 1)]
    schema = StructType([StructField("year", IntegerType(), False)])
    df = spark.createDataFrame(rows, schema)
    df.write.mode("overwrite").parquet(output_path)
    logger.info(f"dim_fecha_anual: {df.count()} registros → {output_path}")
    return df


def main_dimensions(spark, geojson_path, dim_base_path):
    """Orquesta la creación de las 3 dimensiones."""
    logger.info("=" * 60)
    logger.info("INICIANDO GENERACIÓN DE DIMENSIONES (Silver)")
    logger.info("=" * 60)

    create_dim_municipio(
        spark, geojson_path,
        f"{dim_base_path}/dim_municipio.parquet"
    )
    create_dim_provincia(
        spark, geojson_path,
        f"{dim_base_path}/dim_provincia.parquet"
    )
    create_dim_fecha_anual(
        spark,
        f"{dim_base_path}/dim_fecha_anual.parquet"
    )

    logger.info("✅ Dimensiones generadas")
