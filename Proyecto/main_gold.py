"""
Orquestador Gold: Silver → Gold (Spark)

Lee dimensiones y facts de Silver, construye el dataframe maestro
desnormalizado y lo escribe como Parquet en Gold.
"""

import logging
import os
from datetime import datetime
from pyspark.sql import SparkSession, Window, functions as F
from pyspark.sql.types import StringType

# Mapeo prov_id (2 dígitos) → Comunidad Autónoma
_PROV_TO_REGION = {
    "01": "País Vasco",       "20": "País Vasco",       "48": "País Vasco",
    "02": "Castilla-La Mancha", "13": "Castilla-La Mancha", "16": "Castilla-La Mancha",
    "19": "Castilla-La Mancha", "45": "Castilla-La Mancha",
    "03": "Comunitat Valenciana", "12": "Comunitat Valenciana", "46": "Comunitat Valenciana",
    "04": "Andalucía", "11": "Andalucía", "14": "Andalucía", "18": "Andalucía",
    "21": "Andalucía", "23": "Andalucía", "29": "Andalucía", "41": "Andalucía",
    "05": "Castilla y León", "09": "Castilla y León", "24": "Castilla y León",
    "34": "Castilla y León", "37": "Castilla y León", "40": "Castilla y León",
    "42": "Castilla y León", "47": "Castilla y León", "49": "Castilla y León",
    "06": "Extremadura", "10": "Extremadura",
    "07": "Illes Balears",
    "08": "Cataluña", "17": "Cataluña", "25": "Cataluña", "43": "Cataluña",
    "15": "Galicia", "27": "Galicia", "32": "Galicia", "36": "Galicia",
    "22": "Aragón", "44": "Aragón", "50": "Aragón",
    "26": "La Rioja",
    "28": "Comunidad de Madrid",
    "30": "Región de Murcia",
    "31": "Comunidad Foral de Navarra",
    "33": "Principado de Asturias",
    "35": "Canarias", "38": "Canarias",
    "39": "Cantabria",
    "51": "Ciudad Autónoma de Ceuta",
    "52": "Ciudad Autónoma de Melilla",
}

logger = logging.getLogger(__name__)


def setup_logger(log_file="logs/main_gold.log"):
    os.makedirs("logs", exist_ok=True)
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler()
        ]
    )


def read_silver_components(spark, dim_base_path, fact_base_path):
    """Lee dimensiones y facts de Silver. VIIRS es opcional."""

    logger.info("Leyendo componentes de Silver...")

    # Dimensiones (requeridas)
    dim_municipio  = spark.read.parquet(f"{dim_base_path}/dim_municipio.parquet")
    dim_provincia  = spark.read.parquet(f"{dim_base_path}/dim_provincia.parquet")
    dim_fecha      = spark.read.parquet(f"{dim_base_path}/dim_fecha_anual.parquet")

    # Facts (requeridas)
    fact_demografia         = spark.read.parquet(f"{fact_base_path}/fact_demografia.parquet")
    fact_energia            = spark.read.parquet(f"{fact_base_path}/fact_energia.parquet")
    fact_renta              = spark.read.parquet(f"{fact_base_path}/fact_renta.parquet")
    fact_migracion          = spark.read.parquet(f"{fact_base_path}/fact_migracion_neta.parquet")
    fact_conectividad       = spark.read.parquet(f"{fact_base_path}/fact_conectividad.parquet")
    fact_empresas           = spark.read.parquet(f"{fact_base_path}/fact_empresas_transporte.parquet")
    fact_osm                = spark.read.parquet(f"{fact_base_path}/fact_osm_logistica.parquet")

    # VIIRS es opcional (puede no haberse descargado aún)
    viirs_path = f"{fact_base_path}/fact_viirs.parquet"
    fact_viirs = None
    # Comprueba existencia compatible con HDFS y local
    try:
        sc = spark.sparkContext
        hadoop_conf = sc._jsc.hadoopConfiguration()
        path_obj = sc._jvm.org.apache.hadoop.fs.Path(viirs_path)
        fs = path_obj.getFileSystem(hadoop_conf)
        viirs_exists = fs.exists(path_obj)
    except Exception:
        viirs_exists = os.path.exists(viirs_path)
    if viirs_exists:
        fact_viirs = spark.read.parquet(viirs_path)
        logger.info("fact_viirs cargada")
    else:
        logger.warning("fact_viirs no encontrada — se omitirá del Gold")

    return {
        "dim_municipio": dim_municipio,
        "dim_provincia": dim_provincia,
        "dim_fecha": dim_fecha,
        "fact_demografia": fact_demografia,
        "fact_energia": fact_energia,
        "fact_renta": fact_renta,
        "fact_migracion": fact_migracion,
        "fact_conectividad": fact_conectividad,
        "fact_empresas": fact_empresas,
        "fact_osm": fact_osm,
        "fact_viirs": fact_viirs,
    }


def build_gold_dataframe(spark, components):
    """Construye el dataframe maestro desnormalizado."""

    logger.info("Construyendo Gold dataframe...")

    dim_muni   = components["dim_municipio"]
    dim_prov   = components["dim_provincia"]
    dim_fecha  = components["dim_fecha"]

    fact_demo        = components["fact_demografia"]
    fact_energia     = components["fact_energia"]       # muni_id, year, consumo_kwh_total
    fact_renta       = components["fact_renta"]         # muni_id, year, renta_neta_media_euros
    fact_migracion   = components["fact_migracion"]     # muni_id, year, valor
    fact_conectividad = components["fact_conectividad"] # muni_id, year, indice_conectividad, num_vehiculos
    fact_empresas    = components["fact_empresas"]      # muni_id, year, num_empresas_total
    fact_osm         = components["fact_osm"]           # muni_id (sin year — snapshot)
    fact_viirs       = components["fact_viirs"]         # None o muni_id, year, radiancia_*

    # Renombrar valor de migración
    # Compatibilidad: facts antiguo usaba "valor", nuevo usa "saldo_migratorio_neto"
    if "valor" in fact_migracion.columns and "saldo_migratorio_neto" not in fact_migracion.columns:
        fact_migracion = fact_migracion.withColumnRenamed("valor", "saldo_migratorio_neto")

    # Cast indice_conectividad: se almacena como String en el parquet
    fact_conectividad = fact_conectividad \
        .withColumn("indice_conectividad", F.col("indice_conectividad").cast("double"))

    # 1. Base: municipios × años
    logger.info("CROSS JOIN: municipios × años...")
    base = dim_muni.crossJoin(dim_fecha).select(
        "muni_id", "muni_name", "prov_id",
        "latitude", "longitude", "area_km2",
        "year"
    )

    # 2. Añadir info de provincia
    base = base.join(
        dim_prov.select("prov_id", "prov_name"),
        on="prov_id", how="left"
    )

    # 3. JOINs con facts (todas por muni_id + year, excepto energía y osm — sin year)
    logger.info("JOINs con facts...")
    gold = base \
        .join(fact_demo,         on=["muni_id", "year"], how="left") \
        .join(fact_energia,      on="muni_id",           how="left") \
        .join(fact_renta,        on=["muni_id", "year"], how="left") \
        .join(fact_migracion,    on=["muni_id", "year"], how="left") \
        .join(fact_conectividad, on=["muni_id", "year"], how="left") \
        .join(fact_empresas,     on=["muni_id", "year"], how="left") \
        .join(fact_osm,          on="muni_id",           how="left")

    # VIIRS opcional — agregar a nivel anual (avg mensual) para evitar duplicados
    if fact_viirs is not None:
        logger.info("JOIN con fact_viirs...")
        viirs_annual = fact_viirs.groupBy("muni_id", "year").agg(
            F.avg("radiancia_media").alias("radiancia_media_anual"),
            F.avg("radiancia_max").alias("radiancia_max_anual"),
            F.avg("radiancia_min").alias("radiancia_min_anual"),
            F.avg("radiancia_stddev").alias("radiancia_stddev_anual"),
        )
        gold = gold.join(viirs_annual, on=["muni_id", "year"], how="left")

    # 4. Variables derivadas
    logger.info("Calculando variables derivadas...")

    # Densidad de población
    gold = gold.withColumn(
        "densidad_poblacion_km2",
        F.when(
            F.col("area_km2").isNotNull() & (F.col("area_km2") > 0),
            F.col("poblacion_total").cast("double") / F.col("area_km2")
        ).otherwise(F.lit(None).cast("double"))
    )

    # Consumo per cápita
    gold = gold.withColumn(
        "consumo_per_capita",
        F.when(
            F.col("poblacion_total").isNotNull() & (F.col("poblacion_total") > 0),
            F.col("consumo_kwh_total") / F.col("poblacion_total").cast("double")
        ).otherwise(F.lit(None).cast("double"))
    )

    # Consumo per km²
    gold = gold.withColumn(
        "consumo_per_km2",
        F.when(
            F.col("area_km2").isNotNull() & (F.col("area_km2") > 0),
            F.col("consumo_kwh_total") / F.col("area_km2")
        ).otherwise(F.lit(None).cast("double"))
    )

    # Densidad de empresas por 1000 hab
    gold = gold.withColumn(
        "densidad_empresas_1000hab",
        F.when(
            F.col("poblacion_total").isNotNull() & (F.col("poblacion_total") > 0),
            (F.col("num_empresas_total").cast("double") * 1000) / F.col("poblacion_total").cast("double")
        ).otherwise(F.lit(None).cast("double"))
    )

    # Crecimiento interanual de población
    window = Window.partitionBy("muni_id").orderBy("year")
    gold = gold.withColumn("poblacion_lag1", F.lag("poblacion_total").over(window)) \
               .withColumn("poblacion_lag3", F.lag("poblacion_total", 3).over(window)) \
               .withColumn(
                   "crecimiento_pob_yoy_pct",
                   F.when(
                       F.col("poblacion_lag1").isNotNull() & (F.col("poblacion_lag1") > 0),
                       ((F.col("poblacion_total").cast("double") - F.col("poblacion_lag1").cast("double"))
                        / F.col("poblacion_lag1").cast("double")) * 100
                   ).otherwise(F.lit(None).cast("double"))
               ) \
               .withColumn(
                   "crecimiento_pob_3y_pct",
                   F.when(
                       F.col("poblacion_lag3").isNotNull() & (F.col("poblacion_lag3") > 0),
                       ((F.col("poblacion_total").cast("double") - F.col("poblacion_lag3").cast("double"))
                        / F.col("poblacion_lag3").cast("double")) * 100 / 3
                   ).otherwise(F.lit(None).cast("double"))
               )

    # Renta vs media nacional del año
    renta_nacional = gold.groupBy("year").agg(
        F.avg("renta_neta_media_euros").alias("_renta_nacional_avg")
    )
    gold = gold.join(renta_nacional, on="year", how="left") \
               .withColumn(
                   "renta_vs_nacional_pct",
                   F.when(
                       F.col("_renta_nacional_avg").isNotNull() & (F.col("_renta_nacional_avg") > 0),
                       (F.col("renta_neta_media_euros") / F.col("_renta_nacional_avg")) * 100
                   ).otherwise(F.lit(None).cast("double"))
               ).drop("_renta_nacional_avg")

    # Categoría de municipio por tamaño
    gold = gold.withColumn(
        "categoria_municipio",
        F.when(F.col("poblacion_total") > 100000, "Gran ciudad")
         .when(F.col("poblacion_total") > 10000,  "Ciudad mediana")
         .when(F.col("poblacion_total") > 1000,   "Rural")
         .when(F.col("poblacion_total") > 0,       "Muy rural")
         .otherwise(F.lit(None))
    )

    # Comunidad Autónoma (region_name) derivada del prov_id
    mapping_expr = F.create_map(
        *[x for kv in _PROV_TO_REGION.items() for x in (F.lit(kv[0]), F.lit(kv[1]))]
    )
    gold = gold.withColumn("region_name", mapping_expr[F.col("prov_id")])

    # quarter: columna nula — datos anuales, no hay granularidad trimestral
    gold = gold.withColumn("quarter", F.lit(None).cast(StringType()))

    # Score de riesgo de despoblación
    gold = gold.withColumn(
        "riesgo_despoblacion_score",
        F.when(
            (F.col("crecimiento_pob_yoy_pct") < -2) & (F.col("saldo_migratorio_neto") < 0),
            F.lit(85)
        ).when(
            (F.col("crecimiento_pob_yoy_pct") < 0) | (F.col("saldo_migratorio_neto") < -100),
            F.lit(60)
        ).when(
            F.col("crecimiento_pob_yoy_pct") > 0,
            F.lit(20)
        ).otherwise(F.lit(50))
    )

    count = gold.count()
    logger.info(f"Gold dataframe construido: {count} registros")
    return gold


def select_final_columns(df):
    """Selecciona columnas finales que existen en el dataframe."""

    desired = [
        # Identificadores
        "muni_id", "muni_name", "prov_id", "prov_name", "region_name",
        # Temporal
        "year", "quarter",
        # Geografía
        "latitude", "longitude", "area_km2",
        # Demografía
        "poblacion_total", "densidad_poblacion_km2",
        "crecimiento_pob_yoy_pct", "crecimiento_pob_3y_pct",
        "poblacion_lag1", "poblacion_lag3",
        # Economía
        "renta_neta_media_euros", "renta_vs_nacional_pct",
        # Energía (snapshot — sin year)
        "consumo_kwh_total", "consumo_per_capita", "consumo_per_km2",
        # Migración
        "saldo_migratorio_neto",
        # Conectividad vial
        "num_vehiculos", "indice_conectividad",
        # OSM logística
        "num_estaciones", "num_operadores",
        "distancia_min_km", "distancia_media_km",
        "densidad_estaciones_km2", "ratio_accesibilidad",
        # Empresas transporte
        "num_empresas_total", "densidad_empresas_1000hab",
        # VIIRS (opcional)
        "radiancia_media_anual", "radiancia_max_anual",
        "radiancia_min_anual", "radiancia_stddev_anual",
        # Derivadas / scoring
        "categoria_municipio", "riesgo_despoblacion_score",
    ]

    existing = [c for c in desired if c in df.columns]
    logger.info(f"Columnas finales: {len(existing)}/{len(desired)}")
    return df.select(existing)


def main(
    dim_base_path="data/silver/dim",
    fact_base_path="data/silver/fact",
    gold_base_path="data/gold"
):
    setup_logger()

    logger.info("=" * 70)
    logger.info("INICIANDO PIPELINE SILVER → GOLD (GeoLúmica)")
    logger.info(f"Timestamp: {datetime.now().isoformat()}")
    logger.info("=" * 70)

    if not gold_base_path.startswith("hdfs://"):
        os.makedirs(gold_base_path, exist_ok=True)

    logger.info("Iniciando sesión Spark...")
    spark = SparkSession.builder \
        .appName("GeoLumica-Gold") \
        .config("spark.sql.shuffle.partitions", "1") \
        .config("spark.driver.memory", "2g") \
        .config("spark.hadoop.fs.defaultFS", "hdfs://localhost:9000") \
        .getOrCreate()

    try:
        components = read_silver_components(spark, dim_base_path, fact_base_path)
        gold = build_gold_dataframe(spark, components)
        gold_final = select_final_columns(gold)

        out_path = f"{gold_base_path}/df_maestro.parquet"
        logger.info(f"Escribiendo Gold en {out_path}...")
        gold_final.write.mode("overwrite").parquet(out_path)

        final_count = gold_final.count()
        logger.info(f"✅ Gold escrita: {final_count} registros, {len(gold_final.columns)} columnas")

        logger.info("=" * 70)
        logger.info("✅ PIPELINE COMPLETADO EXITOSAMENTE")
        logger.info("=" * 70)
        return True

    except Exception as e:
        logger.error(f"❌ ERROR EN PIPELINE: {e}", exc_info=True)
        return False

    finally:
        spark.stop()
        logger.info("Sesión Spark cerrada")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Pipeline Silver → Gold para GeoLúmica")
    parser.add_argument("--dim",  default="../data/silver/dim",  help="Dimensiones Silver")
    parser.add_argument("--fact", default="../data/silver/fact", help="Facts Silver")
    parser.add_argument("--gold", default="../data/gold",        help="Salida Gold")
    args = parser.parse_args()

    success = main(
        dim_base_path=args.dim,
        fact_base_path=args.fact,
        gold_base_path=args.gold
    )

    exit(0 if success else 1)

