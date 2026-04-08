from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import re
from utils_spark import save_output

INPUT_CONSUMO = "hdfs://namenode:9000/data/raw/energia/consumo_electrico.csv"
OUTPUT_CONSUMO_CSV = "hdfs://namenode:9000/data/clean/consumo_electrico_final_limpio"

INPUT_MIGRACION = "hdfs://namenode:9000/data/raw/migracion/migracion_interior_municipios.csv"
OUTPUT_MIGRACION_CSV = "hdfs://namenode:9000/data/clean/migracion_municipios_final_limpio"

INPUT_RENTA = "hdfs://namenode:9000/data/raw/renta/renta_municipios.csv"
OUTPUT_RENTA_CSV = "hdfs://namenode:9000/data/clean/rentamedia_municipios_final_limpio"


def safe_colname(name: str) -> str:
    name = name.strip().lower()
    name = (
        name.replace("á", "a")
        .replace("é", "e")
        .replace("í", "i")
        .replace("ó", "o")
        .replace("ú", "u")
        .replace("ü", "u")
        .replace("ñ", "n")
    )
    name = re.sub(r"[^\w]+", "_", name)
    name = re.sub(r"_+", "_", name).strip("_")
    return name


def limpiar_consumo(spark):
    print("\n" + "=" * 60)
    print("📥 Cargando consumo eléctrico...")
    print("=" * 60)

    df = (
        spark.read
        .option("header", True)
        .option("sep", ";")
        .option("inferSchema", True)
        .csv(INPUT_CONSUMO)
    )

    print(f"📊 Filas iniciales: {df.count()}")
    print("🔎 Columnas detectadas:")
    print(df.columns)

    print("🧹 Aplicando transformaciones...")

    df = (
        df.withColumn("Codigo", F.lpad(F.trim(F.col("Codigo").cast("string")), 5, "0"))
          .withColumn("Nombre", F.trim(F.col("Nombre")))
          .withColumn("Nombre_provincia", F.trim(F.col("Nombre_provincia")))
          .withColumn("Consumo eléctrico", F.trim(F.col("Consumo eléctrico")))
          .withColumn("Total", F.col("Total").cast("double"))
    )

    df_pivot = (
        df.groupBy("Codigo", "Nombre", "Nombre_provincia")
          .pivot("Consumo eléctrico")
          .agg(F.first("Total"))
    )

    columnas_base = {"Codigo", "Nombre", "Nombre_provincia"}
    for c in df_pivot.columns:
        if c not in columnas_base:
            df_pivot = df_pivot.withColumnRenamed(c, safe_colname(c))

    df_final = (
        df_pivot
        .withColumnRenamed("Codigo", "codigo_municipio")
        .withColumnRenamed("Nombre", "nombre_municipio")
        .withColumnRenamed("Nombre_provincia", "nombre_provincia")
        .orderBy("nombre_provincia", "nombre_municipio")
    )

    print("💾 Guardando resultado...")
    save_output(df_final, OUTPUT_CONSUMO_CSV)

    print(f"📊 Filas finales: {df_final.count()}")
    print("🧾 Columnas finales:")
    print(df_final.columns)


def limpiar_migracion(spark):
    print("\n" + "=" * 60)
    print("📥 Cargando migración...")
    print("=" * 60)

    df = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(INPUT_MIGRACION)
    )

    print(f"📊 Filas iniciales: {df.count()}")
    print("🔎 Columnas detectadas:")
    print(df.columns)

    print("🧹 Aplicando transformaciones...")

    if "codigo_municipio" in df.columns:
        df = df.withColumn("codigo_municipio", F.lpad(F.col("codigo_municipio").cast("string"), 5, "0"))

    if "codigo_provincia" in df.columns:
        df = df.withColumn("codigo_provincia", F.lpad(F.col("codigo_provincia").cast("string"), 2, "0"))

    if "anio" in df.columns:
        df = df.withColumn("anio", F.col("anio").cast("int"))

    if "valor" in df.columns:
        df = df.withColumn("valor", F.col("valor").cast("double"))
        df = df.withColumnRenamed("valor", "cantidad_personas")

    columnas_a_eliminar = []
    if "codigo_provincia" in df.columns:
        columnas_a_eliminar.append("codigo_provincia")
    if "sexo" in df.columns:
        columnas_a_eliminar.append("sexo")
    if columnas_a_eliminar:
        df = df.drop(*columnas_a_eliminar)

    columnas_ordenadas = [
        "provincia", "nombre_municipio", "codigo_municipio",
        "anio", "nacionalidad", "cantidad_personas"
    ]

    columnas_presentes = [c for c in columnas_ordenadas if c in df.columns]
    df_final = df.select(*columnas_presentes)

    columnas_sort = [c for c in ["provincia", "nombre_municipio", "anio"] if c in df_final.columns]
    if columnas_sort:
        df_final = df_final.orderBy(*columnas_sort)

    if "cantidad_personas" in df_final.columns:
        df_final = df_final.filter(F.col("cantidad_personas").isNotNull())

    print("💾 Guardando resultado...")
    save_output(df_final, OUTPUT_MIGRACION_CSV)

    print(f"📊 Filas finales: {df_final.count()}")
    print("🧾 Columnas finales:")
    print(df_final.columns)


def limpiar_renta(spark):
    print("\n" + "=" * 60)
    print("📥 Cargando renta / PIB municipal...")
    print("=" * 60)

    df = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(INPUT_RENTA)
    )

    print(f"📊 Filas iniciales: {df.count()}")
    print("🔎 Columnas detectadas:")
    print(df.columns)

    print("🧹 Aplicando transformaciones...")

    if "codigo_municipio" in df.columns:
        df = df.withColumn("codigo_municipio", F.lpad(F.col("codigo_municipio").cast("string"), 5, "0"))

    if "anio" in df.columns:
        df = df.withColumn("anio", F.col("anio").cast("int"))

    if "valor" in df.columns:
        df = df.withColumn("valor", F.col("valor").cast("double"))
        df = df.withColumnRenamed("valor", "pib")

    columnas_ordenadas = [
        "provincia", "nombre_municipio", "codigo_municipio", "anio", "pib"
    ]

    columnas_presentes = [c for c in columnas_ordenadas if c in df.columns]
    df_final = df.select(*columnas_presentes)

    columnas_sort = [c for c in ["provincia", "nombre_municipio", "anio"] if c in df_final.columns]
    if columnas_sort:
        df_final = df_final.orderBy(*columnas_sort)

    if "pib" in df_final.columns:
        df_final = df_final.filter(F.col("pib").isNotNull())

    print("💾 Guardando resultado...")
    save_output(df_final, OUTPUT_RENTA_CSV)

    print(f"📊 Filas finales: {df_final.count()}")
    print("🧾 Columnas finales:")
    print(df_final.columns)


def main():
    spark = SparkSession.builder.appName("limpieza_bloque_socioeconomico").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    limpiar_consumo(spark)
    limpiar_migracion(spark)
    limpiar_renta(spark)

    print("\n" + "=" * 60)
    print("✨ Bloque socioeconómico finalizado.")
    print("=" * 60)

    spark.stop()


if __name__ == "__main__":
    main()