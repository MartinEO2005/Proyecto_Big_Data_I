from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import os
import shutil
import glob
import re

INPUT_CONSUMO = "/app/data/energia/consumo_electrico.csv"
OUTPUT_CONSUMO_TMP = "/app/data/clean/consumo_electrico_final_limpio_tmp"
OUTPUT_CONSUMO_CSV = "/app/data/clean/consumo_electrico_final_limpio.csv"

INPUT_MIGRACION = "/app/data/migracion/migracion_interior_municipios.csv"
OUTPUT_MIGRACION_TMP = "/app/data/clean/migracion_municipios_final_limpio_tmp"
OUTPUT_MIGRACION_CSV = "/app/data/clean/migracion_municipios_final_limpio.csv"

INPUT_RENTA = "/app/data/renta/renta_municipios.csv"
OUTPUT_RENTA_TMP = "/app/data/clean/rentamedia_municipios_final_limpio_tmp"
OUTPUT_RENTA_CSV = "/app/data/clean/rentamedia_municipios_final_limpio.csv"


def mover_part_a_csv(carpeta_tmp, archivo_final):
    partes = glob.glob(os.path.join(carpeta_tmp, "part-*.csv"))
    if not partes:
        raise FileNotFoundError(f"No se encontró ningún part-*.csv en {carpeta_tmp}")

    if os.path.exists(archivo_final):
        if os.path.isdir(archivo_final):
            shutil.rmtree(archivo_final)
        else:
            os.remove(archivo_final)

    shutil.move(partes[0], archivo_final)
    shutil.rmtree(carpeta_tmp, ignore_errors=True)


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
    print("🔌 Cargando consumo eléctrico...")
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

    df = (
        df.withColumn("Codigo", F.lpad(F.trim(F.col("Codigo").cast("string")), 5, "0"))
          .withColumn("Nombre", F.trim(F.col("Nombre")))
          .withColumn("Nombre_provincia", F.trim(F.col("Nombre_provincia")))
          .withColumn("Consumo eléctrico", F.trim(F.col("Consumo eléctrico")))
          .withColumn("Total", F.col("Total").cast("double"))
    )

    print("🔎 Ejemplos Codigo original:")
    df.select("Codigo", "Nombre", "Nombre_provincia").show(20, truncate=False)

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

    print(f"📊 Filas finales consumo: {df_final.count()}")
    print("🧾 Columnas finales consumo:")
    print(df_final.columns)

    (
        df_final.coalesce(1)
        .write
        .mode("overwrite")
        .option("header", True)
        .csv(OUTPUT_CONSUMO_TMP)
    )

    mover_part_a_csv(OUTPUT_CONSUMO_TMP, OUTPUT_CONSUMO_CSV)
    print(f"✅ CSV guardado en: {OUTPUT_CONSUMO_CSV}")


def limpiar_migracion(spark):
    print("\n" + "=" * 60)
    print("📦 Cargando migración...")
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

    if "codigo_municipio" in df.columns:
        df = df.withColumn("codigo_municipio", F.lpad(F.col("codigo_municipio").cast("string"), 5, "0"))

    if "codigo_provincia" in df.columns:
        df = df.withColumn("codigo_provincia", F.lpad(F.col("codigo_provincia").cast("string"), 2, "0"))

    if "anio" in df.columns:
        df = df.withColumn("anio", F.col("anio").cast("int"))

    if "valor" in df.columns:
        df = df.withColumn("valor", F.col("valor").cast("double"))

    columnas_a_eliminar = []
    if "codigo_provincia" in df.columns:
        columnas_a_eliminar.append("codigo_provincia")
    if "sexo" in df.columns:
        columnas_a_eliminar.append("sexo")

    if columnas_a_eliminar:
        df = df.drop(*columnas_a_eliminar)

    if "valor" in df.columns:
        df = df.withColumnRenamed("valor", "cantidad_personas")

    columnas_ordenadas = [
        "provincia",
        "nombre_municipio",
        "codigo_municipio",
        "anio",
        "nacionalidad",
        "cantidad_personas"
    ]

    columnas_presentes = [c for c in columnas_ordenadas if c in df.columns]
    df_final = df.select(*columnas_presentes)

    columnas_sort = [c for c in ["provincia", "nombre_municipio", "anio"] if c in df_final.columns]
    if columnas_sort:
        df_final = df_final.orderBy(*columnas_sort)

    if "cantidad_personas" in df_final.columns:
        n_nulos = df_final.filter(F.col("cantidad_personas").isNull()).count()
        n_ceros = df_final.filter(F.col("cantidad_personas") == 0).count()

        print("\n📌 En 'cantidad_personas':")
        print(f"   - Valores Nulos (NaN): {n_nulos}")
        print(f"   - Valores Cero (0):   {n_ceros}")

        df_final = df_final.filter(F.col("cantidad_personas").isNotNull())

        print(f"\n🧽 Filas eliminadas por NaN en migración: {n_nulos}")
        print(f"📌 Nuevo tamaño df_final: {df_final.count()}")

    (
        df_final.coalesce(1)
        .write
        .mode("overwrite")
        .option("header", True)
        .csv(OUTPUT_MIGRACION_TMP)
    )

    mover_part_a_csv(OUTPUT_MIGRACION_TMP, OUTPUT_MIGRACION_CSV)
    print(f"✅ CSV guardado en: {OUTPUT_MIGRACION_CSV}")
    print(f"📊 Filas finales: {df_final.count()}")


def limpiar_renta(spark):
    print("\n" + "=" * 60)
    print("💰 Cargando renta / PIB municipal...")
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

    if "codigo_municipio" in df.columns:
        df = df.withColumn("codigo_municipio", F.lpad(F.col("codigo_municipio").cast("string"), 5, "0"))

    if "anio" in df.columns:
        df = df.withColumn("anio", F.col("anio").cast("int"))

    if "valor" in df.columns:
        df = df.withColumn("valor", F.col("valor").cast("double"))

    if "valor" in df.columns:
        df = df.withColumnRenamed("valor", "pib")

    columnas_ordenadas = [
        "provincia",
        "nombre_municipio",
        "codigo_municipio",
        "anio",
        "pib"
    ]

    columnas_presentes = [c for c in columnas_ordenadas if c in df.columns]
    df_final = df.select(*columnas_presentes)

    columnas_sort = [c for c in ["provincia", "nombre_municipio", "anio"] if c in df_final.columns]
    if columnas_sort:
        df_final = df_final.orderBy(*columnas_sort)

    if "pib" in df_final.columns:
        n_nulos = df_final.filter(F.col("pib").isNull()).count()
        n_ceros = df_final.filter(F.col("pib") == 0).count()

        print("\n📌 En 'pib':")
        print(f"   - Valores Nulos (NaN): {n_nulos}")
        print(f"   - Valores Cero (0):   {n_ceros}")

        df_final = df_final.filter(F.col("pib").isNotNull())

        print(f"\n🧽 Filas eliminadas por NaN en renta: {n_nulos}")
        print(f"📌 Nuevo tamaño df_final: {df_final.count()}")

    (
        df_final.coalesce(1)
        .write
        .mode("overwrite")
        .option("header", True)
        .csv(OUTPUT_RENTA_TMP)
    )

    mover_part_a_csv(OUTPUT_RENTA_TMP, OUTPUT_RENTA_CSV)
    print(f"✅ CSV guardado en: {OUTPUT_RENTA_CSV}")
    print(f"📊 Filas finales: {df_final.count()}")


def main():
    spark = SparkSession.builder.appName("limpieza_bloque_socioeconomico").getOrCreate()

    limpiar_consumo(spark)
    limpiar_migracion(spark)
    limpiar_renta(spark)

    print("\n" + "=" * 60)
    print("✨ Bloque socioeconómico finalizado.")
    print("=" * 60)

    spark.stop()


if __name__ == "__main__":
    main()