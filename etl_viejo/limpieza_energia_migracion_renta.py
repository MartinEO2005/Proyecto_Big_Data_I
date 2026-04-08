from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import os
import shutil
import glob

INPUT_CSV = "/app/data/migracion/migracion_interior_municipios.csv"
OUTPUT_TMP = "/app/data/clean/migracion_municipios_final_limpio_tmp"
OUTPUT_CSV = "/app/data/clean/migracion_municipios_final_limpio.csv"


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


def main():
    spark = SparkSession.builder.appName("limpieza_migracion_spark").getOrCreate()

    if not os.path.exists(INPUT_CSV):
        print(f"❌ No existe {INPUT_CSV}")
        spark.stop()
        return

    print("📥 Cargando migración...")
    df = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(INPUT_CSV)
    )

    print(f"📊 Filas iniciales: {df.count()}")
    print("🔎 Columnas detectadas:")
    print(df.columns)

    # Tipos y limpieza básica
    if "codigo_municipio" in df.columns:
        df = df.withColumn("codigo_municipio", F.lpad(F.col("codigo_municipio").cast("string"), 5, "0"))

    if "codigo_provincia" in df.columns:
        df = df.withColumn("codigo_provincia", F.lpad(F.col("codigo_provincia").cast("string"), 2, "0"))

    if "anio" in df.columns:
        df = df.withColumn("anio", F.col("anio").cast("int"))

    if "valor" in df.columns:
        df = df.withColumn("valor", F.col("valor").cast("double"))

    # Mantener lógica antigua: quitar columnas
    columnas_a_eliminar = []
    if "codigo_provincia" in df.columns:
        columnas_a_eliminar.append("codigo_provincia")
    if "sexo" in df.columns:
        columnas_a_eliminar.append("sexo")

    if columnas_a_eliminar:
        df = df.drop(*columnas_a_eliminar)

    # Renombrar valor
    if "valor" in df.columns:
        df = df.withColumnRenamed("valor", "cantidad_personas")

    # Orden de columnas como en el script antiguo
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

    # Orden final
    columnas_sort = [c for c in ["provincia", "nombre_municipio", "anio"] if c in df_final.columns]
    if columnas_sort:
        df_final = df_final.orderBy(*columnas_sort)

    print("\n✅ DataFrame reorganizado y simplificado:")
    df_final.show(5, truncate=False)

    # Conteo de nulos por columna
    print("\n🔎 Conteo de valores nulos por columna:")
    for c in df_final.columns:
        nulos = df_final.filter(F.col(c).isNull()).count()
        print(f"   - {c}: {nulos}")

    # Conteo de nulos y ceros en cantidad_personas
    if "cantidad_personas" in df_final.columns:
        n_nulos = df_final.filter(F.col("cantidad_personas").isNull()).count()
        n_ceros = df_final.filter(F.col("cantidad_personas") == 0).count()

        print("\n📌 En 'cantidad_personas':")
        print(f"   - Valores Nulos (NaN): {n_nulos}")
        print(f"   - Valores Cero (0):   {n_ceros}")

        # Mantener lógica antigua: eliminar filas con nulo en cantidad
        df_final = df_final.filter(F.col("cantidad_personas").isNotNull())

        print(f"\n🧽 Filas eliminadas por NaN en migración: {n_nulos}")
        print(f"📌 Nuevo tamaño df_final: {df_final.count()}")

    # Guardar en un único CSV
    (
        df_final.coalesce(1)
        .write
        .mode("overwrite")
        .option("header", True)
        .csv(OUTPUT_TMP)
    )

    mover_part_a_csv(OUTPUT_TMP, OUTPUT_CSV)

    print(f"\n✅ CSV guardado en: {OUTPUT_CSV}")
    print(f"📊 Filas finales: {df_final.count()}")

    spark.stop()


if __name__ == "__main__":
    main()