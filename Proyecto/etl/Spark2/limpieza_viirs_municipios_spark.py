from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from utils_spark import save_output


def main():
    spark = (
        SparkSession.builder
        .appName("limpieza_viirs_municipios_spark")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    ruta_entrada = "hdfs://namenode:9000/data/raw/luz_nocturna/municipios/luz_nocturna/viirs_luz_nocturna.csv"
    salida_final = "hdfs://namenode:9000/data/clean/viirs_municipios_limpio"

    print("📥 Cargando VIIRS municipios...")

    df = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(ruta_entrada)
    )

    print(f"📄 Filas iniciales: {df.count()}")
    print(f"🧱 Columnas iniciales: {len(df.columns)}")
    print("🔎 Columnas detectadas:")
    print(df.columns)

    columnas_esperadas = [
        "lau_id",
        "lau_name",
        "area_km2",
        "pop_2023",
        "year",
        "date",
        "mean",
        "min",
        "max",
        "stddev",
        "cntr_code",
        "gisco_id",
    ]

    print("🧹 Aplicando transformaciones...")

    columnas_presentes = [c for c in columnas_esperadas if c in df.columns]
    faltantes = [c for c in columnas_esperadas if c not in df.columns]

    if faltantes:
        print(f"⚠️ Faltan columnas esperadas en origen: {faltantes}")

    df_final = df.select([col(c) for c in columnas_presentes])

    print("💾 Guardando resultado...")
    save_output(df_final, salida_final)

    print(f"📄 Filas finales: {df_final.count()}")
    print(f"🧱 Columnas finales: {len(df_final.columns)}")
    print("🧾 Columnas finales:")
    print(df_final.columns)

    spark.stop()
    print("✅ Proceso VIIRS municipios finalizado correctamente.")


if __name__ == "__main__":
    main()