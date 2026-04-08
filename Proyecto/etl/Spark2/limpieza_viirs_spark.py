from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from utils_spark import save_output

INPUT_CSV = "hdfs://namenode:9000/data/raw/luz_nocturna/provincias/viirs_provincias_2018_2022.csv"
OUTPUT_CSV = "hdfs://namenode:9000/data/clean/viirsFinal_limpio"


def main():
    spark = SparkSession.builder.appName("limpieza_viirs_spark").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    print("📥 Cargando VIIRS provincias...")

    df = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(INPUT_CSV)
    )

    print("🔎 Columnas detectadas:")
    print(df.columns)

    rename_map = {
        "year": "anio",
        "month": "mes",
        "prov_name": "nombre_provincia",
        "province": "nombre_provincia",
        "provincia": "nombre_provincia",
        "province_code": "codigo_provincia",
        "codigo_provincia": "codigo_provincia",
        "mean_rad": "media_radiancia",
        "avg_rad": "media_radiancia",
        "sum_rad": "suma_radiancia",
        "total_rad": "suma_radiancia",
        "min_rad": "min_radiancia",
        "max_rad": "max_radiancia",
        "std_rad": "std_radiancia",
        "median_rad": "mediana_radiancia",
        "n_pixels": "num_pixeles",
        "pixels": "num_pixeles"
    }

    print("🧹 Aplicando transformaciones...")

    for old, new in rename_map.items():
        if old in df.columns and old != new:
            df = df.withColumnRenamed(old, new)

    cols_prioridad = [
        "codigo_provincia",
        "nombre_provincia",
        "anio",
        "mes",
        "media_radiancia",
        "suma_radiancia",
        "min_radiancia",
        "max_radiancia",
        "std_radiancia",
        "mediana_radiancia",
        "num_pixeles"
    ]

    cols_existentes = [c for c in cols_prioridad if c in df.columns]
    resto = [c for c in df.columns if c not in cols_existentes]

    df = df.select(*(cols_existentes + resto))

    if "nombre_provincia" in df.columns:
        df = df.filter(col("nombre_provincia").isNotNull())

    orden = [c for c in ["nombre_provincia", "anio", "mes"] if c in df.columns]
    if orden:
        df = df.orderBy(*orden)

    print("💾 Guardando resultado...")
    save_output(df, OUTPUT_CSV)

    print(f"📊 Filas finales: {df.count()}")
    print("🧾 Columnas finales:")
    print(df.columns)

    spark.stop()


if __name__ == "__main__":
    main()