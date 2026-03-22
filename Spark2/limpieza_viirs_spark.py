from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import os
import shutil

INPUT_CSV = "/app/data/luz_nocturna/provincias/viirs_provincias_2018_2022.csv"
OUTPUT_CSV = "/app/data/clean/viirsFinal_limpio.csv"


def save_as_single_csv(df, output_csv_path):
    tmp_dir = output_csv_path + "_tmp"

    if os.path.exists(tmp_dir):
        shutil.rmtree(tmp_dir)
    if os.path.exists(output_csv_path):
        os.remove(output_csv_path)

    (
        df.coalesce(1)
        .write
        .mode("overwrite")
        .option("header", True)
        .csv(tmp_dir)
    )

    part_file = None
    for f in os.listdir(tmp_dir):
        if f.startswith("part-") and f.endswith(".csv"):
            part_file = os.path.join(tmp_dir, f)
            break

    if part_file is None:
        raise FileNotFoundError("No se encontró el part-*.csv generado por Spark")

    shutil.move(part_file, output_csv_path)
    shutil.rmtree(tmp_dir)


def main():
    spark = SparkSession.builder.appName("limpieza_viirs_spark").getOrCreate()

    print("📥 Cargando VIIRS provincias...")

    df = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(INPUT_CSV)
    )

    print("🔎 Columnas detectadas:")
    print(df.columns)

    # Renombrado estándar si existen esas columnas
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

    for old, new in rename_map.items():
        if old in df.columns and old != new:
            df = df.withColumnRenamed(old, new)

    # Mantener todas las columnas, pero ordenar primero las más importantes
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

    # Filtrar filas vacías si aplica
    if "nombre_provincia" in df.columns:
        df = df.filter(col("nombre_provincia").isNotNull())

    # Orden lógico
    orden = [c for c in ["nombre_provincia", "anio", "mes"] if c in df.columns]
    if orden:
        df = df.orderBy(*orden)

    save_as_single_csv(df, OUTPUT_CSV)

    print(f"✅ CSV guardado en: {OUTPUT_CSV}")
    print(f"📊 Filas: {df.count()}")
    print("🧾 Columnas finales:")
    print(df.columns)

    spark.stop()


if __name__ == "__main__":
    main()