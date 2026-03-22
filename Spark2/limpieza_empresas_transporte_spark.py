from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import os
import shutil

INPUT_CSV = "/app/data/empresas_transporte/empresas_transporte_prov_mun_anchos.csv"
OUTPUT_CSV = "/app/data/clean/empresas_transporte_final_limpio.csv"


def write_single_csv(df, output_path):
    temp_dir = output_path + "_tmp"

    if os.path.exists(temp_dir):
        shutil.rmtree(temp_dir)
    if os.path.exists(output_path):
        os.remove(output_path)

    (
        df.coalesce(1)
        .write
        .mode("overwrite")
        .option("header", True)
        .option("encoding", "utf-8")
        .csv(temp_dir)
    )

    part_file = None
    for f in os.listdir(temp_dir):
        if f.startswith("part-") and f.endswith(".csv"):
            part_file = os.path.join(temp_dir, f)
            break

    if part_file is None:
        raise FileNotFoundError("No se encontró el archivo part-*.csv generado por Spark")

    shutil.move(part_file, output_path)
    shutil.rmtree(temp_dir)


def main():
    spark = (
        SparkSession.builder
        .appName("limpieza_empresas_transporte_spark")
        .getOrCreate()
    )

    print("📥 Cargando empresas de transporte...")

    df = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(INPUT_CSV)
    )

    print("🔎 Columnas detectadas:")
    print(df.columns)

    # Orden lógico si existen estas columnas
    posibles_geo = ["provincia", "nombre_provincia", "municipio", "nombre_municipio", "codigo_municipio"]
    cols_geo = [c for c in posibles_geo if c in df.columns]

    cols_anios = [c for c in df.columns if c.isdigit()]
    otras_cols = [c for c in df.columns if c not in cols_geo + cols_anios]

    columnas_finales = cols_geo + otras_cols + cols_anios
    df_final = df.select(*columnas_finales)

    # Ordenar si se puede
    sort_cols = [c for c in ["provincia", "nombre_provincia", "municipio", "nombre_municipio"] if c in df_final.columns]
    if sort_cols:
        df_final = df_final.orderBy(*sort_cols)

    write_single_csv(df_final, OUTPUT_CSV)

    print(f"✅ CSV guardado en: {OUTPUT_CSV}")
    print(f"📊 Filas: {df_final.count()}")
    print("🧾 Columnas finales:")
    print(df_final.columns)

    spark.stop()


if __name__ == "__main__":
    main()