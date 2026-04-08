from pyspark.sql import SparkSession
from utils_spark import save_output

INPUT_CSV = "hdfs://namenode:9000/data/raw/empresas_transporte/empresas_transporte_prov_mun_anchos.csv"
OUTPUT_CSV = "hdfs://namenode:9000/data/clean/empresas_transporte_final_limpio"


def main():
    spark = (
        SparkSession.builder
        .appName("limpieza_empresas_transporte_spark")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    print("📥 Cargando empresas de transporte...")

    df = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(INPUT_CSV)
    )

    print("🔎 Columnas detectadas:")
    print(df.columns)

    print("🧹 Aplicando transformaciones...")

    posibles_geo = ["provincia", "nombre_provincia", "municipio", "nombre_municipio", "codigo_municipio"]
    cols_geo = [c for c in posibles_geo if c in df.columns]

    cols_anios = [c for c in df.columns if c.isdigit()]
    otras_cols = [c for c in df.columns if c not in cols_geo + cols_anios]

    columnas_finales = cols_geo + otras_cols + cols_anios
    df_final = df.select(*columnas_finales)

    sort_cols = [c for c in ["provincia", "nombre_provincia", "municipio", "nombre_municipio"] if c in df_final.columns]
    if sort_cols:
        df_final = df_final.orderBy(*sort_cols)

    print("💾 Guardando resultado...")
    save_output(df_final, OUTPUT_CSV)

    print(f"📊 Filas finales: {df_final.count()}")
    print("🧾 Columnas finales:")
    print(df_final.columns)

    spark.stop()


if __name__ == "__main__":
    main()