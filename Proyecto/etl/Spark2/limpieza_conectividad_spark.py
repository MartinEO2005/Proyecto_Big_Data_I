from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from utils_spark import save_output

INPUT_CSV = "hdfs://namenode:9000/data/raw/transporte/conectividad_municipal_2010_2025.csv"
REF_CLEAN_CSV = "hdfs://namenode:9000/data/clean/demografia_municipios_final"
OUTPUT_CSV = "hdfs://namenode:9000/data/clean/conectividad_final_limpio"


def main():
    print("📥 Cargando conectividad municipal...")

    spark = SparkSession.builder.appName("limpieza_conectividad_spark").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    df = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(INPUT_CSV)
    )

    print("🔎 Columnas detectadas:")
    print(df.columns)

    print("🧹 Aplicando transformaciones...")

    df = df.withColumn("LAU_ID", F.lpad(F.col("LAU_ID").cast("string"), 5, "0"))
    df = df.withColumn("region_code", F.substring(F.col("LAU_ID"), 1, 2))

    df_ref = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(REF_CLEAN_CSV)
    )

    df_ref = df_ref.select("region_code", "region_name").dropDuplicates(["region_code"])
    df = df.join(df_ref, on="region_code", how="left")
    df = df.withColumnRenamed("region_name", "PROV_NAME")

    print("ℹ️ Provincias mapeadas desde el catálogo limpio.")

    df_prov_anual = (
        df.groupBy("PROV_NAME", "Anio")
        .agg(F.sum("Vehiculos_Oficial").alias("Vehiculos_Prov_Total"))
    )

    df_final = df.join(df_prov_anual, on=["PROV_NAME", "Anio"], how="left")

    df_final = df_final.withColumn(
        "Pct_Vehiculos_Muni_vs_Prov",
        F.round((F.col("Vehiculos_Oficial") / F.col("Vehiculos_Prov_Total")) * 100, 4)
    )

    cols = [
        "LAU_ID", "LAU_NAME", "Anio", "Vehiculos_Oficial",
        "Indice_Conectividad", "Poblacion_Est", "PROV_NAME",
        "Vehiculos_Prov_Total", "Pct_Vehiculos_Muni_vs_Prov"
    ]

    df_final = df_final.select(*cols)

    print("💾 Guardando resultado...")
    save_output(df_final, OUTPUT_CSV)

    print(f"📊 Filas finales: {df_final.count()}")
    print("🧾 Columnas finales:")
    print(df_final.columns)

    spark.stop()


if __name__ == "__main__":
    main()