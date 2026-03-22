from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import os
import sys


def move_part_file_to_final(tmp_dir, final_file):
    part_files = [f for f in os.listdir(tmp_dir) if f.startswith("part-") and f.endswith(".csv")]
    if not part_files:
        raise FileNotFoundError(f"No se encontró ningún part-*.csv en {tmp_dir}")

    src = os.path.join(tmp_dir, part_files[0])

    os.makedirs(os.path.dirname(final_file) or ".", exist_ok=True)

    if os.path.exists(final_file):
        os.remove(final_file)

    os.replace(src, final_file)

    for f in os.listdir(tmp_dir):
        fp = os.path.join(tmp_dir, f)
        if os.path.isfile(fp):
            os.remove(fp)
    os.rmdir(tmp_dir)


def main():
    print("   -> Vinculando Movilidad con nombres oficiales de Demografía...")

    input_csv = "/app/data/transporte/conectividad_municipal_2010_2025.csv"
    ref_clean_csv = "/app/data/clean/demografia_municipios_final.csv"
    output_file = "/app/data/clean/conectividad_final_limpio.csv"

    spark = SparkSession.builder.appName("limpieza_conectividad_spark").getOrCreate()

    if not os.path.exists(input_csv):
        print(f"   ❌ Error: No existe {input_csv}")
        spark.stop()
        sys.exit(1)

    df = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(input_csv)
    )

    df = df.withColumn("LAU_ID", F.lpad(F.col("LAU_ID").cast("string"), 5, "0"))
    df = df.withColumn("region_code", F.substring(F.col("LAU_ID"), 1, 2))

    if os.path.exists(ref_clean_csv):
        df_ref = (
            spark.read
            .option("header", True)
            .option("inferSchema", True)
            .csv(ref_clean_csv)
        )

        df_ref = df_ref.select("region_code", "region_name").dropDuplicates(["region_code"])
        df = df.join(df_ref, on="region_code", how="left")
        df = df.withColumnRenamed("region_name", "PROV_NAME")

        print("   ℹ️ Provincias mapeadas desde el catálogo limpio.")
    else:
        print(f"   ⚠️ ATENCIÓN: No se encontró {ref_clean_csv}. Ejecuta primero la limpieza de demografía.")
        df = df.withColumn("PROV_NAME", F.col("region_code"))

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

    tmp_output = output_file + "_tmp"

    (
        df_final.coalesce(1)
        .write
        .mode("overwrite")
        .option("header", True)
        .csv(tmp_output)
    )

    move_part_file_to_final(tmp_output, output_file)

    total_registros = df_final.count()
    print(f"   ✅ [LISTO] {total_registros} registros procesados con nombres oficiales.")
    print(f"   📁 CSV final generado en: {output_file}")

    spark.stop()


if __name__ == "__main__":
    main()