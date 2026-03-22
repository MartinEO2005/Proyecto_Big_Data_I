import os
import shutil

from pyspark.sql import SparkSession
from pyspark.sql.functions import col


def guardar_csv_unico(df, salida_final):
    salida_temp = salida_final + "_tmp"

    if os.path.exists(salida_temp):
        shutil.rmtree(salida_temp)

    if os.path.exists(salida_final):
        if os.path.isdir(salida_final):
            shutil.rmtree(salida_final)
        else:
            os.remove(salida_final)

    df.coalesce(1).write.mode("overwrite").option("header", True).csv(salida_temp)

    part_file = None
    for f in os.listdir(salida_temp):
        if f.startswith("part-") and f.endswith(".csv"):
            part_file = os.path.join(salida_temp, f)
            break

    if part_file is None:
        raise FileNotFoundError("No se encontró el archivo part-*.csv generado por Spark")

    shutil.move(part_file, salida_final)
    shutil.rmtree(salida_temp)

    print(f"✅ CSV final guardado en: {salida_final}")


def main():
    spark = (
        SparkSession.builder
        .appName("Limpieza VIIRS Municipios")
        .getOrCreate()
    )

    # =========================================================
    # ENTRADA
    # =========================================================
    ruta_entrada = "/app/outputs/data/luz_nocturna/viirs_luz_nocturna.csv"
    salida_final = "/app/data/clean/viirs_municipios_limpio.csv"

    print(f"📥 Leyendo datos desde: {ruta_entrada}")

    df = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(ruta_entrada)
    )

    print("✅ Dataset cargado")
    print(f"📄 Filas iniciales: {df.count()}")
    print(f"🧱 Columnas iniciales: {len(df.columns)}")

    # =========================================================
    # TRANSFORMACIONES
    # =========================================================
    # Sustituye esta parte por tu lógica real si ya la tenías hecha.
    # Aquí solo se seleccionan columnas esperadas para dejarlo estructurado.

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

    columnas_presentes = [c for c in columnas_esperadas if c in df.columns]
    faltantes = [c for c in columnas_esperadas if c not in df.columns]

    if faltantes:
        print(f"⚠️ Faltan columnas esperadas en origen: {faltantes}")

    df_final = df.select([col(c) for c in columnas_presentes])

    print("✅ Transformación completada")
    print(f"📄 Filas finales: {df_final.count()}")
    print(f"🧱 Columnas finales: {len(df_final.columns)}")
    print(f"🧾 Columnas finales: {df_final.columns}")

    # =========================================================
    # SALIDA COMO CSV ÚNICO
    # =========================================================
    guardar_csv_unico(df_final, salida_final)

    spark.stop()
    print("🏁 Proceso VIIRS municipios finalizado correctamente.")


if __name__ == "__main__":
    main()