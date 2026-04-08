import os
import sys
import time
import subprocess
from pyspark.sql import SparkSession

SPARK_SUBMIT = "/opt/spark/bin/spark-submit"
HDFS_BASE = "hdfs://namenode:9000"


def hdfs_path(path: str) -> str:
    if path.startswith("hdfs://"):
        return path
    return f"{HDFS_BASE}{path}"


def ejecutar_spark(script_path: str, nombre: str):
    print("\n" + "=" * 60)
    print(f"➡️ Ejecutando {nombre} con Spark...")
    print("=" * 60)

    if not os.path.exists(script_path):
        print(f"❌ No se encontró el script: {script_path}")
        sys.exit(1)

    if not os.path.exists(SPARK_SUBMIT):
        print(f"❌ No se encontró spark-submit en: {SPARK_SUBMIT}")
        sys.exit(1)

    inicio = time.time()
    resultado = subprocess.run([SPARK_SUBMIT, script_path], check=False)
    duracion = round(time.time() - inicio, 2)

    if resultado.returncode != 0:
        print(f"❌ Error ejecutando {nombre} (exit code {resultado.returncode})")
        sys.exit(resultado.returncode)

    print(f"✅ {nombre} completado correctamente en {duracion} s.")


def verificar_salud_hdfs_spark(spark, ruta_hdfs: str):
    ruta = hdfs_path(ruta_hdfs)
    nombre = os.path.basename(ruta.rstrip("/"))

    try:
        df = (
            spark.read
            .option("header", True)
            .option("inferSchema", True)
            .csv(ruta)
        )
    except Exception as e:
        print(f"❌ [MISSING] {nombre} - No se pudo leer desde HDFS: {e}")
        return

    try:
        filas = df.count()
    except Exception as e:
        print(f"❌ [ERROR]   {nombre} - Error contando filas: {e}")
        return

    if filas == 0:
        print(f"❌ [VACÍO]   {nombre} - 0 filas.")
        return

    print(f"✅ [OK]      {nombre} - {filas} filas.")


def main():
    print("=" * 60)
    print("🚀 PROCESO INTEGRADO DE LIMPIEZA - PROYECTO BIG DATA 2026")
    print("=" * 60)

    scripts_spark = [
        ("/app/Proyecto/etl/Spark2/limpiezaDemografia_spark.py", "Demografía"),
        ("/app/Proyecto/etl/Spark2/limpieza_conectividad_spark.py", "Conectividad"),
        ("/app/Proyecto/etl/Spark2/limpieza_empresas_transporte_spark.py", "Empresas transporte"),
        ("/app/Proyecto/etl/Spark2/limpieza_socioeconomico_spark.py", "Bloque socioeconómico"),
        ("/app/Proyecto/etl/Spark2/limpiezaosm_spark.py", "OSM"),
        ("/app/Proyecto/etl/Spark2/limpieza_viirs_spark.py", "VIIRS provincias"),
        ("/app/Proyecto/etl/Spark2/limpieza_viirs_municipios_spark.py", "VIIRS municipios"),
    ]

    for script, nombre in scripts_spark:
        ejecutar_spark(script, nombre)

    print("\n" + "=" * 60)
    print("📊 REPORTE DE SALUD DE DATOS EN HDFS (/data/clean)")
    print("=" * 60)

    rutas_finales_hdfs = [
        "/data/clean/demografia_municipios_final",
        "/data/clean/conectividad_final_limpio",
        "/data/clean/consumo_electrico_final_limpio",
        "/data/clean/empresas_transporte_final_limpio",
        "/data/clean/migracion_municipios_final_limpio",
        "/data/clean/rentamedia_municipios_final_limpio",
        "/data/clean/muni_station_osm_limpio",
        "/data/clean/viirsFinal_limpio",
        "/data/clean/viirs_municipios_limpio",
    ]

    spark = SparkSession.builder.appName("reporte_salud_hdfs").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    for ruta in rutas_finales_hdfs:
        verificar_salud_hdfs_spark(spark, ruta)

    spark.stop()

    print("\n" + "=" * 60)
    print("✨ Proceso finalizado. Los datos están listos en HDFS.")
    print("=" * 60)


if __name__ == "__main__":
    main()