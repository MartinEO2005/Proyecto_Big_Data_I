import os
import sys
import subprocess
import pandas as pd

SPARK_SUBMIT = "/opt/spark/bin/spark-submit"
BASE_DIR = "/app"


def verificar_salud_csv(ruta):
    ruta_abs = os.path.join(BASE_DIR, ruta)

    if not os.path.exists(ruta_abs):
        print(f"❌ [MISSING] {os.path.basename(ruta)} - El archivo no se generó.")
        return

    if os.path.isdir(ruta_abs):
        print(f"⚠️ [DIRECTORIO] {os.path.basename(ruta)} - Spark guardó una carpeta en vez de un CSV único.")
        try:
            contenido = os.listdir(ruta_abs)
            part_files = [f for f in contenido if f.startswith("part-") and f.endswith(".csv")]
            if part_files:
                print(f"   ↳ CSV Spark detectado: {part_files[0]}")
            else:
                print("   ↳ No se encontró ningún part-*.csv dentro del directorio.")
        except Exception as e:
            print(f"   ↳ No se pudo inspeccionar el directorio: {e}")
        return

    try:
        df = pd.read_csv(ruta_abs)
    except Exception as e:
        print(f"❌ [ERROR]   {os.path.basename(ruta)} - No se pudo leer: {e}")
        return

    if len(df) == 0:
        print(f"❌ [VACÍO]   {os.path.basename(ruta)} - 0 filas.")
        return

    nulos = int(df.isna().sum().sum())

    if nulos == 0:
        print(f"✅ [OK]      {os.path.basename(ruta)} - {len(df)} filas.")
    else:
        col_con_nulos = df.columns[df.isnull().any()].tolist()
        print(f"⚠️ [NULOS]   {os.path.basename(ruta)} - {nulos} nulos en: {col_con_nulos}")


def ejecutar_spark(script_path, nombre):
    print(f"\n➡️ Ejecutando {nombre} con Spark...")

    if not os.path.exists(script_path):
        print(f"❌ No se encontró el script: {script_path}")
        sys.exit(1)

    if not os.path.exists(SPARK_SUBMIT):
        print(f"❌ No se encontró spark-submit en: {SPARK_SUBMIT}")
        sys.exit(1)

    resultado = subprocess.run([SPARK_SUBMIT, script_path], check=False)

    if resultado.returncode != 0:
        print(f"❌ Error ejecutando {nombre} (exit code {resultado.returncode})")
        sys.exit(resultado.returncode)

    print(f"✅ {nombre} completado correctamente.")


def main():
    print("=" * 60)
    print("🚀 PROCESO INTEGRADO DE LIMPIEZA - PROYECTO BIG DATA 2026")
    print("=" * 60)

    scripts_spark = [
        ("/app/Spark2/limpiezaDemografia_spark.py", "Demografía"),
        ("/app/Spark2/limpieza_conectividad_spark.py", "Conectividad"),
        ("/app/Spark2/limpieza_empresas_transporte_spark.py", "Empresas transporte"),
        ("/app/Spark2/limpieza_socioeconomico_spark.py", "Bloque socioeconómico"),
        ("/app/Spark2/limpiezaosm_spark.py", "OSM"),
        ("/app/Spark2/limpieza_viirs_spark.py", "VIIRS provincias"),
        ("/app/Spark2/limpieza_viirs_municipios_spark.py", "VIIRS municipios"),
    ]

    for script, nombre in scripts_spark:
        ejecutar_spark(script, nombre)

    print("\n" + "=" * 60)
    print("📊 REPORTE DE SALUD DE DATOS (data/clean/)")
    print("=" * 60)

    archivos_finales = [
        "data/clean/demografia_municipios_final.csv",
        "data/clean/conectividad_final_limpio.csv",
        "data/clean/consumo_electrico_final_limpio.csv",
        "data/clean/empresas_transporte_final_limpio.csv",
        "data/clean/migracion_municipios_final_limpio.csv",
        "data/clean/rentamedia_municipios_final_limpio.csv",
        "data/clean/muni_station_osm_limpio.csv",
        "data/clean/viirsFinal_limpio.csv",
        "data/clean/viirs_municipios_limpio.csv",
    ]

    for ruta in archivos_finales:
        verificar_salud_csv(ruta)

    print("\n" + "=" * 60)
    print("✨ Proceso finalizado. Los datos están listos.")
    print("=" * 60)


if __name__ == "__main__":
    main()