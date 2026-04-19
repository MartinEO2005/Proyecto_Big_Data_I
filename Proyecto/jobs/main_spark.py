import sys
import os

# 1. Le decimos a Spark dónde está exactamente tu Python de Windows
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

# 2. Forzamos a Python a reconocer la carpeta raíz
sys.path.append(os.getcwd())

from pyspark.sql import SparkSession

# ... (aquí siguen tus imports de silver y gold igual que antes)
from Proyecto.jobs.silver.silver_demografia import process_demography
from Proyecto.jobs.silver.silver_viirs import process_viirs
from Proyecto.jobs.silver.silver_osm import process_osm
from Proyecto.jobs.silver.silver_conectividad import process_conectividad
from Proyecto.jobs.silver.silver_empresas import process_empresas
from Proyecto.jobs.silver.silver_socioeconomico import run_socioeconomico
from Proyecto.jobs.gold.gold_feature_store import build_master_features

def main():
    spark = SparkSession.builder \
        .appName("GeoLumica_Master_Pipeline") \
        .config("spark.sql.parquet.compression.codec", "snappy") \
        .getOrCreate()

    print("🚀 Iniciando Pipeline E2E de GeoLúmica...")

    # Rutas Base
    RAW_BASE = "./data/raw"
    SILVER_BASE = "./data/silver"
    GOLD_BASE = "./data/gold"

    # Diccionarios de rutas (Necesarios para socioeconomico y gold)
    raw_paths = {
        "renta": f"{RAW_BASE}/renta",
        "migracion": f"{RAW_BASE}/migracion",
        "consumo": f"{RAW_BASE}/consumo"
    }
    
    silver_paths = {
        "demografia": f"{SILVER_BASE}/demografia",
        "viirs": f"{SILVER_BASE}/viirs",
        "osm": f"{SILVER_BASE}/osm",
        "conectividad": f"{SILVER_BASE}/conectividad",
        "empresas": f"{SILVER_BASE}/empresas",
        "renta": f"{SILVER_BASE}/renta",
        "migracion": f"{SILVER_BASE}/migracion",
        "consumo": f"{SILVER_BASE}/consumo"
    }

    try:
        print("\n--- 🥈 Procesando Capa Silver (Clean) ---")
        
        # Llamadas a funciones individuales
        process_demography(spark, f"{RAW_BASE}/demografia", silver_paths["demografia"])
        process_viirs(spark, f"{RAW_BASE}/luz_nocturna", silver_paths["viirs"])
        process_osm(spark, f"{RAW_BASE}/transporte", silver_paths["osm"])
        process_conectividad(spark, f"{RAW_BASE}/transporte", silver_paths["conectividad"])
        process_empresas(spark, f"{RAW_BASE}/empresas_transporte", silver_paths["empresas"])
        
        # Llamada al script que procesa 3 dimensiones a la vez (usando el diccionario)
        run_socioeconomico(spark, raw_paths, silver_paths)

        print("\n--- 🥇 Generando Capa Gold (Dataset Maestro) ---")
        
        # Llamada al cruce final (usando el diccionario)
        build_master_features(spark, silver_paths, f"{GOLD_BASE}/master_feature_store")

        print("\n✅ Pipeline completado con éxito. Datos listos en Gold.")

    except Exception as e:
        print(f"❌ Error crítico en el pipeline: {str(e)}")
    finally:
        spark.stop()

if __name__ == "__main__":
    main()