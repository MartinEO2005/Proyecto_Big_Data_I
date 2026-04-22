"""
Orquestador principal: Raw → Silver (Spark)

Este script:
1. Valida que Raw está completo
2. Genera dimensiones
3. Genera fact tables
4. Valida integridad de Silver
"""

import logging
import os
import subprocess
import shutil
from datetime import datetime
from pyspark.sql import SparkSession

# Importar módulos locales
import sys
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from silver.dimensions import main_dimensions
from silver.facts import main_facts
from silver.satelital import create_fact_satelital

logger = logging.getLogger(__name__)


def setup_logger(log_file="logs/main_silver.log"):
    """Configura logging."""
    os.makedirs("logs", exist_ok=True)
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler()
        ]
    )


def validate_raw_layer(raw_base_path):
    """
    Valida que la capa Raw esté completa (archivos requeridos).
    Retorna True si todo está OK, False si falta algo requerido.
    """
    
    logger.info("Validando capa Raw...")
    
    required_files = {
        "demografia": [
            "demografia_poblacion_municipios.csv",
            "demografia_poblacion_provincias.csv"
        ],
        "energia": ["consumo_electrico.csv"],
        "renta": ["renta_municipios.csv"],
        "migracion": ["migracion_interior_municipios.csv"],
        "transporte": [
            "conectividad_municipal_2010_2025.csv",
        ],
        "empresas_transporte": ["empresas_transporte_prov_mun_anchos.csv"]
    }

    optional_files = [
        "transporte/muni_station_metrics_reduced.csv",
    ]
    
    missing = []

    for tema, archivos in required_files.items():
        for archivo in archivos:
            path = os.path.join(raw_base_path, tema, archivo)
            if not os.path.exists(path):
                missing.append(f"{tema}/{archivo}")
                logger.warning(f"FALTA (REQUERIDO): {path}")

    for rel_path in optional_files:
        path = os.path.join(raw_base_path, rel_path)
        if not os.path.exists(path):
            logger.warning(f"OPCIONAL ausente (se omitirá): {path}")
    
    if missing:
        logger.error(f"Raw incompleto. Faltan {len(missing)} archivos REQUERIDOS:")
        for m in missing:
            logger.error(f"  - {m}")
        return False
    
    logger.info("✅ Raw validado correctamente (archivos requeridos presentes)")
    return True


def ensure_output_dirs(base_paths):
    """Crea directorios de salida si no existen (solo para paths locales)."""
    for path in base_paths:
        if not path.startswith("hdfs://"):
            os.makedirs(path, exist_ok=True)
            logger.info(f"Directorio listo: {path}")
        else:
            logger.info(f"Path HDFS (ya creado): {path}")


def validate_silver_layer(dim_base_path, fact_base_path):
    """
    Valida que la capa Silver se generó correctamente.
    Verifica que existan archivos Parquet.
    """
    
    logger.info("Validando capa Silver...")
    
    required_dims = [
        "dim_municipio.parquet",
        "dim_provincia.parquet",
        "dim_fecha_anual.parquet"
    ]
    
    required_facts = [
        "fact_demografia.parquet",
        "fact_energia.parquet",
        "fact_renta.parquet",
        "fact_migracion_neta.parquet",
        "fact_conectividad.parquet",
        "fact_empresas_transporte.parquet",
        "fact_osm_logistica.parquet",
    ]
    optional_facts = [
        "fact_viirs.parquet",
        "fact_satelital.parquet",
    ]

    def _exists(base, name):
        full = f"{base}/{name}" if base.startswith("hdfs://") else os.path.join(base, name)
        if base.startswith("hdfs://"):
            hdfs_bin = shutil.which("hdfs") or os.path.expanduser("~/hadoop-3.3.6/bin/hdfs")
            r = subprocess.run(
                [hdfs_bin, "dfs", "-test", "-e", full],
                capture_output=True
            )
            return r.returncode == 0
        return os.path.exists(full)

    all_ok = True

    for dim in required_dims:
        if not _exists(dim_base_path, dim):
            logger.error(f"FALTA dimensión: {dim_base_path}/{dim}")
            all_ok = False
        else:
            logger.info(f"✅ {dim}")

    for fact in required_facts:
        if not _exists(fact_base_path, fact):
            logger.error(f"FALTA fact: {fact_base_path}/{fact}")
            all_ok = False
        else:
            logger.info(f"✅ {fact}")

    for fact in optional_facts:
        if not _exists(fact_base_path, fact):
            logger.warning(f"OPCIONAL no encontrada: {fact_base_path}/{fact}")
        else:
            logger.info(f"✅ {fact} (opcional)")

    return all_ok


def main(
    geojson_path="municipios_es.geojson",
    raw_base_path="data/raw",
    dim_base_path="data/silver/dim",
    fact_base_path="data/silver/fact"
):
    """
    Ejecuta el pipeline completo: Raw → Silver
    """
    
    setup_logger()
    
    logger.info("="*70)
    logger.info("INICIANDO PIPELINE RAW → SILVER (GeoLúmica)")
    logger.info(f"Timestamp: {datetime.now().isoformat()}")
    logger.info("="*70)
    
    # 1. Validar Raw
    if not validate_raw_layer(raw_base_path):
        logger.error("❌ Raw layer inválida. Abortando.")
        return False
    
    # 2. Crear directorios de salida
    ensure_output_dirs([dim_base_path, fact_base_path])
    
    # 3. Inicializar Spark
    logger.info("Iniciando sesión Spark...")
    spark = SparkSession.builder \
        .appName("GeoLumica-Silver") \
        .master("local[*]") \
        .config("spark.sql.shuffle.partitions", "4") \
        .config("spark.driver.memory", "2g") \
        .config("spark.default.parallelism", "4") \
        .config("spark.hadoop.fs.defaultFS", "hdfs://localhost:9000") \
        .getOrCreate()
    
    logger.info(f"Spark versión: {spark.version}")
    
    try:
        # 4. Generar dimensiones
        logger.info("-"*70)
        main_dimensions(spark, geojson_path, dim_base_path)
        
        # 5. Generar facts
        logger.info("-"*70)
        main_facts(spark, raw_base_path, dim_base_path, fact_base_path)

        # 5b. Generar fact satelital (Sentinel-2 catalog)
        sat_csv = os.path.join(raw_base_path, "satelital", "sentinel2_products.csv")
        if os.path.exists(sat_csv):
            create_fact_satelital(spark, raw_base_path, f"{fact_base_path}/fact_satelital.parquet")
        else:
            logger.warning("Catálogo Sentinel-2 no encontrado — fact_satelital omitida")
        
        # 6. Validar Silver
        logger.info("-"*70)
        if validate_silver_layer(dim_base_path, fact_base_path):
            logger.info("✅ Silver layer válida")
        else:
            logger.warning("⚠️ Silver layer con problemas")
        
        logger.info("="*70)
        logger.info("✅ PIPELINE COMPLETADO EXITOSAMENTE")
        logger.info("="*70)
        
        return True
    
    except Exception as e:
        logger.error(f"❌ ERROR EN PIPELINE: {e}", exc_info=True)
        return False
    
    finally:
        spark.stop()
        logger.info("Sesión Spark cerrada")


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(
        description="Pipeline Raw → Silver para GeoLúmica"
    )
    parser.add_argument(
        "--geojson",
        default="../municipios_es.geojson",
        help="Ruta al GeoJSON de municipios"
    )
    parser.add_argument(
        "--raw",
        default="../data/raw",
        help="Ruta base de la capa Raw"
    )
    parser.add_argument(
        "--dim",
        default="../data/silver/dim",
        help="Ruta base para dimensiones Silver"
    )
    parser.add_argument(
        "--fact",
        default="../data/silver/fact",
        help="Ruta base para facts Silver"
    )
    
    args = parser.parse_args()
    
    success = main(
        geojson_path=args.geojson,
        raw_base_path=args.raw,
        dim_base_path=args.dim,
        fact_base_path=args.fact
    )
    
    exit(0 if success else 1)
