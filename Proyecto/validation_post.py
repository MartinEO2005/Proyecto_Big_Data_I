"""
Validación POST-EJECUCIÓN de GeoLúmica

Después de ejecutar main_silver.py y main_gold.py, este script comprueba:
1. Que Silver se generó completo
2. Que Gold no tiene duplicados en (muni_id, year)
3. Que las dimensiones y facts tienen el tamaño esperado
"""

import logging
from pathlib import Path
from pyspark.sql import SparkSession, functions as F

logger = logging.getLogger(__name__)


def count_duplicates(df, key_cols):
    """Cuenta duplicados ignorando filas con claves nulas."""
    filtered = df.dropna(subset=key_cols)
    return filtered.groupBy(*key_cols).count().filter("count > 1").count()


def setup_logger(log_file="logs/validation_post.log"):
    Path("logs").mkdir(exist_ok=True)
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler()
        ]
    )


def validate_silver(spark, dim_base_path, fact_base_path):
    """
    Valida que Silver se generó correctamente.
    """
    
    print("\n" + "="*70)
    print("1️⃣ VALIDANDO CAPA SILVER")
    print("="*70)
    
    problemas = []
    
    # Leer dimensiones
    try:
        print("\nLeyendo dimensiones...")
        dim_muni = spark.read.parquet(f"{dim_base_path}/dim_municipio.parquet")
        dim_prov = spark.read.parquet(f"{dim_base_path}/dim_provincia.parquet")
        dim_fecha = spark.read.parquet(f"{dim_base_path}/dim_fecha_anual.parquet")
        
        count_muni = dim_muni.count()
        count_prov = dim_prov.count()
        count_fecha = dim_fecha.count()
        
        print(f"✓ dim_municipio: {count_muni} registros")
        print(f"✓ dim_provincia: {count_prov} registros")
        print(f"✓ dim_fecha_anual: {count_fecha} registros")
        
        # Validar que tengas ~8k municipios
        if count_muni < 8000 or count_muni > 9000:
            problemas.append(f"dim_municipio con {count_muni} registros (esperado ~8131)")
            print(f"❌ dim_municipio con tamaño inesperado: {count_muni}")
        else:
            print(f"✓ Tamaño de municipios OK")
        
        # Validar columnas de dim_municipio
        muni_cols = dim_muni.columns
        required_muni_cols = ['muni_id', 'prov_id', 'muni_name', 'area_km2']
        missing_cols = [c for c in required_muni_cols if c not in muni_cols]
        if missing_cols:
            problemas.append(f"dim_municipio falta columnas: {missing_cols}")
            print(f"❌ Columnas faltantes: {missing_cols}")
        else:
            print(f"✓ Columnas de dim_municipio OK")
        
    except Exception as e:
        problemas.append(f"Error leyendo dimensiones: {e}")
        print(f"❌ Error: {e}")
        return False, problemas
    
    # Leer facts
    try:
        print("\n" + "-"*70)
        print("Leyendo facts...")
        
        # VIIRS es opcional
        facts_required = {
            "fact_demografia": f"{fact_base_path}/fact_demografia.parquet",
            "fact_energia": f"{fact_base_path}/fact_energia.parquet",
            "fact_renta": f"{fact_base_path}/fact_renta.parquet",
            "fact_migracion_neta": f"{fact_base_path}/fact_migracion_neta.parquet",
            "fact_conectividad": f"{fact_base_path}/fact_conectividad.parquet",
            "fact_empresas_transporte": f"{fact_base_path}/fact_empresas_transporte.parquet",
            "fact_osm_logistica": f"{fact_base_path}/fact_osm_logistica.parquet",
        }
        facts_optional = {
            "fact_viirs":     f"{fact_base_path}/fact_viirs.parquet",
            "fact_satelital": f"{fact_base_path}/fact_satelital.parquet",
        }

        fact_key_map = {
            "fact_energia": ["muni_id"],
            "fact_renta": ["muni_id", "year"],
            "fact_migracion_neta": ["muni_id", "year", "nacionalidad"],
            "fact_conectividad": ["muni_id", "year"],
            "fact_empresas_transporte": ["muni_id", "year", "tipo"],
            "fact_osm_logistica": ["muni_id"],
            "fact_viirs": ["muni_id", "year", "fecha"],
            "fact_satelital": ["product_id"],
        }

        for fact_name, fact_path in {**facts_required, **facts_optional}.items():
            optional = fact_name in facts_optional
            try:
                df = spark.read.parquet(fact_path)
                count = df.count()
                print(f"{'⚠' if optional else '✓'} {fact_name}: {count} registros{'  (opcional)' if optional else ''}")

                if fact_name == "fact_demografia":
                    print("  → Unicidad de fact_demografia omitida: Gold valida la no multiplicación final")
                    continue

                key_cols = fact_key_map.get(fact_name)
                if key_cols and set(key_cols).issubset(df.columns):
                    duplicates = count_duplicates(df, key_cols)
                    if duplicates > 0:
                        problemas.append(f"{fact_name} tiene {duplicates} duplicados en {tuple(key_cols)}")
                        print(f"  ❌ {fact_name} tiene {duplicates} duplicados en {tuple(key_cols)}")
                    else:
                        print(f"  → Sin duplicados en {tuple(key_cols)} ✓")
            except Exception as e:
                if optional:
                    print(f"  ⚠ {fact_name}: no encontrada (opcional, se omite)")
                else:
                    problemas.append(f"Error leyendo {fact_name}: {e}")
                    print(f"❌ Error en {fact_name}: {e}")
        
    except Exception as e:
        problemas.append(f"Error leyendo facts: {e}")
        print(f"❌ Error: {e}")
        return False, problemas
    
    print("\n" + "-"*70)
    if problemas:
        print(f"❌ Silver tiene {len(problemas)} problema(s)")
        return False, problemas
    else:
        print("✅ Silver validada correctamente")
        return True, []


def validate_gold(spark, gold_path):
    """
    Valida que Gold se generó correctamente y sin duplicados.
    ESTE ES EL VALIDADOR MÁS IMPORTANTE.
    """
    
    print("\n" + "="*70)
    print("2️⃣ VALIDANDO CAPA GOLD (CRÍTICO)")
    print("="*70)
    
    problemas = []
    
    try:
        print(f"\nLeyendo Gold desde {gold_path}/df_maestro.parquet...")
        gold = spark.read.parquet(f"{gold_path}/df_maestro.parquet").cache()
        
        total_count = gold.count()
        print(f"✓ df_maestro.parquet: {total_count} registros")
        
        # VALIDADOR CRÍTICO: Duplicados en la clave final del Gold.
        print("\n" + "🔴 VALIDACIÓN CRÍTICA: Duplicados en (muni_id, year)".center(70))

        key_cols = ["muni_id", "year"]
        null_key_count = gold.filter(F.col("muni_id").isNull() | F.col("year").isNull()).count()
        if null_key_count > 0:
            problemas.append(f"GOLD tiene {null_key_count} filas con clave nula en (muni_id, year)")
            print(f"\n❌ PROBLEMA: Hay {null_key_count} filas con clave nula en (muni_id, year)")

        duplicate_check = gold.dropna(subset=key_cols).groupBy(*key_cols).count().filter("count > 1")
        duplicate_count = duplicate_check.count()
        
        if duplicate_count > 0:
            problemas.append(f"⚠️ GOLD tiene {duplicate_count} duplicados en (muni_id, year)")
            print(f"\n❌ PROBLEMA: Hay {duplicate_count} pares (muni_id, year) duplicados")
            print("   Esto significa que algún LEFT JOIN multiplicó la granularidad final.")
            print("   Mostrando ejemplos:")
            duplicate_check.limit(10).show()
            
            # Mostrar ejemplo de fila duplicada
            first_dup = duplicate_check.limit(1).collect()[0]
            muni_id = first_dup['muni_id']
            year = first_dup['year']
            
            print(f"\n   Filas duplicadas para muni_id={muni_id}, year={year}:")
            gold.filter((F.col("muni_id") == muni_id) & (F.col("year") == year)).show(truncate=False)
        else:
            print(f"\n✅ NO HAY DUPLICADOS en (muni_id, year)")
            print(f"   Total registros: {total_count}")
            stats = gold.agg(
                F.countDistinct("muni_id").alias("munis"),
                F.countDistinct("year").alias("anios")
            ).collect()[0]
            print(f"   Municipios únicos: {stats['munis']}")
            print(f"   Años únicos: {stats['anios']}")
        
        # Validar número de columnas
        num_cols = len(gold.columns)
        print(f"\n✓ Número de columnas: {num_cols}")
        print(f"  Columnas: {', '.join(gold.columns[:10])}...")
        if num_cols != 77:
            problemas.append(f"GOLD tiene {num_cols} columnas en lugar de 77")
            print(f"  ❌ Número de columnas inesperado: {num_cols}")
        else:
            print("  → Esquema esperado de 77 columnas ✓")
        
        # Validar que no haya NULLs masivos en columnas clave
        print(f"\n" + "-"*70)
        print("Validando cobertura de datos (NULLs en columnas clave):")
        
        # Umbrales por columna: algunos datasets no cubren todos los municipios/años
        key_cols = {
            "indice_conectividad": 1,
            "poblacion_muni":      10,
            "renta_media":         80,
            "intensidad_luz":      85,
        }
        for col, threshold in key_cols.items():
            if col in gold.columns:
                null_count = gold.filter(F.col(col).isNull()).count()
                null_pct = (null_count / total_count) * 100
                status = "✅" if null_pct < threshold else "❌"
                print(f"  {status} {col}: {null_pct:.1f}% NULLs ({null_count}) [umbral: <{threshold}%]")
        
        gold.unpersist()

    except Exception as e:
        problemas.append(f"Error leyendo Gold: {e}")
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
        return False, problemas
    
    print("\n" + "-"*70)
    if problemas:
        print(f"❌ GOLD tiene {len(problemas)} problema(s):")
        for p in problemas:
            print(f"   - {p}")
        return False, problemas
    else:
        print("✅ GOLD validada correctamente")
        return True, []


def main(
    dim_base_path="data/silver/dim",
    fact_base_path="data/silver/fact",
    gold_base_path="data/gold"
):
    """
    Ejecuta validaciones post-ejecución.
    """
    
    setup_logger()
    
    print("\n" + "🔍 VALIDACIÓN POST-EJECUCIÓN DE GEOLÚMICA 🔍".center(70))
    print("="*70)
    
    spark = SparkSession.builder \
        .appName("GeoLumica-ValidationPost") \
        .master("local[*]") \
        .config("spark.sql.shuffle.partitions", "4") \
        .config("spark.hadoop.fs.defaultFS", "hdfs://localhost:9000") \
        .getOrCreate()
    
    all_ok = True
    all_problems = []
    
    try:
        # 1. Validar Silver
        ok, probs = validate_silver(spark, dim_base_path, fact_base_path)
        all_ok = all_ok and ok
        all_problems.extend(probs)
        
        # 2. Validar Gold (CRÍTICO)
        ok, probs = validate_gold(spark, gold_base_path)
        all_ok = all_ok and ok
        all_problems.extend(probs)
        
    finally:
        spark.stop()
    
    # Resumen final
    print("\n" + "="*70)
    print("📋 RESUMEN FINAL")
    print("="*70)
    
    if all_ok:
        print("\n✅ ¡TODAS LAS VALIDACIONES POST-EJECUCIÓN PASARON!")
        print("\nPuedes proceder a:")
        print("  1. Usar Gold en ML (hdfs://localhost:9000/geolumica/gold/df_maestro.parquet)")
        print("  2. Consumir Silver en dashboards")
    else:
        print(f"\n❌ VALIDACIÓN FALLÓ: {len(all_problems)} problema(s)")
        print("\nProblemas encontrados:")
        for i, p in enumerate(all_problems, 1):
            print(f"  {i}. {p}")
        print("\n⚠️ No uses estos datos hasta que corrijas los problemas.")
    
    print("\n" + "="*70)
    
    return all_ok


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Validación post-ejecución GeoLúmica")
    parser.add_argument("--dim",  default="hdfs://localhost:9000/geolumica/silver/dim")
    parser.add_argument("--fact", default="hdfs://localhost:9000/geolumica/silver/fact")
    parser.add_argument("--gold", default="hdfs://localhost:9000/geolumica/gold")
    
    args = parser.parse_args()
    
    success = main(
        dim_base_path=args.dim,
        fact_base_path=args.fact,
        gold_base_path=args.gold
    )
    
    exit(0 if success else 1)
