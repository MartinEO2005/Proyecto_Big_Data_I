import pandas as pd
from sqlalchemy import create_engine, text

# Conexión
engine = create_engine("mysql+pymysql://bd_rvm_gelumica:Rio45Abc@10.151.30.2:3306/bd_rvm_gelumica")

def check_geolumica_integrity():
    print("="*80)
    print("🔍 DIAGNÓSTICO ESTRUCTURAL DE LA BASE DE DATOS")
    print("="*80)

    # 1. ¿Cuántos municipios reales hay en la base?
    muni_count = pd.read_sql("SELECT COUNT(DISTINCT muni_key) as total FROM dim_geografia", engine).iloc[0,0]
    print(f"📍 Municipios en dim_geografia (Objetivo): {muni_count}")

    # 2. Verificar duplicados en tablas críticas (Muni + Año)
    # Si esto da > 0, los joins se multiplicarán
    tablas_criticas = ['fact_demografia', 'fact_renta', 'fact_conectividad', 'fact_empresas']
    
    print("\n⚠️ CHEQUEO DE CLAVES ÚNICAS (Muni + Año):")
    for t in tablas_criticas:
        query = f"""
            SELECT COUNT(*) FROM (
                SELECT muni_id_join, Anio, COUNT(*) 
                FROM {t} 
                GROUP BY muni_id_join, Anio 
                HAVING COUNT(*) > 1
            ) AS dups
        """
        dups = pd.read_sql(query, engine).iloc[0,0]
        print(f"   • {t:<20}: {dups} combinaciones duplicadas")

    # 3. Simulación de la "Foto Actual"
    # Aquí es donde suele romperse el Trozo 1 del script de ML
    print("\n📈 SIMULACIÓN DE SEGMENTACIÓN (Año Máximo):")
    for t in tablas_criticas:
        max_anio = pd.read_sql(f"SELECT MAX(Anio) FROM {t}", engine).iloc[0,0]
        count_actual = pd.read_sql(f"SELECT COUNT(DISTINCT muni_id_join) FROM {t} WHERE Anio = {max_anio}", engine).iloc[0,0]
        print(f"   • {t:<20} ({max_anio}): {count_actual} municipios únicos")

    # 4. Verificación de la Capa Gold (La que usamos para Clustering)
    gold_rows = pd.read_sql("SELECT COUNT(*) FROM fact_master_gold", engine).iloc[0,0]
    gold_dist = pd.read_sql("SELECT COUNT(DISTINCT muni_id_join, Anio) FROM fact_master_gold", engine).iloc[0,0]
    print(f"\n🥇 CAPA GOLD (fact_master_gold):")
    print(f"   • Filas totales: {gold_rows:,}")
    print(f"   • Combinaciones únicas Muni+Año: {gold_dist:,}")

    if gold_rows > gold_dist:
        print("\n❌ ERROR DETECTADO: Hay duplicados físicos en la tabla Gold.")
    else:
        print("\n✅ ESTRUCTURA DB: Los datos en MariaDB son correctos y únicos.")

check_geolumica_integrity()