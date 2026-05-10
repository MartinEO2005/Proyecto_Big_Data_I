import sys
import os
import time
import json

# 1. FIX LIBRERÍAS
sys.path.append("/home/22300471student/mis_librerias")

import pandas as pd
from sqlalchemy import create_engine, text
from pyspark.sql import SparkSession

# CONFIGURACIÓN DB
DB_URL = "mysql+pymysql://bd_rvm_gelumica:Rio45Abc@10.151.30.2:3306/bd_rvm_gelumica"
engine = create_engine(DB_URL)

def orquestrar_gold_completo():
    start_time = time.time()
    spark = SparkSession.builder.appName("GeoLumica_Production_Fix").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    
    CLEAN_PATH = "hdfs://eespcacbgdpro01:9000/user/22300471student/data_raw/clean/"
    
    # --- FASE 1: DIMENSIONES ---
    print("📋 Cargando GeoJSON y Dimensiones...")
    geojson_path = '/home/22300471student/Proyecto_Open_Data_I/municipios_es.geojson'
    with open(geojson_path, 'r', encoding='utf-8') as f:
        data = json.load(f)
    df_traductor = pd.DataFrame([{'muni_key': str(p['properties']['LAU_ID']).zfill(5), 
                                  'muni_display': p['properties'].get('LAU_NAME', '').strip()} 
                                 for p in data['features'] if p['properties'].get('LAU_ID')])
    df_traductor.to_sql('dim_geografia', engine, if_exists='replace', index=False)

    # --- FASE 2: SILVER (FACT TABLES) ---
    mapeo = {
        'demografia': 'demografia_municipios_final.csv',
        'viirs': 'viirsFinal_limpio.csv',
        'renta': 'rentamedia_municipios_final_limpio.csv',
        'migracion': 'migracion_municipios_final_limpio.csv',
        'consumo': 'consumo_electrico_final_limpio.csv',
        'conectividad': 'conectividad_final_limpio.csv',
        'osm': 'muni_station_osm_limpio.csv',
        'empresas': 'empresas_transporte_final_limpio.csv'
    }

    print("\n🚀 PROCESANDO CAPA SILVER (ESTRELLA)...")
    
    for dim, filename in mapeo.items():
        try:
            df_pd = spark.read.option("header", "true").option("inferSchema", "true").csv(f"{CLEAN_PATH}{filename}").toPandas()
            df_pd.columns = [c.strip() for c in df_pd.columns]

            # 1. NORMALIZACIÓN DE TIEMPO (VIIRS MENSUAL)
            col_t = next((c for c in df_pd.columns if c.lower() in ['year', 'anio', 'año', 'date']), None)
            if col_t:
                if col_t == 'date': 
                    # Extraemos Año y Mes para que el Silver sea realmente Mensual
                    df_pd['Anio'] = df_pd['date'].astype(str).str[:4].astype(int)
                    df_pd['Mes'] = df_pd['date'].astype(str).str[5:7].astype(int)
                else: 
                    df_pd = df_pd.rename(columns={col_t: 'Anio'})

            # 2. NORMALIZACIÓN DE ID (muni_id_join)
            posibles_ids = ['lau_id', 'muni_id_join', 'codigo_municipio', 'codigo', 'sog_id']
            col_id = next((c for c in df_pd.columns if c.lower() in posibles_ids), None)
            
            if col_id and dim != 'demografia':
                df_pd['muni_id_join'] = df_pd[col_id].astype(str).str.replace('ES_', '').str.split('.').str[0].str.zfill(5)
            else:
                col_n = next((c for c in df_pd.columns if c.lower() in ['municipio', 'nombre', 'lau_name']), None)
                df_pd = pd.merge(df_pd, df_traductor, left_on=col_n, right_on='muni_display', how='left')
                df_pd = df_pd.rename(columns={'muni_key': 'muni_id_join'})

            # 3. RENOMBRES ESPECÍFICOS
            if dim == 'migracion': df_pd = df_pd.rename(columns={'cantidad (personas)': 'migracion_total'})
            if dim == 'empresas':
                yr_cols = [c for c in df_pd.columns if c.isdigit()]
                df_pd = df_pd.melt(id_vars=[c for c in df_pd.columns if c not in yr_cols], 
                                  value_vars=yr_cols, var_name='Anio', value_name='num_empresas_transporte')
                df_pd['Anio'] = df_pd['Anio'].astype(int)

            df_pd = df_pd.drop(columns=[c for c in df_pd.columns if c.lower() in posibles_ids + ['muni_display'] and c != 'muni_id_join'], errors='ignore')
            table_name = f"fact_{dim}"
            df_pd.to_sql(table_name, engine, if_exists='replace', index=False)
            
            with engine.begin() as conn:
                conn.execute(text(f"ALTER TABLE {table_name} MODIFY muni_id_join VARCHAR(5);"))
                conn.execute(text(f"CREATE INDEX idx_{table_name} ON {table_name}(muni_id_join);"))
            
            print(f"📦 {dim.upper():<12} | ✅ OK | Filas: {len(df_pd):>9,} | Granularidad: {'Mensual' if 'Mes' in df_pd.columns else 'Anual'}")

        except Exception as e: print(f"📦 {dim.upper():<12} | ❌ ERROR: {e}")

# --- FASE 3: GOLD (SISTEMA ANTI-COLISIONES + IMPUTACIÓN) ---
    print("\n🥇 GENERANDO TABLÓN MAESTRO GOLD (MODO ML-READY)...")
    try:
        # 3.1. Generamos columnas con COALESCE para evitar NaNs en el Join
        with engine.connect() as conn:
            res_base = conn.execute(text("SELECT * FROM fact_conectividad LIMIT 0"))
            cols_usadas = {c.lower() for c in res_base.keys()}
        
        def get_safe_cols_imputed(table_name, alias, prefix):
            with engine.connect() as conn:
                res = conn.execute(text(f"SELECT * FROM {table_name} LIMIT 0"))
                listado = []
                for c in res.keys():
                    c_low = c.lower()
                    if c_low in ['muni_id_join', 'anio', 'mes']: continue
                    # Imputamos con 0 si es nulo para que el modelo ML no falle
                    sql_part = f"COALESCE({alias}.`{c}`, 0)"
                    nombre_final = f"{prefix}_{c}" if c_low in cols_usadas else c
                    listado.append(f"{sql_part} AS `{nombre_final}`")
                    cols_usadas.add(nombre_col.lower() if 'nombre_col' in locals() else nombre_final.lower())
                return ", ".join(listado) if listado else ""

        columnas_finales = ", ".join([p for p in [
            get_safe_cols_imputed('fact_osm', 'o', 'osm'), 
            get_safe_cols_imputed('fact_consumo', 'e', 'cons'), 
            get_safe_cols_imputed('fact_demografia', 'd', 'dem'), 
            get_safe_cols_imputed('fact_renta', 'r', 'renta'), 
            get_safe_cols_imputed('fact_migracion', 'm', 'mig'), 
            get_safe_cols_imputed('fact_empresas', 'emp', 'emp')] if p])

        with engine.begin() as conn:
            conn.execute(text("DROP TABLE IF EXISTS fact_master_gold"))
            # Agregación de VIIRS con media anual e intensidad_luz imputada
            sql_master = f"""
            CREATE TABLE fact_master_gold AS
            SELECT 
                c.*, 
                {columnas_finales},
                COALESCE(v.luz_media, 0) AS intensidad_luz
            FROM fact_conectividad c
            LEFT JOIN fact_osm o ON c.muni_id_join = o.muni_id_join
            LEFT JOIN fact_consumo e ON c.muni_id_join = e.muni_id_join
            LEFT JOIN fact_demografia d ON c.muni_id_join = d.muni_id_join AND c.Anio = d.Anio
            LEFT JOIN fact_renta r ON c.muni_id_join = r.muni_id_join AND c.Anio = r.Anio
            LEFT JOIN fact_migracion m ON c.muni_id_join = m.muni_id_join AND c.Anio = m.Anio
            LEFT JOIN fact_empresas emp ON c.muni_id_join = emp.muni_id_join AND c.Anio = emp.Anio
            LEFT JOIN (
                SELECT muni_id_join, Anio, AVG(`mean`) as luz_media
                FROM fact_viirs GROUP BY muni_id_join, Anio
            ) v ON c.muni_id_join = v.muni_id_join AND c.Anio = v.Anio
            """
            conn.execute(text(sql_master))
            conn.execute(text("CREATE INDEX idx_gold_final ON fact_master_gold(muni_id_join, Anio);"))

        print(f"✅ ¡CAPA GOLD CREADA! Matriz lista para Clustering.")

    except Exception as e: print(f"❌ Error crítico en Gold: {e}")
    spark.stop()

if __name__ == "__main__":
    orquestrar_gold_completo()