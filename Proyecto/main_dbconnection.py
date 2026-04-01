import pandas as pd
from sqlalchemy import create_engine, text
import os
import time
import json
from dotenv import load_dotenv

# --- CONFIGURACIÓN ---
load_dotenv()
DB_USER = os.getenv('DB_USER', 'root')
DB_PASS = os.getenv('DB_PASS', '1234')
DB_HOST = '127.0.0.1'
DB_PORT = os.getenv('DB_PORT', '3306')
DB_NAME = os.getenv('DB_NAME', 'proyecto_big_data')

INE_PROV_MAP = {
    "01": "Álava", "02": "Albacete", "03": "Alicante", "04": "Almería", "05": "Ávila",
    "06": "Badajoz", "07": "Islas Baleares", "08": "Barcelona", "09": "Burgos", "10": "Cáceres",
    "11": "Cádiz", "12": "Castellón", "13": "Ciudad Real", "14": "Córdoba", "15": "A Coruña",
    "16": "Cuenca", "17": "Girona", "18": "Granada", "19": "Guadalajara", "20": "Gipuzkoa",
    "21": "Huelva", "22": "Huesca", "23": "Jaén", "24": "León", "25": "Lleida",
    "26": "La Rioja", "27": "Lugo", "28": "Madrid", "29": "Málaga", "30": "Murcia",
    "31": "Navarra", "32": "Ourense", "33": "Asturias", "34": "Palencia", "35": "Las Palmas",
    "36": "Pontevedra", "37": "Salamanca", "38": "Santa Cruz de Tenerife", "39": "Cantabria",
    "40": "Segovia", "41": "Sevilla", "42": "Soria", "43": "Tarragona", "44": "Teruel",
    "45": "Toledo", "46": "Valencia", "47": "Valladolid", "48": "Bizkaia", "49": "Zamora",
    "50": "Zaragoza", "51": "Ceuta", "52": "Melilla"
}

def add_keys_and_indexes(engine, table_names):
    with engine.begin() as conn:
        print("\n🏗️  Estableciendo integridad referencial e índices...")
        conn.execute(text("ALTER TABLE dim_provincia MODIFY prov_key VARCHAR(255) NOT NULL PRIMARY KEY;"))
        conn.execute(text("ALTER TABLE dim_geografia MODIFY muni_key VARCHAR(255) NOT NULL PRIMARY KEY;"))

        for table in table_names:
            if table in ['dim_geografia', 'dim_provincia']: continue
            print(f"⚡ Indexando {table}... ", end="", flush=True)
            conn.execute(text(f"ALTER TABLE {table} MODIFY muni_id_join VARCHAR(255);"))
            conn.execute(text(f"CREATE INDEX idx_{table}_muni ON {table}(muni_id_join);"))
            print("HECHO ✅")

def prepare_and_load():
    start_time = time.time()
    print(f"🧨 Reiniciando base de datos: {DB_NAME}")
    
    server_conn = f"mysql+pymysql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}"
    server_engine = create_engine(server_conn)
    with server_engine.connect() as conn:
        conn.execute(text(f"DROP DATABASE IF EXISTS {DB_NAME}"))
        conn.execute(text(f"CREATE DATABASE {DB_NAME}"))
    
    engine = create_engine(f"{server_conn}/{DB_NAME}")
    
    datasets_files = {
        "demografia": "data/clean/demografia_municipios_final.csv",
        "conectividad": "data/clean/conectividad_final_limpio.csv",
        "migraciones": "data/clean/migracion_municipios_final_limpio.csv",
        "consumo_viviendas": "data/clean/consumo_electrico_final_limpio.csv",
        "osm": "data/clean/muni_station_osm_limpio.csv",
        "pib": "data/clean/rentamedia_municipios_final_limpio.csv",
        "viirs": "data/clean/viirsFinal_limpio.csv"
    }

    created_tables = []
    
    # ---------------------------------------------------------
    # FASE 1: CREAR DIMENSIONES DESDE LA VERDAD ABSOLUTA (GeoJSON)
    # ---------------------------------------------------------
    print("📋 Construyendo Dimensiones Geográficas desde GeoJSON...")
    with open('municipios_es.geojson', 'r', encoding='utf-8') as f:
        geojson_data = json.load(f)

    filas_geo = []
    for feature in geojson_data['features']:
        props = feature['properties']
        lau_id = props.get('LAU_ID')
        if lau_id:
            lau_id = str(lau_id).zfill(5)
            prov_key = lau_id[:2]
            nombre = props.get('LAU_NAME', '')
            filas_geo.append({
                'muni_key': lau_id,
                'prov_id_join': prov_key,
                'muni_display': nombre
            })

    # Subir Municipios
    dim_geografia = pd.DataFrame(filas_geo)
    dim_geografia.to_sql('dim_geografia', engine, if_exists='replace', index=False)
    created_tables.append('dim_geografia')

    # Subir Provincias
    dim_provincia = pd.DataFrame({
        'prov_key': list(INE_PROV_MAP.keys()),
        'prov_display': list(INE_PROV_MAP.values())
    })
    dim_provincia.to_sql('dim_provincia', engine, if_exists='replace', index=False)
    created_tables.append('dim_provincia')
    print(f"   ✅ {len(dim_geografia)} municipios y 52 provincias inyectadas.")

    # ---------------------------------------------------------
    # FASE 2: CARGAR LAS 7 TABLAS DE HECHOS
    # ---------------------------------------------------------
    for name, path in datasets_files.items():
        table_name = f'fact_{name}'
        print(f"📥 Cargando {table_name}... ", end="", flush=True)
        
        try:
            df = pd.read_csv(path)
            
            # Aseguramos que la clave de unión existe y tiene 5 dígitos (por si Pandas lo leyó como número)
            col_id = next((c for c in df.columns if c.lower() in ['muni_id_join', 'lau_id', 'codigo_municipio']), None)
            if col_id:
                df['muni_id_join'] = df[col_id].apply(lambda x: str(int(float(x))).zfill(5) if pd.notnull(x) else None)
                # Si la columna original no se llamaba muni_id_join, la podemos borrar o dejar
            else:
                print("⚠️ ADVERTENCIA: No se detectó columna LAU_ID/muni_id_join en este CSV.")
                
            df.to_sql(table_name, engine, if_exists='replace', index=False)
            created_tables.append(table_name)
            print("OK ✅")
        except FileNotFoundError:
            print(f"❌ Error: Archivo no encontrado en {path}")

    # FASE 3: RELACIONES E ÍNDICES
    add_keys_and_indexes(engine, created_tables)
    print(f"\n🚀 PROYECTO GEO-LÚMICA: ¡Despliegue completado en {round((time.time() - start_time)/60, 2)} minutos!")

if __name__ == "__main__":
    prepare_and_load()