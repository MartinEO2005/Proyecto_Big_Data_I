import pandas as pd
from sqlalchemy import create_engine, text
import unicodedata
import os

# os.getenv busca la variable; si no existe, usa el segundo valor como respaldo
DB_USER = os.getenv('DB_USER', 'root')
DB_PASS = os.getenv('DB_PASS', '1234')
DB_HOST = os.getenv('DB_HOST', 'mysql_server') 
DB_PORT = os.getenv('DB_PORT', '3306')
DB_NAME = os.getenv('DB_NAME', 'proyecto_big_data')

def limpiar_texto(texto):
    if not isinstance(texto, str): return texto
    texto = str(texto).lower()
    # Normalización para quitar acentos y caracteres especiales
    texto = unicodedata.normalize('NFKD', texto).encode('ascii', 'ignore').decode('utf-8')
    for char in ['/', '-', ',', '.', '(', ')', '*']: # He añadido el asterisco que venía en tu nuevo CSV
        texto = texto.replace(char, ' ')
    return " ".join(texto.split())

def add_keys_and_indexes(engine, table_names):
    with engine.connect() as conn:
        print("\n🏗️  Construyendo relaciones multinivel en el almacén de datos...")
        
        # 1. PRIMARY KEYS para las Dimensiones
        conn.execute(text("ALTER TABLE dim_provincia MODIFY prov_key VARCHAR(255) NOT NULL;"))
        if 'PRIMARY' not in [r[0] for r in conn.execute(text(f"SHOW KEYS FROM dim_provincia WHERE Key_name = 'PRIMARY'")).fetchall()]:
            conn.execute(text("ALTER TABLE dim_provincia ADD PRIMARY KEY (prov_key);"))
        
        conn.execute(text("ALTER TABLE dim_geografia MODIFY muni_key VARCHAR(255) NOT NULL;"))
        if 'PRIMARY' not in [r[0] for r in conn.execute(text(f"SHOW KEYS FROM dim_geografia WHERE Key_name = 'PRIMARY'")).fetchall()]:
            conn.execute(text("ALTER TABLE dim_geografia ADD PRIMARY KEY (muni_key);"))
        
        # 2. RELACIÓN MUNICIPIO -> PROVINCIA (Modelo Copo de Nieve)
        try:
            conn.execute(text("ALTER TABLE dim_geografia MODIFY prov_id_join VARCHAR(255);"))
            conn.execute(text("ALTER TABLE dim_geografia ADD CONSTRAINT fk_geo_prov FOREIGN KEY (prov_id_join) REFERENCES dim_provincia(prov_key);"))
            print("   ✅ Jerarquía Provincia -> Municipio establecida.")
        except Exception as e:
            print(f"   ⚠️ Nota en jerarquía: {e}")

        # 3. CONEXIÓN DE TABLAS DE HECHOS
        for table in table_names:
            if table in ['dim_geografia', 'dim_provincia']: continue 
            
            conectada = False
            # INTENTO 1: Conexión por MUNICIPIO (muni_id_join)
            res_muni = conn.execute(text(f"SHOW COLUMNS FROM {table} LIKE 'muni_id_join'"))
            if res_muni.fetchone():
                try:
                    conn.execute(text(f"ALTER TABLE {table} MODIFY muni_id_join VARCHAR(255);"))
                    conn.execute(text(f"CREATE INDEX idx_{table}_muni ON {table}(muni_id_join);"))
                    conn.execute(text(f"DELETE FROM {table} WHERE muni_id_join NOT IN (SELECT muni_key FROM dim_geografia)"))
                    
                    count = conn.execute(text(f"SELECT COUNT(*) FROM {table}")).scalar()
                    if count > 0:
                        conn.execute(text(f"ALTER TABLE {table} ADD CONSTRAINT fk_{table}_muni FOREIGN KEY (muni_id_join) REFERENCES dim_geografia(muni_key);"))
                        print(f"   ✅ {table} -> Conectada a Municipio")
                        conectada = True
                except Exception: pass

            # INTENTO 2: Conexión por PROVINCIA (Plan B)
            if not conectada:
                res_prov = conn.execute(text(f"SHOW COLUMNS FROM {table} LIKE 'prov_id_join'"))
                if res_prov.fetchone():
                    try:
                        conn.execute(text(f"ALTER TABLE {table} MODIFY prov_id_join VARCHAR(255);"))
                        conn.execute(text(f"CREATE INDEX idx_{table}_prov ON {table}(prov_id_join);"))
                        conn.execute(text(f"DELETE FROM {table} WHERE prov_id_join NOT IN (SELECT prov_key FROM dim_provincia)"))
                        conn.execute(text(f"ALTER TABLE {table} ADD CONSTRAINT fk_{table}_prov FOREIGN KEY (prov_id_join) REFERENCES dim_provincia(prov_key);"))
                        print(f"   ✅ {table} -> Conectada a Provincia (Plan B)")
                    except Exception as e:
                        print(f"   ❌ {table} no se pudo vincular: {e}")

def prepare_and_load():
    print("🧨 Reseteando Base de Datos para nueva carga limpia...")
    server_conn = f"mysql+pymysql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}"
    server_engine = create_engine(server_conn)
    with server_engine.connect() as conn:
        conn.execute(text("SET FOREIGN_KEY_CHECKS = 0;"))
        conn.execute(text(f"DROP DATABASE IF EXISTS {DB_NAME}"))
        conn.execute(text(f"CREATE DATABASE {DB_NAME}"))
    
    engine = create_engine(f"{server_conn}/{DB_NAME}")
    
    # He actualizado el diccionario con tus nuevos archivos
    datasets = {
        "demografia": pd.read_csv("data/clean/demografia_municipios_final.csv"),
        "conectividad": pd.read_csv("data/clean/conectividad_final_limpio.csv"), # ¡NUEVO!
        "migraciones": pd.read_csv("data/clean/migracion_municipios_final_limpio.csv"),
        "consumo_viviendas": pd.read_csv("data/clean/consumo_electrico_final_limpio.csv"), # ¡REEMPLAZADO!
        "osm": pd.read_csv("data/clean/muni_station_osm_limpio.csv"),
        "pib": pd.read_csv("data/clean/rentamedia_municipios_final_limpio.csv"),
        "viirs": pd.read_csv("data/clean/viirsFinal_limpio.csv")
    }

    all_munis_list = []
    all_provs_list = []

    for name, df in datasets.items():
        # Lógica para Geografía: He añadido 'LAU_NAME' y 'Nombre' para tus nuevos archivos
        col_muni = next((c for c in ['LAU_NAME', 'Nombre', 'nombre', 'municipio', 'nombre_municipio'] if c in df.columns), None)
        col_p = next((c for c in ['PROV_NAME', 'nombre_provincia', 'provincia', 'region_name', 'Nombre Provincia'] if c in df.columns), None)

        if col_muni:
            df['muni_id_join'] = df[col_muni].apply(limpiar_texto)
            muni_info = df[[col_muni, 'muni_id_join']].copy().rename(columns={col_muni: 'muni_display', 'muni_id_join': 'muni_key'})
            if col_p:
                df['prov_id_join'] = df[col_p].apply(limpiar_texto)
                muni_info['prov_id_join'] = df['prov_id_join']
            all_munis_list.append(muni_info)

        if col_p:
            df['prov_id_join'] = df[col_p].apply(limpiar_texto)
            prov_info = df[[col_p, 'prov_id_join']].copy().rename(columns={col_p: 'prov_display', 'prov_id_join': 'prov_key'})
            all_provs_list.append(prov_info)

    # Creamos las dimensiones maestras
    dim_provincia = pd.concat(all_provs_list).drop_duplicates('prov_key').dropna()
    dim_provincia.to_sql('dim_provincia', engine, if_exists='replace', index=False)

    dim_geografia = pd.concat(all_munis_list).drop_duplicates('muni_key').dropna(subset=['muni_key'])
    dim_geografia.to_sql('dim_geografia', engine, if_exists='replace', index=False)

    # Cargamos las tablas de hechos
    created_tables = ['dim_provincia', 'dim_geografia']
    for name, df in datasets.items():
        table_name = f'fact_{name}'
        df.to_sql(table_name, engine, if_exists='replace', index=False)
        created_tables.append(table_name)
        print(f"✅ {table_name} inyectada con éxito.")
    
    add_keys_and_indexes(engine, created_tables)
    print("\n🚀 ¡SISTEMA BIG DATA DESPLEGADO Y VINCULADO!")

if __name__ == "__main__":
    prepare_and_load()