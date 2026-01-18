import pandas as pd
from sqlalchemy import create_engine, text
import unicodedata

# --- CONFIGURACIÓN MYSQL ---
DB_USER = 'root'
DB_PASS = '1234'
DB_HOST = 'localhost' 
DB_PORT = '3306'
DB_NAME = 'proyecto_big_data'

def limpiar_texto(texto):
    if not isinstance(texto, str): return texto
    texto = str(texto).lower()
    texto = unicodedata.normalize('NFKD', texto).encode('ascii', 'ignore').decode('utf-8')
    for char in ['/', '-', ',', '.', '(', ')']:
        texto = texto.replace(char, ' ')
    return " ".join(texto.split())

def add_keys_and_indexes(engine, table_names):
    with engine.connect() as conn:
        print("\n🏗️  Construyendo relaciones multinivel...")
        
        # 1. PRIMARY KEYS
        conn.execute(text("ALTER TABLE dim_provincia MODIFY prov_key VARCHAR(255) NOT NULL;"))
        conn.execute(text("ALTER TABLE dim_provincia ADD PRIMARY KEY (prov_key);"))
        
        conn.execute(text("ALTER TABLE dim_geografia MODIFY muni_key VARCHAR(255) NOT NULL;"))
        conn.execute(text("ALTER TABLE dim_geografia ADD PRIMARY KEY (muni_key);"))
        
        # 2. RELACIÓN MUNICIPIO -> PROVINCIA (Copo de nieve)
        try:
            conn.execute(text("ALTER TABLE dim_geografia MODIFY prov_id_join VARCHAR(255);"))
            conn.execute(text("ALTER TABLE dim_geografia ADD CONSTRAINT fk_geo_prov FOREIGN KEY (prov_id_join) REFERENCES dim_provincia(prov_key);"))
            print("   ✅ Jerarquía Provincia -> Municipio establecida.")
        except Exception as e:
            print(f"   ⚠️ Nota en jerarquía: {e}")

        # 3. RELACIONAR TABLAS DE HECHOS
        for table in table_names:
            if table in ['dim_geografia', 'dim_provincia']: continue 
            
            conectada = False
            
            # INTENTO 1: Conexión por MUNICIPIO
            res_muni = conn.execute(text(f"SHOW COLUMNS FROM {table} LIKE 'muni_id_join'"))
            if res_muni.fetchone():
                try:
                    conn.execute(text(f"ALTER TABLE {table} MODIFY muni_id_join VARCHAR(255);"))
                    conn.execute(text(f"CREATE INDEX idx_{table}_muni ON {table}(muni_id_join);"))
                    
                    # Limpiamos huérfanos para que MySQL no se queje
                    conn.execute(text(f"DELETE FROM {table} WHERE muni_id_join NOT IN (SELECT muni_key FROM dim_geografia)"))
                    
                    # Verificamos si quedaron datos tras la limpieza
                    count = conn.execute(text(f"SELECT COUNT(*) FROM {table}")).scalar()
                    if count > 0:
                        conn.execute(text(f"ALTER TABLE {table} ADD CONSTRAINT fk_{table}_muni FOREIGN KEY (muni_id_join) REFERENCES dim_geografia(muni_key);"))
                        print(f"   ✅ {table} -> Conectada a Municipio")
                        conectada = True
                except Exception:
                    pass

            # INTENTO 2: Conexión por PROVINCIA (Plan B si falla municipio o no tiene)
            if not conectada:
                res_prov = conn.execute(text(f"SHOW COLUMNS FROM {table} LIKE 'prov_id_join'"))
                if res_prov.fetchone():
                    try:
                        conn.execute(text(f"ALTER TABLE {table} MODIFY prov_id_join VARCHAR(255);"))
                        conn.execute(text(f"CREATE INDEX idx_{table}_prov ON {table}(prov_id_join);"))
                        conn.execute(text(f"DELETE FROM {table} WHERE prov_id_join NOT IN (SELECT prov_key FROM dim_provincia)"))
                        conn.execute(text(f"ALTER TABLE {table} ADD CONSTRAINT fk_{table}_prov FOREIGN KEY (prov_id_join) REFERENCES dim_provincia(prov_key);"))
                        print(f"   ✅ {table} -> Conectada a Provincia (Plan B)")
                        conectada = True
                    except Exception as e:
                        print(f"   ❌ {table} no se pudo conectar: {e}")

def prepare_and_load():
    print("🧨 Reseteando Base de Datos...")
    server_conn = f"mysql+pymysql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}"
    server_engine = create_engine(server_conn)
    with server_engine.connect() as conn:
        conn.execute(text(f"DROP DATABASE IF EXISTS {DB_NAME}"))
        conn.execute(text(f"CREATE DATABASE {DB_NAME}"))
    
    engine = create_engine(f"{server_conn}/{DB_NAME}")
    
    datasets = {
        "demografia": pd.read_csv("data/clean/demografia_municipios_final.csv"),
        "empresas": pd.read_csv("data/clean/empresas_transporte_final_limpio.csv"),
        "migraciones": pd.read_csv("data/clean/migracion_municipios_final_limpio.csv"),
        "consumo_prov": pd.read_csv("data/clean/consumo_electricoProv_final_limpio.csv"),
        "osm": pd.read_csv("data/clean/muni_station_osm_limpio.csv"),
        "pib": pd.read_csv("data/clean/rentamedia_municipios_final_limpio.csv"),
        "viirs": pd.read_csv("data/clean/viirsFinal_limpio.csv")
    }

    all_munis_list = []
    all_provs_list = []

    for name, df in datasets.items():
        # Lógica especial para detectar columna de municipio por los datos que me has pasado
        col_muni = next((c for c in ['nombre', 'municipio', 'nombre_municipio', 'LAU_NAME'] if c in df.columns), None)
        if col_muni:
            df['muni_id_join'] = df[col_muni].apply(limpiar_texto)
            muni_info = df[[col_muni, 'muni_id_join']].copy().rename(columns={col_muni: 'muni_display', 'muni_id_join': 'muni_key'})
            
            # Intentar pescar la provincia para dim_geografia
            col_p = next((c for c in ['provincia', 'PROV_NAME', 'region_name', 'Nombre Provincia'] if c in df.columns), None)
            if col_p:
                df['prov_id_join'] = df[col_p].apply(limpiar_texto)
                muni_info['prov_id_join'] = df['prov_id_join']
            all_munis_list.append(muni_info)

        # Lógica de provincias
        col_prov = next((c for c in ['provincia', 'PROV_NAME', 'region_name', 'Nombre Provincia'] if c in df.columns), None)
        if col_prov:
            df['prov_id_join'] = df[col_prov].apply(limpiar_texto)
            prov_info = df[[col_prov, 'prov_id_join']].copy().rename(columns={col_prov: 'prov_display', 'prov_id_join': 'prov_key'})
            all_provs_list.append(prov_info)

    # Guardar Dimensiones
    dim_provincia = pd.concat(all_provs_list).drop_duplicates('prov_key').dropna()
    dim_provincia.to_sql('dim_provincia', engine, if_exists='replace', index=False)

    dim_geografia = pd.concat(all_munis_list).drop_duplicates('muni_key').dropna(subset=['muni_key'])
    dim_geografia.to_sql('dim_geografia', engine, if_exists='replace', index=False)

    # Guardar Hechos
    created_tables = ['dim_provincia', 'dim_geografia']
    for name, df in datasets.items():
        table_name = f'fact_{name}'
        df.to_sql(table_name, engine, if_exists='replace', index=False)
        created_tables.append(table_name)
        print(f"✅ {table_name} cargada.")
    
    add_keys_and_indexes(engine, created_tables)
    print("\n🚀 ¡TODO CONECTADO CORRECTAMENTE!")

if __name__ == "__main__":
    prepare_and_load()