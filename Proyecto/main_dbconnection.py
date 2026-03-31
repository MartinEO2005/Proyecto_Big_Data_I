import pandas as pd
from sqlalchemy import create_engine, text
import unicodedata
import os

# --- CONFIGURACIÓN DE CONEXIÓN ---
DB_USER = os.getenv('DB_USER', 'root')
DB_PASS = os.getenv('DB_PASS', '1234')
DB_HOST = '127.0.0.1'
DB_PORT = os.getenv('DB_PORT', '3306')
DB_NAME = os.getenv('DB_NAME', 'proyecto_big_data')

# --- DICCIONARIOS DE NORMALIZACIÓN ---
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

DICCIONARIO_REBELDES = {
    "manjabalago y ortigosa de rioa": "manjabalago y ortigosa de rioalmar",
    "san martin de la vega del albe": "san martin de la vega del alberche",
    "partido de la sierra en tobalin": "partido de la sierra en tobalina",
    "quintanilla del agua y torduele": "quintanilla del agua y tordueles",
    "villarcayo de merindad de casti": "villarcayo de merindad de castilla la vieja",
    "san sebastian de los ballester": "san sebastian de los ballesteros",
    "pontes de garcia rodriguez a": "as pontes de garcia rodriguez",
    "gargantilla del lozoya y pinill": "gargantilla del lozoya y pinilla de buitrago",
    "bustillo del paramo de carrio": "bustillo del paramo de carrion",
    "santa maria de guia de gran c": "santa maria de guia de gran canaria",
    "montejo de la vega de la serrez": "montejo de la vega de la serrezuela",
    "vandellos i l hospitalet de l": "vandellos i l hospitalet de l infant",
    "villanueva del rebollar de la s": "villanueva del rebollar de la sierra",
    "san martin de la virgen de mon": "san martin de la virgen de moncayo",
    "cruilles monells i sant sadur": "cruilles monells i sant sadurni de l heura"
}

def limpiar_texto(texto):
    if not isinstance(texto, str): return "desconocido"
    
    # 1. Manejar nombres invertidos (Ej: "Molar, El" -> "El Molar")
    if ',' in texto:
        partes = texto.split(',')
        if len(partes) == 2 and len(partes[1].strip()) <= 3:
            texto = f"{partes[1].strip()} {partes[0].strip()}"

    # 2. Normalización básica (quitar bilingüismo, acentos y minúsculas)
    texto = texto.split('/')[0].split('-')[0].strip().lower()
    texto = "".join(
        c for c in unicodedata.normalize('NFKD', texto)
        if not unicodedata.combining(c)
    )
    
    # 3. Limpieza de símbolos
    for char in [',', '.', '(', ')', '*', '\"', '\'', '  ']:
        texto = texto.replace(char, ' ')
    
    resultado = " ".join(texto.split())
    
    # 4. Aplicar correcciones manuales para nombres truncados
    return DICCIONARIO_REBELDES.get(resultado, resultado)

def add_keys_and_indexes(engine, table_names):
    with engine.connect() as conn:
        print("\n🏗️  Estableciendo integridad referencial (PK/FK)...")
        
        # Primary Keys
        conn.execute(text("ALTER TABLE dim_provincia MODIFY prov_key VARCHAR(255) NOT NULL;"))
        if 'PRIMARY' not in [r[0] for r in conn.execute(text("SHOW KEYS FROM dim_provincia WHERE Key_name = 'PRIMARY'")).fetchall()]:
            conn.execute(text("ALTER TABLE dim_provincia ADD PRIMARY KEY (prov_key);"))
        
        conn.execute(text("ALTER TABLE dim_geografia MODIFY muni_key VARCHAR(255) NOT NULL;"))
        if 'PRIMARY' not in [r[0] for r in conn.execute(text("SHOW KEYS FROM dim_geografia WHERE Key_name = 'PRIMARY'")).fetchall()]:
            conn.execute(text("ALTER TABLE dim_geografia ADD PRIMARY KEY (muni_key);"))

        # Índices en tablas de hechos
        for table in table_names:
            if table in ['dim_geografia', 'dim_provincia']: continue
            try:
                conn.execute(text(f"ALTER TABLE {table} MODIFY muni_id_join VARCHAR(255);"))
                conn.execute(text(f"CREATE INDEX idx_{table}_muni ON {table}(muni_id_join);"))
                print(f"   ✅ {table} indexada.")
            except Exception: pass
        conn.commit()

def prepare_and_load():
    print(f"🧨 Reiniciando base de datos: {DB_NAME}")
    server_conn = f"mysql+pymysql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}"
    server_engine = create_engine(server_conn)
    with server_engine.connect() as conn:
        conn.execute(text(f"DROP DATABASE IF EXISTS {DB_NAME}"))
        conn.execute(text(f"CREATE DATABASE {DB_NAME}"))
    
    engine = create_engine(f"{server_conn}/{DB_NAME}")
    
    datasets = {
        "demografia": pd.read_csv("data/clean/demografia_municipios_final.csv"),
        "conectividad": pd.read_csv("data/clean/conectividad_final_limpio.csv"),
        "migraciones": pd.read_csv("data/clean/migracion_municipios_final_limpio.csv"),
        "consumo_viviendas": pd.read_csv("data/clean/consumo_electrico_final_limpio.csv"),
        "osm": pd.read_csv("data/clean/muni_station_osm_limpio.csv"),
        "pib": pd.read_csv("data/clean/rentamedia_municipios_final_limpio.csv"),
        "viirs": pd.read_csv("data/clean/viirsFinal_limpio.csv")
    }

    # --- FASE 1: DIMENSIONES (Skeleton de 8131 municipios) ---
    print("📋 Creando Dimensiones Maestras...")
    df_base = datasets["osm"] 
    
    # Provincias
    df_base['prov_key'] = df_base['INE_PROV_CODE'].apply(lambda x: str(int(x)).zfill(2))
    df_base['prov_display'] = df_base['prov_key'].map(INE_PROV_MAP)
    dim_provincia = df_base[['prov_display', 'prov_key']].drop_duplicates().dropna()
    dim_provincia.to_sql('dim_provincia', engine, if_exists='replace', index=False)

    # Geografía (muni_key de 5 dígitos)
    df_base['muni_key'] = df_base['LAU_ID'].apply(lambda x: str(int(x)).zfill(5))
    dim_geografia = df_base[['LAU_NAME', 'muni_key', 'prov_key']].copy()
    dim_geografia.columns = ['muni_display', 'muni_key', 'prov_id_join']
    dim_geografia = dim_geografia.drop_duplicates('muni_key')
    dim_geografia.to_sql('dim_geografia', engine, if_exists='replace', index=False)
    
    print(f"✅ Dim_Geografia: {len(dim_geografia)} municipios inyectados.")

    # --- FASE 2: TABLAS DE HECHOS ---
    # Crear un mapeo de Nombre Limpio + Provincia -> Código INE para los casos sin ID numérico
    df_base['muni_clean'] = df_base['LAU_NAME'].apply(limpiar_texto)
    df_base['union_key'] = df_base['prov_key'] + "_" + df_base['muni_clean']
    mapa_nombre_a_id = dict(zip(df_base['union_key'], df_base['muni_key']))

    created_tables = ['dim_provincia', 'dim_geografia']
    for name, df in datasets.items():
        table_name = f'fact_{name}'
        
        # Prioridad 1: Identificar columna de ID numérico
        col_id = next((c for c in df.columns if c.lower() in ['lau_id', 'codigo_municipio', 'codigo', 'codigo_muni']), None)
        
        if col_id:
            df['muni_id_join'] = df[col_id].apply(lambda x: str(int(float(x))).zfill(5) if pd.notnull(x) else None)
        else:
            # Prioridad 2: Si no hay ID, usar Nombre + Provincia
            col_m = next((c for c in df.columns if c.lower() in ['municipio', 'nombre', 'nombre_municipio', 'lau_name']), None)
            col_p = next((c for c in df.columns if c.lower() in ['region_code', 'prov_name', 'provincia', 'ine_prov_code']), None)
            
            if col_m and col_p:
                df['p_tmp'] = df[col_p].apply(lambda x: str(int(float(x))).zfill(2) if str(x).isdigit() else None)
                df['m_tmp'] = df[col_m].apply(limpiar_texto)
                df['u_tmp'] = df['p_tmp'] + "_" + df['m_tmp']
                df['muni_id_join'] = df['u_tmp'].map(mapa_nombre_a_id)
                df.drop(columns=['p_tmp', 'm_tmp', 'u_tmp'], inplace=True)

        df.to_sql(table_name, engine, if_exists='replace', index=False)
        created_tables.append(table_name)
        print(f"✅ {table_name} cargada.")

    add_keys_and_indexes(engine, created_tables)
    print("\n🚀 PROYECTO GEO-LÚMICA: Almacén de datos desplegado al 100%.")

if __name__ == "__main__":
    prepare_and_load()