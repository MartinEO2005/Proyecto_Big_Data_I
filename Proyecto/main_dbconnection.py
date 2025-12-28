import pandas as pd
from sqlalchemy import create_engine
import unicodedata

def limpiar_texto(texto):
    """Limpia nombres para que 'Álava' y 'alava' coincidan."""
    if not isinstance(texto, str): return texto
    texto = texto.lower()
    # Quitar acentos y caracteres especiales
    texto = unicodedata.normalize('NFKD', texto).encode('ascii', 'ignore').decode('utf-8')
    # Quitar caracteres comunes de separacion
    for char in ['/', '-', ',', '.']:
        texto = texto.replace(char, ' ')
    return " ".join(texto.split()) # Quita espacios extra

def prepare_and_load():
    # 1. Configurar conexión (Ejemplo con SQLite, puedes cambiarlo a Postgres/MySQL)
    engine = create_engine('sqlite:///proyecto_open_data.db')
    
    # 2. Carga de CSVs
    datasets = {
        "demografia": pd.read_csv("data/clean/demografia_municipios_final.csv"),
        "empresas": pd.read_csv("data/clean/empresas_transporte_final_limpio.csv"),
        "migraciones": pd.read_csv("data/clean/migracion_municipios_final_limpio.csv"),
        "consumo_prov": pd.read_csv("data/clean/consumo_electricoProv_final_limpio.csv"),
        "osm": pd.read_csv("data/clean/muni_station_osm_limpio.csv"),
        "pib": pd.read_csv("data/clean/rentamedia_municipios_final_limpio.csv"),
        "viirs": pd.read_csv("data/clean/viirsFinal_limpio.csv")
    }

    # 3. CREAR COLUMNA DE UNIÓN (id_join)
    # Aplicamos la limpieza a las columnas de municipio y provincia en todos los datasets
    for name, df in datasets.items():
        if 'municipio' in df.columns:
            df['muni_id_join'] = df['municipio'].apply(limpiar_texto)
        elif 'nombre_municipio' in df.columns:
            df['muni_id_join'] = df['nombre_municipio'].apply(limpiar_texto)
        elif 'LAU_NAME' in df.columns:
            df['muni_id_join'] = df['LAU_NAME'].apply(limpiar_texto)
            
        if 'provincia' in df.columns:
            df['prov_id_join'] = df['provincia'].apply(limpiar_texto)
        elif 'region_name' in df.columns:
            df['prov_id_join'] = df['region_name'].apply(limpiar_texto)
        elif 'PROV_NAME' in df.columns:
            df['prov_id_join'] = df['PROV_NAME'].apply(limpiar_texto)

    # 4. CREAR TABLA MAESTRA GEOGRÁFICA
    # Usamos OSM como base porque suele ser muy completo
    geo_master = datasets['osm'][['muni_id_join', 'LAU_NAME', 'prov_id_join', 'PROV_NAME']].drop_duplicates()
    geo_master.columns = ['muni_key', 'muni_display', 'prov_key', 'prov_display']
    geo_master.to_sql('dim_geografia', engine, if_exists='replace', index=False)

    # 5. CARGAR TABLAS DE HECHOS
    for name, df in datasets.items():
        # Guardamos cada dataframe como una tabla en la BBDD
        df.to_sql(f'fact_{name}', engine, if_exists='replace', index=False)
        print(f"Tabla fact_{name} cargada con éxito.")

if __name__ == "__main__":
    prepare_and_load()