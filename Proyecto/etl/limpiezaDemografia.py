import pandas as pd
import os
import numpy as np

# IMPORTAMOS NUESTRA VERDAD ABSOLUTA
from geo_utils import get_maestro_municipios, limpiar_texto

MUNI_RAW = "data/demografia/demografia_poblacion_municipios.csv"
GEOJSON = "municipios_es.geojson"
PROV_CSV = "data/demografia/demografia_poblacion_provincias.csv"
OUTPUT = "data/clean/demografia_municipios_final.csv"

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

def main():
    print("Iniciando limpieza de Demografía...")
    
    # 1. Cargamos el maestro geográfico
    print("   - Cargando GeoJSON maestro...")
    df_maestro = get_maestro_municipios(GEOJSON)
    mapa_ids = dict(zip(df_maestro['union_key'], df_maestro['muni_key']))
    
    # 🚨 LA GRAN SOLUCIÓN: Mapa de rescate para adivinar la provincia 🚨
    mapa_rescate_prov = dict(zip(df_maestro['muni_display'].apply(limpiar_texto), df_maestro['prov_key']))

    # 2. Carga y separación de géneros
    print("   - Procesando datos a nivel municipal (Extrayendo Total/Hombres/Mujeres)...")
    df_m = pd.read_csv(MUNI_RAW, dtype=str)

    # Separamos "Ababuj. Total. Personas" en columnas útiles
    split_data = df_m['municipio'].str.split('.', n=2, expand=True)
    df_m['muni_real'] = split_data[0].str.strip()
    df_m['categoria'] = split_data[1].str.strip() 

    # 🚨 APLICAMOS EL RESCATE DE PROVINCIA 🚨
    df_m['muni_clean'] = df_m['muni_real'].apply(limpiar_texto)
    df_m['region_code'] = df_m['muni_clean'].map(mapa_rescate_prov)

    # Aseguramos el formato
    df_m['region_code'] = df_m['region_code'].astype(str).str.zfill(2)
    df_m['year'] = pd.to_numeric(df_m['year'], errors='coerce')
    df_m['population'] = pd.to_numeric(df_m['population'], errors='coerce')

    # Eliminamos las filas donde no logramos rescatar el código (no nos interesan si no están en OSM)
    df_m = df_m.dropna(subset=['year'])
    df_m = df_m[df_m['region_code'] != 'nan']
    
    # Pivoteamos para crear las columnas: Total, Hombres, Mujeres
    df_pivot = df_m.pivot_table(
        index=['region_code', 'muni_real', 'year'], 
        columns='categoria',
        values='population', 
        aggfunc='sum'
    ).reset_index()
    
    df_pivot = df_pivot.rename(columns={'muni_real': 'municipio'})

    print("   - Procesando datos a nivel provincial...")
    df_p = pd.read_csv(PROV_CSV, dtype=str)
    
    if 'cod_prov' in df_p.columns:
        df_p = df_p.rename(columns={'cod_prov': 'region_code'})
        
    df_p['region_code'] = df_p['region_code'].str.zfill(2)
    df_p['year'] = pd.to_numeric(df_p['year'], errors='coerce')
    df_p['population'] = pd.to_numeric(df_p['population'], errors='coerce')
    df_p = df_p.dropna(subset=['region_code', 'year'])

    # Unimos municipios con sus totales provinciales
    df_final = pd.merge(
        df_pivot, 
        df_p[['region_code', 'year', 'population']], 
        on=['region_code', 'year'], 
        how='left'
    )
    
    df_final = df_final.rename(columns={'population': 'provincia_population'})
    df_final['region_name'] = df_final['region_code'].map(INE_PROV_MAP)

    df_final = df_final.sort_values(['region_code', 'year'])

    print("   - Rellenando huecos de población provincial...")
    df_final['provincia_population'] = df_final.groupby('region_code')['provincia_population'].ffill().bfill()
    
    # Aseguramos que existan las columnas incluso si el INE no mandó alguna
    for col in ['Total', 'Hombres', 'Mujeres']:
        if col not in df_final.columns:
            df_final[col] = np.nan

    # Volvemos a tu orden estricto original
    columnas_orden = ['region_code', 'region_name', 'year', 'provincia_population', 'municipio', 'Total', 'Hombres', 'Mujeres']
    df_final = df_final[columnas_orden]

    # 3. EL CRUCE DEFINITIVO
    print("   - Mapeando LAU_ID oficiales...")
    df_final['muni_clean'] = df_final['municipio'].apply(limpiar_texto)
    df_final['union_key'] = df_final['region_code'] + "_" + df_final['muni_clean']
    
    # Inyectamos el ID oficial de GeoJSON
    df_final['muni_id_join'] = df_final['union_key'].map(mapa_ids)
    
    # 4. Limpiamos y guardamos
    columnas_basura = ['muni_clean', 'union_key']
    df_final = df_final.drop(columns=[c for c in columnas_basura if c in df_final.columns])
    
    df_final.to_csv(OUTPUT, index=False)
    print(f"✅ Demografía exportada con éxito: {OUTPUT} ({len(df_final)} filas)")

if __name__ == "__main__":
    main()