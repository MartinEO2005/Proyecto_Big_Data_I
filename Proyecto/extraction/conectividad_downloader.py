# extraction/conectividad_downloader.py
import pandas as pd
import geopandas as gpd
import os
from extraction.storage import save_df_to_theme

def fetch_conectividad_historica_and_save(geojson_path, base_outdir="data"):
    # 1. Lectura
    munis = gpd.read_file(geojson_path)
    
    años = [str(anio) for anio in range(2010, 2026)] 
    resultados = []

    # 2. Procesamiento (tu lógica que sí funcionaba)
    for _, row in munis.iterrows():
        pop_base = row['POP_2023']
        area = row['AREA_KM2']
        
        for anio in años:
            dif_anios = int(anio) - 2023
            poblacion_estimada = int(pop_base * (1 + (dif_anios * 0.003)))
            factor_vehiculos = 1 + (int(anio) - 2010) * 0.015
            vehiculos = int(poblacion_estimada * 0.62 * factor_vehiculos)
            
            densidad = poblacion_estimada / area if area > 0 else 0
            indice = (vehiculos / poblacion_estimada) * densidad if poblacion_estimada > 0 else 0
            
            resultados.append({
                'LAU_ID': row['LAU_ID'],
                'LAU_NAME': row['LAU_NAME'],
                'Anio': anio,
                'Vehiculos_Oficial': vehiculos,
                'Indice_Conectividad': round(indice, 2),
                'Poblacion_Est': poblacion_estimada # Columna extra de regalo
            })

    df_final = pd.DataFrame(resultados)
    
    # 3. Guardado usando tu infraestructura
    # Esto fallaba porque "movilidad" no estaba en THEME_DIRS
    # En extraction/conectividad_downloader.py cambia el final:
    return save_df_to_theme(
        df_final, 
        theme="transporte", # <--- Cambia "movilidad" por "transporte" temporalmente
        filename="conectividad_municipal_2010_2025.csv", 
        base_outdir=base_outdir
    )