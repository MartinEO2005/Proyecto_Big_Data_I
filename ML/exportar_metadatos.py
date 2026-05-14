# exportar_metadatos.py
import json
import os
import sys
import pandas as pd
import numpy as np

# 1. CONFIGURACIÓN DE RUTAS
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.append(BASE_DIR)

try:
    from pipeline_clustering import trozo_1_matriz_maestra, trozo_2_machine_learning, COLORES_GEOLUMICA
except ImportError:
    print("❌ Error: Coloca este script en la misma carpeta que 'pipeline_clustering.py'")
    sys.exit()

MODEL_DIR = os.path.join(BASE_DIR, "models", "modelos_exportados")
os.makedirs(MODEL_DIR, exist_ok=True)

def generar_metadatos_geolumica():
    print("🚀 Iniciando exportación de inteligencia territorial...")

    # 2. EJECUCIÓN DEL PIPELINE
    df_maestra = trozo_1_matriz_maestra()
    df_final = trozo_2_machine_learning(df_maestra)

    # 3. IDENTIFICACIÓN DINÁMICA DE COLUMNAS
    # Buscamos los nombres de columnas que realmente existen en tu df_final
    posibles_columnas = {
        'poblacion': ['pob_absoluta_actual', 'pob', 'Total'],
        'pib': ['pib_act', 'pib', 'pib_per_capita'],
        'luz': ['luz_act', 'luz', 'mean'],
        'delta_pob': ['delta_pob_pct', 'crecimiento_pob'],
        'empresas': ['delta_emp_pct', 'emp', 'num_empresas_transporte'],
        'conectividad': ['con', 'Indice_Conectividad']
    }

    columnas_reales = []
    for categoria, opciones in posibles_columnas.items():
        for opcion in opciones:
            if opcion in df_final.columns:
                columnas_reales.append(opcion)
                break # Encontrada, pasamos a la siguiente categoría

    print(f"📊 Calculando medias para las columnas encontradas: {columnas_reales}")
    
    # 4. CÁLCULO DE ESTADÍSTICAS
    stats_perfiles = df_final.groupby('Perfil_Final')[columnas_reales].mean().round(2).to_dict(orient='index')

    # 5. MAPEADO DE IDENTIDADES
    id_col = 'muni_id_join' if 'muni_id_join' in df_final.columns else 'LAU_ID'
    mapeo_municipios = dict(zip(df_final[id_col], df_final['Perfil_Final']))

    # 6. GUARDADO
    metadata_final = {
        "configuracion": { "colores": COLORES_GEOLUMICA },
        "estadisticas_perfiles": stats_perfiles,
        "municipios": mapeo_municipios
    }

    ruta_archivo = os.path.join(MODEL_DIR, "geolumica_metadata.json")
    with open(ruta_archivo, 'w', encoding='utf-8') as f:
        json.dump(metadata_final, f, ensure_ascii=False, indent=4)

    print(f"✅ ¡LOGRADO! Metadatos exportados en: {ruta_archivo}")

if __name__ == "__main__":
    generar_metadatos_geolumica()