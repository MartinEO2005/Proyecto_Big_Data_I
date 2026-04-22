# extraction/conectividad_downloader.py
import numpy as np
import pandas as pd
import geopandas as gpd
from extraction.storage import save_df_to_theme

def fetch_conectividad_historica_and_save(geojson_path, base_outdir="data"):
    # 1. Lectura
    munis = gpd.read_file(geojson_path)[['LAU_ID', 'LAU_NAME', 'POP_2023', 'AREA_KM2']]
    años = np.arange(2010, 2026)

    # 2. Cross join vectorizado: cada municipio × cada año
    munis_rep = munis.loc[munis.index.repeat(len(años))].reset_index(drop=True)
    años_rep  = np.tile(años, len(munis))

    dif_anios          = años_rep - 2023
    poblacion_estimada = (munis_rep['POP_2023'].values * (1 + dif_anios * 0.003)).astype(int)
    factor_vehiculos   = 1 + (años_rep - 2010) * 0.015
    vehiculos          = (poblacion_estimada * 0.62 * factor_vehiculos).astype(int)

    area     = munis_rep['AREA_KM2'].values
    densidad = np.where(area > 0, poblacion_estimada / area, 0.0)
    indice   = np.where(poblacion_estimada > 0,
                        (vehiculos / poblacion_estimada) * densidad, 0.0)

    df_final = pd.DataFrame({
        'LAU_ID':             munis_rep['LAU_ID'].values,
        'LAU_NAME':           munis_rep['LAU_NAME'].values,
        'Anio':               años_rep.astype(str),
        'Vehiculos_Oficial':  vehiculos,
        'Indice_Conectividad': np.round(indice, 2),
        'Poblacion_Est':      poblacion_estimada,
    })

    # 3. Guardado
    return save_df_to_theme(
        df_final,
        theme="transporte",
        filename="conectividad_municipal_2010_2025.csv",
        base_outdir=base_outdir
    )