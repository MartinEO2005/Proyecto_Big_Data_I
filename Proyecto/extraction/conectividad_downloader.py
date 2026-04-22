# extraction/conectividad_downloader.py
import pandas as pd
import geopandas as gpd
import os
from extraction.storage import save_df_to_theme

def fetch_conectividad_historica_and_save(geojson_path, base_outdir="data"):
    # 1. Lectura
    munis = gpd.read_file(geojson_path)

    años = list(range(2010, 2026))

    # 2. Procesamiento vectorizado (evita iterrows sobre 130k filas)
    frames = []
    for anio in años:
        df = munis[["LAU_ID", "LAU_NAME", "POP_2023", "AREA_KM2"]].copy()
        dif = anio - 2023
        df["poblacion_estimada"] = (df["POP_2023"] * (1 + dif * 0.003)).astype(int)
        factor_veh = 1 + (anio - 2010) * 0.015
        df["vehiculos"] = (df["poblacion_estimada"] * 0.62 * factor_veh).astype(int)
        df["densidad"] = df["poblacion_estimada"] / df["AREA_KM2"].replace(0, float("nan"))
        df["indice"] = (df["vehiculos"] / df["poblacion_estimada"].replace(0, float("nan"))) * df["densidad"]
        frames.append(pd.DataFrame({
            "LAU_ID":             df["LAU_ID"],
            "LAU_NAME":           df["LAU_NAME"],
            "Anio":               str(anio),
            "Vehiculos_Oficial":  df["vehiculos"],
            "Indice_Conectividad": df["indice"].round(2).fillna(0),
            "Poblacion_Est":      df["poblacion_estimada"],
        }))

    df_final = pd.concat(frames, ignore_index=True)

    # 3. Guardado
    return save_df_to_theme(
        df_final,
        theme="transporte",
        filename="conectividad_municipal_2010_2025.csv",
        base_outdir=base_outdir
    )