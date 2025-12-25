# DemografiaProvincias.py - VERSIÓN CON URL ESTABLE (Tabla 7421)

import requests
import pandas as pd
import json 
from pyjstat import pyjstat 
from pathlib import Path

# --- DUMMY FUNCTION (Asegúrate de que 'storage.py' funcione o usa esta) ---
def save_df_to_theme(df, theme, filename, base_outdir):
    out_path = Path(base_outdir) / theme / filename
    out_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(out_path, index=False, encoding='utf-8-sig')
    return out_path
# -------------------------------------------------------------------------

__all__ = ["fetch_population_total_nuts3", "fetch_population_and_save"]

# --- Nueva Configuración: API del INE (Tabla 7421: Población residente) ---
# 🚨 URL FINAL y ESTABLE 🚨
INE_API_URL = "https://servicios.ine.es/wstempus/js/es/DATOS_TABLA/t20/e7421/p08/l0.json"


def fetch_population_total_nuts3():
    """
    Descarga población total por provincia desde la API del INE (Tabla 7421).
    """
    try:
        print(f"[INE] Intentando descargar datos desde la fuente estable (Tabla 7421): {INE_API_URL}")
        r = requests.get(INE_API_URL, timeout=60)
        r.raise_for_status() 
        
        print("✅ Conexión exitosa al INE. Procesando JSON-STAT...")
        
        # 1. Cargar el JSON-STAT del INE
        data_json = json.loads(r.text)
        
        # 2. Convertir el objeto JSON-STAT a un DataFrame de pandas usando pyjstat
        dataset_list = pyjstat.Dataset.read(data_json)
        
        if not dataset_list:
             print("❌ El JSON-STAT del INE no contiene datasets.")
             return pd.DataFrame()
             
        # Usamos 'omit' para obtener los totales por Sexo, Edad y Nacionalidad
        df = dataset_list[0].to_dataframe(
            omit=['Sexo', 'Edad (años)'] # La tabla 7421 tiene estas dos dimensiones clave.
        )
        
        # 3. Limpieza y Formato
        df.columns = [c.lower() for c in df.columns]

        col_provincia = next((c for c in df.columns if 'territorio' in c or 'provincia' in c), 'territorio')
        col_año = next((c for c in df.columns if 'período' in c or 'año' in c), 'período')
        col_valor = 'valor'

        if col_provincia not in df.columns or col_año not in df.columns:
            print("❌ No se pudieron identificar las columnas clave ('Provincia' o 'Período') tras pyjstat.")
            return pd.DataFrame()
        
        df_final = df[[col_provincia, col_año, col_valor]].copy()
        df_final.columns = ['region_name', 'year', 'population']

        df_final['region_code'] = df_final['region_name']
        
        df_final = df_final[['region_code', 'region_name', 'year', 'population']]
        df_final['population'] = pd.to_numeric(df_final['population'], errors='coerce')
        df_final['year'] = pd.to_numeric(df_final['year'], errors='coerce').astype('Int64')
        df_final = df_final.dropna(subset=['population', 'year'])

        print(f"✅ Se descargaron {len(df_final)} registros de población total (INE)")
        return df_final

    except requests.exceptions.HTTPError as e:
        print(f"❌ Error HTTP al conectar con el INE: {e.response.status_code}. La nueva URL podría haber fallado también.")
        return pd.DataFrame()
    except Exception as e:
        print(f"❌ Error inesperado al procesar el JSON-STAT del INE: {e}")
        import traceback
        traceback.print_exc()
        return pd.DataFrame()


def fetch_population_and_save(base_outdir="outputs/data", filename="demografia_poblacion_provincias.csv"):
    """
    Función principal llamada desde el main.py.
    """
    df = fetch_population_total_nuts3()
    if df is None or df.empty:
        print("⚠️ No hay datos demográficos para guardar.")
        return None

    path = save_df_to_theme(df, theme="demografia", filename=filename, base_outdir=base_outdir)
    print(f"💾 Datos demográficos guardados en: {path}")
    return path


if __name__ == "__main__":
    fetch_population_and_save()
    
    df_test = fetch_population_total_nuts3()
    if not df_test.empty:
        print("\nPrueba de coherencia (Suma total de provincias por año):")
        print(df_test.groupby('year')['population'].sum().tail(5))