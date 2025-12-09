# demografiaProvincias.py
import requests
import pandas as pd
from pathlib import Path
from jsonstat import load as load_jsonstat # 📦 Usaremos una librería de parsing más genérica
from storage import save_df_to_theme

__all__ = ["fetch_population_total_nuts3", "fetch_population_and_save"]

# --- Nueva Configuración: API del INE (Tabla 2852: Población por Provincia, Sexo y Edad) ---
# Usamos el endpoint del INE para el JSON de la tabla 2852.
INE_API_URL = "https://servicios.ine.es/wstempus/js/es/DATOS_TABLA/t20/e245/p08/l0/000.json"


def fetch_population_total_nuts3():
    """
    Descarga población total por provincia desde la API del INE.
    Utiliza jsonstat.load para asegurar el correcto mapeo de dimensiones.
    """
    try:
        print("[INE] Descargando datos de población por provincia...")
        r = requests.get(INE_API_URL, timeout=60)
        r.raise_for_status()
        
        # 1. Cargar el JSON-STAT del INE usando la librería
        data = load_jsonstat(r.text)
        
        # 2. El dataset del INE (Tabla 2852) tiene 5 dimensiones:
        # 0: Territorio (Provincias), 1: Sexo, 2: Edad, 3: Nacionalidad, 4: Periodo
        
        # Filtramos el Dataset para obtener: 
        # - Sexo: Total (T)
        # - Edad: Todos (TOTAL)
        # - Nacionalidad: Total (TOTAL)
        
        # Buscamos los índices de los filtros que necesitamos
        # Esto depende de cómo el INE haya etiquetado internamente:
        
        # Dataset 0 es la tabla principal.
        dataset = data.dataset(0) 
        
        # 3. Aplicar filtros: Necesitamos TOTAL en Sexo, Edad y Nacionalidad.
        # Creamos un filtro usando los IDs o etiquetas internas del dataset.
        df = dataset.to_dataframe()

        # 4. Limpieza y filtrado (Los nombres de columna pueden variar, pero suelen ser 'Territorio' o 'periodo')
        
        # Renombrar columnas clave (asumiendo nombres típicos del INE/jsonstat)
        df.columns = df.columns.str.lower()
        
        # El INE devuelve nombres completos (ej. 'Almería') en lugar de códigos NUTS.
        # Usaremos 'Provincia' para la columna de región.
        
        # Intentamos identificar la columna de Territorio/Provincia y Periodo/Año
        col_provincia = next((c for c in df.columns if 'territorio' in c or 'provincia' in c), None)
        col_año = next((c for c in df.columns if 'período' in c or 'año' in c), None)
        col_sexo = next((c for c in df.columns if 'sexo' in c), None)
        col_edad = next((c for c in df.columns if 'edad' in c), None)
        col_nacionalidad = next((c for c in df.columns if 'nacionalidad' in c), None)
        col_valor = 'valor' # El valor de población

        if not col_provincia or not col_año:
            print("❌ No se pudieron identificar las columnas clave ('Provincia' o 'Período').")
            return pd.DataFrame()
        
        # Aplicar filtros (si las columnas existen y si no se aplicaron en la descarga)
        if col_sexo:
            df = df[df[col_sexo].str.contains('Total', case=False, na=False)]
        if col_edad:
            df = df[df[col_edad].str.contains('Total', case=False, na=False)]
        if col_nacionalidad:
            df = df[df[col_nacionalidad].str.contains('Total', case=False, na=False)]
            
        # 5. Formato de salida
        df_final = df[[col_provincia, col_año, col_valor]].copy()
        df_final.columns = ['region_name', 'year', 'population']

        # Añadir código (usaremos el nombre como código ya que el INE no siempre da NUTS)
        df_final['region_code'] = df_final['region_name']
        
        # Reordenar y limpiar
        df_final = df_final[['region_code', 'region_name', 'year', 'population']]
        df_final['population'] = pd.to_numeric(df_final['population'], errors='coerce')
        df_final = df_final.dropna(subset=['population'])

        print(f"✅ Se descargaron {len(df_final)} registros de población total (INE)")
        return df_final

    except requests.exceptions.RequestException as e:
        print("❌ Error de conexión al API del INE:", e)
        return pd.DataFrame()
    except Exception as e:
        print(f"❌ Error al procesar el JSON-STAT del INE: {e}")
        # Muestra el error de la librería si falla, que es más útil
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

    # Usa el sistema de carpetas de storage.py
    path = save_df_to_theme(df, theme="demografia", filename=filename, base_outdir=base_outdir)
    print(f"💾 Datos demográficos guardados en: {path}")
    return path


if __name__ == "__main__":
    fetch_population_and_save()
    
    # Prueba de coherencia
    df_test = fetch_population_total_nuts3()
    if not df_test.empty:
        print("\nPrueba de coherencia (Suma total de provincias por año):")
        # El resultado debe ser coherente (ej: ~47 millones para años recientes)
        print(df_test.groupby('year')['population'].sum().tail(5))