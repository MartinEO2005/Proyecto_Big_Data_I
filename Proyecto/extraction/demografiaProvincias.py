import requests
import pandas as pd
from pathlib import Path
from extraction.storage import save_df_to_theme

__all__ = ["fetch_population_total_nuts3", "fetch_population_and_save"]

INE_API_URL = "https://servicios.ine.es/wstempus/js/es/DATOS_TABLA/2852"

def fetch_population_total_nuts3():
    try:
        print(f"[INE] Conectando a la API (Tabla 2852)...")
        r = requests.get(INE_API_URL, timeout=60)
        r.raise_for_status()
        series_list = r.json()

        data_list = []

        # Diccionario auxiliar para poner los códigos que faltan en la API
        # Esto asegura que 'Albacete' siempre tenga su '02'
        mapa_codigos = {
            'Albacete': '02', 'Alicante/Alacant': '03', 'Almería': '04', 'Araba/Álava': '01',
            'Asturias': '33', 'Ávila': '05', 'Badajoz': '06', 'Balears, Illes': '07',
            'Barcelona': '08', 'Bizkaia': '48', 'Burgos': '09', 'Cáceres': '10',
            'Cádiz': '11', 'Cantabria': '39', 'Castellón/Castelló': '12', 'Ciudad Real': '13',
            'Córdoba': '14', 'Coruña, A': '15', 'Cuenca': '16', 'Gipuzkoa': '20',
            'Girona': '17', 'Granada': '18', 'Guadalajara': '19', 'Huelva': '21',
            'Huesca': '22', 'Jaén': '23', 'León': '24', 'Lleida': '25', 'Lugo': '27',
            'Madrid': '28', 'Málaga': '29', 'Murcia': '30', 'Navarra': '31',
            'Ourense': '32', 'Palencia': '34', 'Palmas, Las': '35', 'Pontevedra': '36',
            'Rioja, La': '26', 'Salamanca': '37', 'Santa Cruz de Tenerife': '38',
            'Segovia': '40', 'Sevilla': '41', 'Soria': '42', 'Tarragona': '43',
            'Teruel': '44', 'Toledo': '45', 'Valencia/València': '46', 'Valladolid': '47',
            'Zamora': '49', 'Zaragoza': '50', 'Ceuta': '51', 'Melilla': '52'
        }

        print(f"✅ Procesando series...")

        for serie in series_list:
            nombre = serie.get('Nombre', '')
            
            # FILTRO EXACTO basado en tu ejemplo: "Albacete. Total. Total habitantes. Personas."
            # 1. Buscamos que sea "Total"
            # 2. Que no sea el "Total Nacional"
            # 3. Que NO contenga "Hombres" ni "Mujeres"
            if ". Total." in nombre and "Total Nacional" not in nombre and "Hombres" not in nombre and "Mujeres" not in nombre:
                
                # Extraemos el nombre de la provincia (lo que está antes del primer punto)
                region_name = nombre.split('.')[0].strip()
                
                # Buscamos su código en nuestro mapa
                region_code = mapa_codigos.get(region_name, "00")

                for punto in serie.get('Data', []):
                    data_list.append({
                        'region_code': region_code,
                        'region_name': region_name,
                        'year': punto.get('Anyo'),
                        'population': punto.get('Valor')
                    })

        if not data_list:
            print("⚠️ No se encontraron coincidencias. Revisa el formato de la API.")
            return pd.DataFrame()

        df = pd.DataFrame(data_list)
        df = df.drop_duplicates(subset=['region_name', 'year'])
        
        # Formatos finales
        df['population'] = pd.to_numeric(df['population'], errors='coerce')
        df['year'] = pd.to_numeric(df['year'], errors='coerce').astype('Int64')
        df = df.dropna(subset=['population', 'year'])
        df = df.sort_values(by=['year', 'region_code'], ascending=[False, True])

        return df

    except Exception as e:
        print(f"❌ Error: {e}")
        return pd.DataFrame()

def fetch_population_and_save(base_outdir="outputs/data", filename="demografia_poblacion_provincias.csv"):
    df = fetch_population_total_nuts3()
    if df.empty: return None
    return save_df_to_theme(df, theme="demografia", filename=filename, base_outdir=base_outdir)

if __name__ == "__main__":
    ruta = fetch_population_and_save(base_outdir="./test_output")
    if ruta:
        print(f"✅ ¡CONSEGUIDO! Datos de la API guardados.")
        print(pd.read_csv(ruta).head(10))