import os
import requests
import pandas as pd
import io

def fetch_viviendas_uso_ine(base_outdir="data"):
    url_csv = "https://www.ine.es/jaxi/files/tpx/es/csv_bdsc/59531.csv"
    out_dir = os.path.join(base_outdir, "energia")
    os.makedirs(out_dir, exist_ok=True)
    out_path = os.path.join(out_dir, "consumo_electrico.csv")

    # Diccionario de validación para asegurar que no bajamos CCAA o Totales
    provincias_dict = {
        "01": "Araba/Álava", "02": "Albacete", "03": "Alicante/Alacant", "04": "Almería",
        "05": "Ávila", "06": "Badajoz", "07": "Balears, Illes", "08": "Barcelona",
        "09": "Burgos", "10": "Cáceres", "11": "Cádiz", "12": "Castellón/Castelló",
        "13": "Ciudad Real", "14": "Córdoba", "15": "Coruña, A", "16": "Cuenca",
        "17": "Girona", "18": "Granada", "19": "Guadalajara", "20": "Gipuzkoa",
        "21": "Huelva", "22": "Huesca", "23": "Jaén", "24": "León", "25": "Lleida",
        "26": "Rioja, La", "27": "Lugo", "28": "Madrid", "29": "Málaga", "30": "Murcia",
        "31": "Navarra", "32": "Ourense", "33": "Asturias", "34": "Palencia",
        "35": "Palmas, Las", "36": "Pontevedra", "37": "Salamanca", "38": "Santa Cruz de Tenerife",
        "39": "Cantabria", "40": "Segovia", "41": "Sevilla", "42": "Soria", "43": "Tarragona",
        "44": "Teruel", "45": "Toledo", "46": "Valencia/València", "47": "Valladolid",
        "48": "Bizkaia", "49": "Zamora", "50": "Zaragoza", "51": "Ceuta", "52": "Melilla"
    }

    print(f" -> Iniciando filtrado estricto. Objetivo: 3.237 ubicaciones (Prov + Mun)...")

    r = requests.get(url_csv, headers={'User-Agent': 'Mozilla/5.0'})
    r.raise_for_status()
    
    contenido = r.content.decode('utf-8-sig', errors='ignore')
    df = pd.read_csv(io.StringIO(contenido), sep=';')
    df.columns = [c.strip() for c in df.columns]

    # 1. Determinar columna origen por prioridad (municipio > provincia)
    muni_s = df['Municipios'].fillna('').astype(str).str.strip()
    prov_s = df['Provincias'].fillna('').astype(str).str.strip()

    is_muni = muni_s.str.match(r'^\d{5}')
    is_prov = (~is_muni) & prov_s.str.match(r'^\d{2}\s')

    target = pd.Series('', index=df.index)
    target = target.where(~is_muni, muni_s)
    target = target.where(~is_prov, prov_s)

    df2 = df[is_muni | is_prov].copy()
    target2 = target[is_muni | is_prov]

    # 2. Extraer Código y Nombre con regex vectorizado
    extracted = target2.str.extract(r'^(\d+)\s+(.*)$')
    extracted.columns = ['Codigo', 'Nombre']
    extracted['Nombre'] = extracted['Nombre'].str.strip()

    # 3. Validación estricta (municipios: 5 dígitos; provincias: código en dict)
    valid_muni = extracted['Codigo'].str.len() == 5
    prov_mapped = extracted['Codigo'].map(provincias_dict)
    valid_prov  = (extracted['Codigo'].str.len() == 2) & (prov_mapped == extracted['Nombre'])
    valid = (valid_muni | valid_prov).values

    df3       = df2[valid].copy()
    ext3      = extracted[valid]

    if df3.empty:
        raise ValueError("No se pudieron extraer datos. Revisa el formato del CSV del INE.")

    df3['Codigo'] = ext3['Codigo'].values
    df3['Nombre'] = ext3['Nombre'].values
    df3['Total']  = (df3['Total'].fillna('0').astype(str)
                     .str.replace('.', '', regex=False)
                     .str.replace(',', '.', regex=False))

    df_final = df3[['Codigo', 'Nombre', 'Consumo eléctrico', 'Total']].copy()
    # Eliminar duplicados técnicos por el cruce de tablas del INE
    df_final = df_final.drop_duplicates(subset=['Codigo', 'Consumo eléctrico'])

    df_final.to_csv(out_path, index=False, encoding='utf-8-sig', sep=';')
    
    # Logs de control
    c_prov = len(df_final[df_final['Codigo'].str.len() == 2]['Codigo'].unique())
    c_muni = len(df_final[df_final['Codigo'].str.len() == 5]['Codigo'].unique())
    
    print(f"  ✅ ¡Filtrado completado!")
    print(f"  📊 GeoLúmica Data: {c_prov} Provincias y {c_muni} Municipios guardados.")
    
    return out_path

if __name__ == "__main__":
    fetch_viviendas_uso_ine()