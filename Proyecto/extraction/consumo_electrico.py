import os
import requests
import pandas as pd
import io

def fetch_viviendas_uso_ine(base_outdir="data"):
    # Diccionario exacto Código: Nombre para las 52 provincias/ciudades autónomas
    # Si el código es de 2 dígitos y el nombre NO es este, se elimina (adiós CCAA).
    provincias_ine = {
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

    url_csv = "https://www.ine.es/jaxi/files/tpx/es/csv_bdsc/59531.csv"
    out_dir = os.path.join(base_outdir, "energia")
    os.makedirs(out_dir, exist_ok=True)
    out_path = os.path.join(out_dir, "consumo_electrico.csv")

    print(f" -> Iniciando filtrado estricto. Objetivo: 3.237 ubicaciones en formato filas...")

    try:
        r = requests.get(url_csv, headers={'User-Agent': 'Mozilla/5.0'})
        contenido = r.content.decode('utf-8-sig', errors='ignore')
        df = pd.read_csv(io.StringIO(contenido), sep=';')
        
        df.columns = [c.strip() for c in df.columns]
        col_entidad = "Comunidades Autónomas, Provincias y Municipios"

        # 1. Separar Código y Nombre
        split = df[col_entidad].str.extract(r'^(\d+)\s+(.*)$')
        df['Codigo'] = split[0]
        df['Nombre'] = split[1]

        # 2. FILTRO DE EXCLUSIÓN DE CCAA Y TOTAL NACIONAL
        def validar_ubicacion(row):
            cod = str(row['Codigo'])
            nom = str(row['Nombre']).strip()
            
            if len(cod) == 2:
                # Solo permitimos si el código y el nombre coinciden con la provincia real
                return provincias_ine.get(cod) == nom
            if len(cod) == 5:
                # Municipios se quedan todos (son los 3.185)
                return True
            return False

        df_final = df[df.apply(validar_ubicacion, axis=1)].copy()

        # 3. Limpieza de formato numérico
        df_final['Total'] = df_final['Total'].astype(str).str.replace('.', '', regex=False).str.replace(',', '.', regex=False)

        # 4. Organización final (Formato por FILAS como pediste)
        # Reordenamos columnas para que sea legible
        columnas = ['Codigo', 'Nombre', 'Consumo eléctrico', 'Total']
        df_export = df_final[columnas]

        df_export.to_csv(out_path, index=False, encoding='utf-8-sig', sep=';')
        
        # Conteo de validación
        ubicaciones_unicas = df_export[['Codigo', 'Nombre']].drop_duplicates()
        n_prov = len(ubicaciones_unicas[ubicaciones_unicas['Codigo'].str.len() == 2])
        n_muni = len(ubicaciones_unicas[ubicaciones_unicas['Codigo'].str.len() == 5])

        print(f"  ¡Filtrado completado con éxito!")
        print(f" 📊 Ubicaciones únicas: {len(ubicaciones_unicas)} ({n_prov} Provincias + {n_muni} Municipios)")
        print(f" 📂 Total filas en el CSV: {len(df_export)}")
        
        return out_path

    except Exception as e:
        print(f" ❌ Error: {e}")
        return None

if __name__ == "__main__":
    fetch_viviendas_uso_ine()