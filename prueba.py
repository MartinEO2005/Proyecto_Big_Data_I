import requests
import pandas as pd

# 1. Variables según la documentación que encontraste
# La URL base correcta es catalogodatos.cnmc.es
base_url = "https://catalogodatos.cnmc.es"
# El identificador que copiaste de la web
id_recurso = "0a072691-9d60-41b7-a303-46d36882285c"

header = {"User-Agent": "Application"}

print(f"Conectando a la API de la CNMC...")

try:
    # 2. Consulta directa para obtener los datos del recurso (datastore_search)
    # Usamos la URL exacta de tu ejemplo: api/3/action/datastore_search?limit=32000&resource_id=...
    url_datos = f"{base_url}/api/3/action/datastore_search?limit=32000&resource_id={id_recurso}"
    
    print(f"Solicitando datos al recurso: {id_recurso}")
    r = requests.get(url_datos, headers=header)
    r.raise_for_status() # Verifica que la conexión sea exitosa
    
    # 3. Los datos se ubican en la colección 'records' según tu texto
    datos_json = r.json()
    datos_dataset = datos_json["result"]["records"]
    
    print(f"Éxito: Se han recuperado {len(datos_dataset)} registros.")

    # 4. Convertir a DataFrame y guardar como CSV
    df = pd.DataFrame(datos_dataset)
    
    # Guardamos el archivo
    nombre_archivo = "consumo_provincias_cnmc.csv"
    df.to_csv(nombre_archivo, index=False, encoding='utf-8-sig')
    
    print("-" * 40)
    print(f"ARCHIVO GENERADO: {nombre_archivo}")
    print("Primeras 5 filas del archivo:")
    print(df.head())
    print("-" * 40)

except Exception as e:
    print(f"Error durante la descarga: {e}")