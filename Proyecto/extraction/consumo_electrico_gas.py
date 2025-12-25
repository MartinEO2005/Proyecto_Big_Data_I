# consumo_electrico_gas.py
import os
import requests
import pandas as pd

def fetch_energy_consumption_and_save(base_outdir="data"):
    """
    Descarga datos de consumo de gas por provincias desde la API de la CNMC.
    """
    base_url = "https://catalogodatos.cnmc.es"
    id_recurso = "0a072691-9d60-41b7-a303-46d36882285c"
    header = {"User-Agent": "Application"}
    
    # Definir ruta de salida
    out_dir = os.path.join(base_outdir, "energia")
    os.makedirs(out_dir, exist_ok=True)
    out_path = os.path.join(out_dir, "consumo_provincias_cnmc.csv")

    print(f"  -> Conectando a la API de la CNMC (Recurso: {id_recurso})...")

    try:
        url_datos = f"{base_url}/api/3/action/datastore_search?limit=32000&resource_id={id_recurso}"
        r = requests.get(url_datos, headers=header)
        r.raise_for_status()
        
        datos_json = r.json()
        datos_dataset = datos_json["result"]["records"]
        
        if not datos_dataset:
            print("  ⚠️ La API de la CNMC no devolvió registros.")
            return None

        df = pd.DataFrame(datos_dataset)
        
        # Renombrar columna según tu solicitud
        if "consumo_ventas_de_gas_natural" in df.columns:
            df.rename(columns={"consumo_ventas_de_gas_natural": "Consumo (MWh)"}, inplace=True)
        
        # Guardar
        df.to_csv(out_path, index=False, encoding='utf-8-sig')
        return out_path

    except Exception as e:
        print(f"  ❌ Error en módulo energia_cnmc: {e}")
        return None