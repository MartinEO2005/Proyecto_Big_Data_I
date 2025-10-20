from datetime import date, timedelta
import requests
import pandas as pd
import geopandas as gpd
from shapely.geometry import shape
import os


copernicus_user = "martinotero2005@hotmail.com" # copernicus User
copernicus_password = "Copernicus+2005" # copernicus Password
ft = "POLYGON((-4.5 40.0, -4.5 40.1, -4.4 40.1, -4.4 40.0, -4.5 40.0))"  # WKT Representation of BBOX
data_collection = "SENTINEL-2" # Sentinel satellite

today =  date.today()
today_string = today.strftime("%Y-%m-%d")
yesterday = today - timedelta(days=1)
yesterday_string = yesterday.strftime("%Y-%m-%d")


def get_keycloak(username: str, password: str) -> str:
    data = {
        "client_id": "cdse-public",
        "username": username,
        "password": password,
        "grant_type": "password",
    }
    try:
        r = requests.post(
            "https://identity.dataspace.copernicus.eu/auth/realms/CDSE/protocol/openid-connect/token",
            data=data,
        )
        r.raise_for_status()
    except Exception as e:
        raise Exception(
            f"Keycloak token creation failed. Reponse from the server was: {r.json()}"
        )
    return r.json()["access_token"]


json_ = requests.get(
    f"https://catalogue.dataspace.copernicus.eu/odata/v1/Products?$filter=Collection/Name eq '{data_collection}' and OData.CSC.Intersects(area=geography'SRID=4326;{ft}') and ContentDate/Start gt {yesterday_string}T00:00:00.000Z and ContentDate/Start lt {today_string}T00:00:00.000Z&$count=True&$top=1000"
).json()  
p = pd.DataFrame.from_dict(json_["value"]) # Fetch available dataset
if p.shape[0] > 0 :
    p["geometry"] = p["GeoFootprint"].apply(shape)
    productDF = gpd.GeoDataFrame(p).set_geometry("geometry") # Convert PD to GPD
    productDF = productDF[~productDF["Name"].str.contains("L1C")] # Remove L1C dataset
    print(f" total L2A tiles found {len(productDF)}")
    productDF["identifier"] = productDF["Name"].str.split(".").str[0]
    allfeat = len(productDF) 

    if allfeat == 0:
        print("No tiles found for today")
    else:
        ## download all tiles from server
        for index, feat in enumerate(productDF.iterfeatures()):
            try:
                print(f"\n🔹 Descargando producto {index+1}/{len(productDF)}")

                # 1️⃣ Obtener token Keycloak (nuevo por cada producto)
                keycloak_token = get_keycloak(copernicus_user, copernicus_password)
                session = requests.Session()

                product_id = feat["properties"]["Id"]
                identifier = feat["properties"]["identifier"]

                # 2️⃣ Solicitar URL de descarga
                base_url = f"https://catalogue.dataspace.copernicus.eu/odata/v1/Products({product_id})/$value"
                response = session.get(base_url, allow_redirects=False, headers={"Authorization": f"Bearer {keycloak_token}"})
                redirect_url = response.headers.get("Location")

                if not redirect_url:
                    print(f"⚠️ No se encontró URL de descarga para {identifier}")
                    continue

                # 3️⃣ Añadir token en la URL final (garantiza autenticación)
                final_url = f"{redirect_url}?token={keycloak_token}"
                print(f"⬇️  Descargando desde: {final_url}")

                # 4️⃣ Descargar archivo con stream y timeout largo
                with session.get(final_url, stream=True, timeout=1800) as file:
                    file.raise_for_status()
                    total = 0
                    with open(f"{identifier}.zip", "wb") as out:
                        for chunk in file.iter_content(chunk_size=1024*1024):  # 1 MB
                            if chunk:
                                out.write(chunk)
                                total += len(chunk)
                                if total % (50*1024*1024) < 1024*1024:
                                    print(f"  ...{total/1024/1024:.0f} MB descargados")

                print(f"✅ Archivo guardado: {identifier}.zip")

            except requests.exceptions.HTTPError as http_err:
                print(f"❌ Error HTTP al descargar {identifier}: {http_err}")
                print("   ➤ Intenta regenerar el token o volver a ejecutar el script.")
            except Exception as e:
                print(f"❌ Error general al descargar {identifier}: {e}")
else :
    print('no data found')