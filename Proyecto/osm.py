# osm.py
import requests
import pandas as pd
from shapely import wkt
import time

OVERPASS_URL = "https://overpass-api.de/api/interpreter"

def bbox_from_wkt(aoi_wkt):
    """
    Convierte un WKT POLYGON en bounding box (south, west, north, east)
    para consultas a Overpass API.
    """
    geom = wkt.loads(aoi_wkt)
    if geom is None:
        raise ValueError(f"AOI_WKT inválido: {aoi_wkt}")
    minx, miny, maxx, maxy = geom.bounds
    return miny, minx, maxy, maxx

def fetch_rail_stations(aoi_wkt, retries=3):
    s, w, n, e = bbox_from_wkt(aoi_wkt)
    query = f"""
    [out:json][timeout:120];
    node["railway"="station"]({s},{w},{n},{e});
    out body;
    """
    attempt = 0
    while attempt < retries:
        try:
            r = requests.post(OVERPASS_URL, data={"data": query}, timeout=180)
            r.raise_for_status()
            data = r.json()
            elements = data.get("elements", [])
            if not elements:
                print("⚠️ No se encontraron estaciones ferroviarias en el AOI definido.")
                return pd.DataFrame()
            rows = []
            for el in elements:
                tags = el.get("tags", {})
                lat = el.get("lat")
                lon = el.get("lon")
                rows.append({
                    "osm_id": el.get("id"),
                    "name": tags.get("name"),
                    "lat": lat,
                    "lon": lon,
                    "tags": tags
                })
            print(f"✅ Se encontraron {len(rows)} estaciones ferroviarias.")
            return pd.DataFrame(rows)
        except requests.exceptions.RequestException as e:
            print(f"❌ Error al consultar Overpass API (intento {attempt+1}):", e)
            attempt += 1
            time.sleep(5)
    print("⚠️ No se pudo completar la consulta a Overpass API después de varios intentos.")
    return pd.DataFrame()
