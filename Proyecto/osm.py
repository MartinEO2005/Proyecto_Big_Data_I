# osm.py
import requests
import pandas as pd
from shapely import wkt
import json

OVERPASS_URL = "https://overpass-api.de/api/interpreter"

def bbox_from_wkt(aoi_wkt):
    geom = wkt.loads(aoi_wkt)
    minx, miny, maxx, maxy = geom.bounds
    return miny, minx, maxy, maxx

def fetch_rail_stations(aoi_wkt):
    s, w, n, e = bbox_from_wkt(aoi_wkt)
    query = f"""
    [out:json][timeout:60];
    (
      node["railway"="station"]({s},{w},{n},{e});
      way["railway"="station"]({s},{w},{n},{e});
      relation["railway"="station"]({s},{w},{n},{e});
    );
    out center tags;
    """
    r = requests.post(OVERPASS_URL, data={"data": query}, timeout=120)
    r.raise_for_status()
    js = r.json()
    rows = []
    for el in js.get("elements", []):
        tags = el.get("tags", {})
        lat = el.get("lat") or (el.get("center") or {}).get("lat")
        lon = el.get("lon") or (el.get("center") or {}).get("lon")
        rows.append({
            "osm_id": el.get("id"),
            "name": tags.get("name"),
            "lat": lat,
            "lon": lon,
            "tags": json.dumps(tags, ensure_ascii=False)
        })
    return pd.DataFrame(rows)
