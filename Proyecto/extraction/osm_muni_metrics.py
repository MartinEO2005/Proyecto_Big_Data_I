#!/usr/bin/env python3
# osm_muni_metrics.py
"""
Genera métricas municipales de conectividad ferroviaria a partir de estaciones OSM
y un fichero de municipios (GeoJSON / GPKG / Shapefile o CSV con WKT).
Versión con función compute_metrics_and_export memoria-amigable (iterativa).
"""

import math
import time
from datetime import datetime, timezone
from pathlib import Path

import requests
import pandas as pd
import geopandas as gpd
import numpy as np
from shapely.geometry import Point
from tqdm import tqdm

OVERPASS_URL = "https://overpass-api.de/api/interpreter"
REQUEST_TIMEOUT = 180

# ----------------- CONFIG -----------------
_script_dir = Path(__file__).resolve().parent
MUNIS_PATH = str((_script_dir / ".." / "municipios_es.geojson").resolve())
OUT_PREFIX = str((_script_dir / ".." / "data" / "raw" / "transporte" / "muni_station_metrics_reduced").resolve())
MAX_TILE_AREA_DEG2 = 1.0               # tamaño máximo de cada tile en grados^2
# ------------------------------------------

def read_munis(path):
    gdf = gpd.read_file(path)
    if "SOG_ID" not in gdf.columns:
        if "GISCO_ID" in gdf.columns:
            gdf["SOG_ID"] = gdf["GISCO_ID"]
        elif "LAU_ID" in gdf.columns:
            gdf["SOG_ID"] = gdf["LAU_ID"].astype(str)
        else:
            gdf["SOG_ID"] = gdf.index.astype(str)
    if "AREA_KM2" not in gdf.columns or gdf["AREA_KM2"].isnull().any():
        gdf = gdf.to_crs(epsg=3857)
        gdf["AREA_KM2"] = gdf.geometry.area / 1e6
        gdf = gdf.to_crs(epsg=4326)
    gdf = gdf.to_crs(epsg=4326)
    return gdf

def geom_bounds_to_tiles(total_bounds, max_area_deg2=1.0):
    minx, miny, maxx, maxy = total_bounds
    width = maxx - minx
    height = maxy - miny
    area = width * height
    if area <= max_area_deg2 or width == 0 or height == 0:
        return [(minx, miny, maxx, maxy)]
    n_tiles = max(1, math.ceil(area / max_area_deg2))
    nx = math.ceil(math.sqrt(n_tiles * (width / height))) if height > 0 else n_tiles
    ny = math.ceil(n_tiles / nx)
    xs = [minx + i*(width/nx) for i in range(nx+1)]
    ys = [miny + j*(height/ny) for j in range(ny+1)]
    tiles = []
    for i in range(nx):
        for j in range(ny):
            tiles.append((xs[i], ys[j], xs[i+1], ys[j+1]))
    return tiles

def overpass_query_bbox(s, w, n, e):
    query = f"""
    [out:json][timeout:160];
    (
      node["railway"~"station|halt"]({s},{w},{n},{e});
      node["public_transport"~"station|platform|stop_position"]({s},{w},{n},{e});
      way["railway"~"station|halt"]({s},{w},{n},{e});
      relation["railway"~"station|halt"]({s},{w},{n},{e});
      relation["public_transport"="station"]({s},{w},{n},{e});
    );
    out center tags meta;
    """
    try:
        r = requests.post(OVERPASS_URL, data={"data": query}, timeout=REQUEST_TIMEOUT)
        r.raise_for_status()
        return r.json().get("elements", [])
    except requests.exceptions.RequestException:
        time.sleep(2)
        try:
            r = requests.post(OVERPASS_URL, data={"data": query}, timeout=REQUEST_TIMEOUT)
            r.raise_for_status()
            return r.json().get("elements", [])
        except Exception:
            return []

def elements_to_df(elements):
    rows = []
    for el in elements:
        el_type = el.get("type")
        el_id = el.get("id")
        tags = el.get("tags") or {}
        lat = el.get("lat") or (el.get("center") and el["center"].get("lat"))
        lon = el.get("lon") or (el.get("center") and el["center"].get("lon"))
        if lat is None or lon is None:
            continue
        rows.append({
            "osm_id": f"{el_type}/{el_id}",
            "source_type": el_type,
            "name": tags.get("name"),
            "lat": float(lat),
            "lon": float(lon),
            "tags": tags
        })
    df = pd.DataFrame(rows)
    if not df.empty:
        df = df.drop_duplicates(subset=["lat","lon","name"], keep="first").reset_index(drop=True)
    return df

def fetch_all_stations(gdf_munis, max_tile_area_deg2=1.0):
    minx, miny, maxx, maxy = gdf_munis.total_bounds
    tiles = geom_bounds_to_tiles((minx, miny, maxx, maxy), max_area_deg2=max_tile_area_deg2)
    all_elements = []
    seen = set()
    for (x1, y1, x2, y2) in tqdm(tiles, desc="Tiles Overpass", unit="tile"):
        s, w, n, e = y1, x1, y2, x2
        elems = overpass_query_bbox(s, w, n, e)
        for el in elems:
            key = (el.get("type"), el.get("id"))
            if key in seen:
                continue
            seen.add(key)
            all_elements.append(el)
        time.sleep(1.0)
    return elements_to_df(all_elements)

def df_to_gdf(df):
    if df is None or df.empty:
        return gpd.GeoDataFrame(columns=["osm_id","source_type","name","lat","lon","tags","geometry"], geometry="geometry", crs="EPSG:4326")
    gdf = gpd.GeoDataFrame(df.copy(), geometry=[Point(xy) for xy in zip(df.lon, df.lat)], crs="EPSG:4326")
    gdf = gdf.to_crs(epsg=3857)
    return gdf

def extract_tag_col(gdf, key):
    return gdf["tags"].apply(lambda t: (t.get(key) if isinstance(t, dict) else None))

def drop_empty_and_constant_columns(gdf):
    cols_to_drop = []
    geom_name = gdf.geometry.name
    for c in list(gdf.columns):
        if c == geom_name:
            continue
        ser = gdf[c]
        non_na = ser.dropna()
        if non_na.empty:
            cols_to_drop.append(c)
            continue
        unique_vals = non_na.unique()
        if len(unique_vals) == 1:
            val = unique_vals[0]
            if (isinstance(val, (int, float)) and float(val) == 0.0) or (isinstance(val, str) and val.strip() == ""):
                cols_to_drop.append(c)
    if cols_to_drop:
        gdf = gdf.drop(columns=cols_to_drop)
    return gdf, cols_to_drop

def categorize_connectivity(row):
    if row.get("stations_count", 0) > 0:
        return "directo"
    if (row.get("stations_within_1km_count", 0) > 0) or (row.get("stations_in_muni_plus_1km_count", 0) > 0) or (row.get("min_distance_km_to_station") is not None and row.get("min_distance_km_to_station") < 1.0):
        return "funcional"
    if ((row.get("stations_in_muni_plus_5km_count", 0) == 0) and (row.get("stations_within_5km_count", 0) == 0) and (row.get("min_distance_km_to_station") is not None and row.get("min_distance_km_to_station") >= 5.0)):
        return "aislado"
    return "periferico"

# Deterministic mean distance: centroid + buffer
def mean_dist_for_muni_centroid(row, stations_gdf, search_radius_m=20000):
    poly = row.geometry
    if poly is None or poly.is_empty:
        return None
    centroid = poly.centroid
    if centroid is None or centroid.is_empty:
        return None
    buf = centroid.buffer(search_radius_m)
    try:
        idx_candidates = list(stations_gdf.sindex.intersection(buf.bounds))
    except Exception:
        idx_candidates = []
    if not idx_candidates:
        return None
    candidates = stations_gdf.iloc[idx_candidates]
    candidates = candidates[candidates.geometry.within(buf)]
    if candidates.empty:
        return None
    dists_m = candidates.geometry.distance(centroid)
    mean_km = float(dists_m.mean()) / 1000.0
    return mean_km

# -------------------------
# Memoria-amigable: iterativa, usa sindex sobre estaciones
# -------------------------
def compute_metrics_and_export(gdf_st, gdf_munis, out_prefix=OUT_PREFIX, save_outputs=True, batch_size=500):
    """
    Versión robusta y iterativa:
    - gdf_st: GeoDataFrame de estaciones (puede estar en EPSG:4326; será transformado internamente).
    - gdf_munis: GeoDataFrame de municipios (EPSG:4326; transformado internamente).
    - out_prefix: prefijo de salida para CSV/GPKG.
    - save_outputs: si True escribe CSV/GPKG en cwd.
    - batch_size: número de municipios a procesar por chunk (control de memoria).
    Devuelve GeoDataFrame (EPSG:3857) con métricas.
    """
    import gc
    # preparar CRS en metros
    gdf_munis = gdf_munis.to_crs(epsg=3857)
    gdf_st = gdf_st.to_crs(epsg=3857)

    # extraer columnas de tags en estaciones
    if "tags" in gdf_st.columns:
        gdf_st["wheelchair"] = extract_tag_col(gdf_st, "wheelchair")
        gdf_st["operator"] = extract_tag_col(gdf_st, "operator")
    else:
        gdf_st["wheelchair"] = None
        gdf_st["operator"] = None
    gdf_st["wheelchair_yes"] = gdf_st["wheelchair"].fillna("").astype(str).str.lower().eq("yes")
    gdf_st["has_operator"] = gdf_st["operator"].notna()

    stations = gdf_st.reset_index(drop=True)[["osm_id", "geometry", "wheelchair_yes", "has_operator", "name", "tags"]].copy()
    try:
        sindex = stations.sindex
    except Exception:
        # forzar creación si no existe
        _ = stations.geometry
        sindex = stations.sindex

    out_rows = []
    n = len(gdf_munis)
    ranges = range(0, n, batch_size)

    def _count_in_geom(poly):
        if poly is None or poly.is_empty:
            return 0
        cand_idx = list(sindex.intersection(poly.bounds))
        if not cand_idx:
            return 0
        cand = stations.iloc[cand_idx]
        return int(cand[cand.geometry.within(poly)].shape[0])

    def _count_unique_in_geom(poly):
        if poly is None or poly.is_empty:
            return 0
        cand_idx = list(sindex.intersection(poly.bounds))
        if not cand_idx:
            return 0
        cand = stations.iloc[cand_idx]
        unique = cand[cand.geometry.within(poly)]["osm_id"].nunique()
        return int(unique)

    def _min_dist_km_from_centroid(centroid):
        if centroid is None or centroid.is_empty:
            return None
        # buscar candidatos en un radio razonable en bounds antes de distance
        cand_idx = list(sindex.intersection(centroid.buffer(200000).bounds))  # 200 km buffer bounds
        if not cand_idx:
            return None
        cand = stations.iloc[cand_idx]
        if cand.empty:
            return None
        dists = cand.geometry.distance(centroid)
        if dists.empty:
            return None
        return float(dists.min()) / 1000.0

    def _mean_dist_km_centroid_within(centroid, radius_m=20000):
        if centroid is None or centroid.is_empty:
            return None
        buf = centroid.buffer(radius_m)
        cand_idx = list(sindex.intersection(buf.bounds))
        if not cand_idx:
            return None
        cand = stations.iloc[cand_idx]
        inside = cand[cand.geometry.within(buf)]
        if inside.empty:
            return None
        dists = inside.geometry.distance(centroid)
        return float(dists.mean()) / 1000.0

    for start in ranges:
        end = min(start + batch_size, n)
        batch = gdf_munis.iloc[start:end]
        for idx, muni in batch.iterrows():
            sog = muni.get("SOG_ID", None)
            lau = muni.get("LAU_ID", None) if "LAU_ID" in muni.index else None
            name = muni.get("LAU_NAME", None) if "LAU_NAME" in muni.index else None
            area_km2 = muni.get("AREA_KM2", None)
            pop2023 = muni.get("POP_2023", None) if "POP_2023" in muni.index else None
            poly = muni.geometry

            stations_count = _count_in_geom(poly)
            stations_unique = _count_unique_in_geom(poly)
            stations_density_km2 = (stations_count / float(area_km2)) if (area_km2 is not None and area_km2 > 0) else 0.0

            centroid = poly.centroid if (poly is not None and not poly.is_empty) else None
            stations_within_1km_count = 0
            stations_within_5km_count = 0
            min_distance_km_to_station = None
            mean_distance_km_to_station = None
            if centroid is not None and not centroid.is_empty:
                stations_within_1km_count = _count_in_geom(centroid.buffer(1000))
                stations_within_5km_count = _count_in_geom(centroid.buffer(5000))
                min_distance_km_to_station = _min_dist_km_from_centroid(centroid)
                mean_distance_km_to_station = _mean_dist_km_centroid_within(centroid, radius_m=20000)

            stations_in_muni_plus_1km_count = _count_in_geom(poly.buffer(1000))
            stations_in_muni_plus_5km_count = _count_in_geom(poly.buffer(5000))

            accessible_count = 0
            operator_count = 0
            if poly is not None and not poly.is_empty:
                cand_idx = list(sindex.intersection(poly.bounds))
                if cand_idx:
                    cand = stations.iloc[cand_idx]
                    inside = cand[cand.geometry.within(poly)]
                    if not inside.empty:
                        accessible_count = int(inside["wheelchair_yes"].sum())
                        operator_count = int(inside["has_operator"].sum())

            accessible_share = (accessible_count / stations_count) if stations_count > 0 else 0.0
            stations_with_operator_share = (operator_count / stations_count) if stations_count > 0 else 0.0
            stations_per_10k_pop = (stations_count / (pop2023/10000.0)) if (pop2023 is not None and pop2023 > 0) else None

            # categoría
            if stations_count > 0:
                cat = "directo"
            elif (stations_within_1km_count > 0) or (stations_in_muni_plus_1km_count > 0) or (min_distance_km_to_station is not None and min_distance_km_to_station < 1.0):
                cat = "funcional"
            elif ((stations_in_muni_plus_5km_count == 0) and (stations_within_5km_count == 0) and (min_distance_km_to_station is not None and min_distance_km_to_station >= 5.0)):
                cat = "aislado"
            else:
                cat = "periferico"

            out_rows.append({
                "SOG_ID": sog,
                "LAU_ID": lau,
                "LAU_NAME": name,
                "AREA_KM2": area_km2,
                "POP_2023": pop2023,
                "stations_count": int(stations_count),
                "stations_unique": int(stations_unique),
                "stations_density_km2": stations_density_km2,
                "stations_with_operator_share": stations_with_operator_share,
                "operator_count": int(operator_count),
                "stations_per_10k_pop": stations_per_10k_pop,
                "stations_within_1km_count": int(stations_within_1km_count),
                "stations_within_5km_count": int(stations_within_5km_count),
                "stations_in_muni_plus_1km_count": int(stations_in_muni_plus_1km_count),
                "stations_in_muni_plus_5km_count": int(stations_in_muni_plus_5km_count),
                "min_distance_km_to_station": min_distance_km_to_station,
                "mean_distance_km_to_station": mean_distance_km_to_station,
                "accessible_count": int(accessible_count),
                "accessible_share": accessible_share,
                "category_connectivity": cat,
                "geometry": poly
            })
        gc.collect()

    gdf_out = gpd.GeoDataFrame(out_rows, geometry="geometry", crs="EPSG:3857")
    try:
        export_gdf = gdf_out.to_crs(epsg=4326)
    except Exception:
        export_gdf = gdf_out.copy()

    if save_outputs:
        try:
            gpkg = f"{out_prefix}.gpkg"
            csvf = f"{out_prefix}.csv"
            export_gdf.to_file(gpkg, layer="municipal_metrics", driver="GPKG")
            export_gdf.drop(columns=[export_gdf.geometry.name], errors="ignore").to_csv(csvf, index=False)
        except Exception as e:
            print("⚠️ Error guardando outputs en compute_metrics_and_export:", type(e), e)

    return gdf_out

def run_osm_full(munis_path=MUNIS_PATH, out_prefix=OUT_PREFIX, max_tile_area_deg2=MAX_TILE_AREA_DEG2):
    """
    Ejecuta el pipeline OSM completo (leer municipios, tilear Overpass, crear estaciones, calcular métricas).
    Devuelve diccionario con rutas/GeoDataFrame.
    """
    result = {"rail_csv": None, "metrics_csv": None, "metrics_gpkg": None, "gdf_out": None}
    gdf_munis = read_munis(munis_path)
    print("Municipios leídos:", len(gdf_munis))
    df_st = fetch_all_stations(gdf_munis, max_tile_area_deg2=max_tile_area_deg2)
    print("Estaciones OSM obtenidas:", len(df_st))
    try:
        rail_csv = Path("rail_stations.csv")
        df_st.to_csv(str(rail_csv), index=False)
        result["rail_csv"] = str(rail_csv)
        print("Rail stations guardado en:", rail_csv)
    except Exception as e:
        print("⚠️ No pude guardar rail_stations.csv:", type(e), e)

    gdf_st = df_to_gdf(df_st)
    if gdf_st.empty:
        gdf_st = gpd.GeoDataFrame(columns=["osm_id","source_type","name","lat","lon","tags","geometry"], geometry="geometry", crs="EPSG:4326").to_crs(epsg=3857)
    gdf_out = compute_metrics_and_export(gdf_st, gdf_munis, out_prefix=out_prefix, save_outputs=True)
    result["gdf_out"] = gdf_out
    csvf = Path(f"{out_prefix}.csv")
    gpkg = Path(f"{out_prefix}.gpkg")
    if csvf.exists():
        result["metrics_csv"] = str(csvf)
    if gpkg.exists():
        result["metrics_gpkg"] = str(gpkg)
    return result

def main():
    res = run_osm_full()
    print("Resultado OSM:", {k:v for k,v in res.items() if v})
    if res.get("metrics_csv"):
        try:
            dfm = pd.read_csv(res["metrics_csv"])
            print("Métricas (csv) filas:", len(dfm))
        except Exception:
            pass


if __name__ == "__main__":
    main()
