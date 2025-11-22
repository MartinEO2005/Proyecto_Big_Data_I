#!/usr/bin/env python3
# osm_muni_metrics_final_centroid.py
"""
Genera métricas municipales de conectividad ferroviaria a partir de estaciones OSM
y un fichero de municipios (GeoJSON / GPKG / Shapefile o CSV con WKT).
Versión que calcula mean_distance_km_to_station de forma determinista usando centroid + buffer.
"""

import math
import time
from datetime import datetime, timezone

import requests
import pandas as pd
import geopandas as gpd
import numpy as np
from shapely.geometry import Point
from tqdm import tqdm

OVERPASS_URL = "https://overpass-api.de/api/interpreter"
REQUEST_TIMEOUT = 180

# ----------------- CONFIG -----------------
MUNIS_PATH = "municipios_es.geojson"   # ruta a tu geojson de municipios
OUT_PREFIX = "muni_station_metrics_reduced"
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
    if df.empty:
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

def compute_metrics_and_export(gdf_st, gdf_munis, out_prefix=OUT_PREFIX):
    gdf_munis = gdf_munis.to_crs(epsg=3857)
    gdf_st = gdf_st.to_crs(epsg=3857)

    gdf_st["wheelchair"] = extract_tag_col(gdf_st, "wheelchair")
    gdf_st["platforms"] = extract_tag_col(gdf_st, "platforms")
    gdf_st["operator"] = extract_tag_col(gdf_st, "operator")
    gdf_st["name"] = gdf_st["name"].fillna("")

    st_with_muni = gpd.sjoin(gdf_st, gdf_munis[["SOG_ID","LAU_ID","LAU_NAME","AREA_KM2","POP_2023","geometry"]], how="left", predicate="within")

    agg_basic = st_with_muni.groupby("SOG_ID").agg(
        stations_count = ("osm_id","count"),
        stations_unique = ("osm_id","nunique")
    ).reset_index()

    st_with_muni["wheelchair_yes"] = st_with_muni["wheelchair"].fillna("").str.lower().eq("yes")
    st_with_muni["has_operator"] = st_with_muni["operator"].notna()
    st_with_muni["has_platforms"] = st_with_muni["platforms"].notna()

    tag_agg = st_with_muni.groupby("SOG_ID").agg(
        accessible_count = ("wheelchair_yes","sum"),
        operator_count = ("has_operator","sum"),
        has_platforms_count = ("has_platforms","sum")
    ).reset_index()

    gdf_munis["centroid_geom"] = gdf_munis.geometry.centroid
    centroids = gdf_munis.set_geometry("centroid_geom")
    st_pts = gdf_st[["osm_id","geometry","name"]]

    nearest = gpd.sjoin_nearest(centroids[["SOG_ID","centroid_geom"]].set_geometry("centroid_geom"),
                                st_pts.set_geometry("geometry"),
                                how="left", distance_col="dist_m")
    nearest = nearest[["SOG_ID","osm_id","dist_m","name"]].rename(columns={"osm_id":"nearest_station_osm_id","name":"nearest_station_name"})
    nearest["min_distance_km_to_station"] = nearest["dist_m"] / 1000.0

    stations_geom = gdf_st.reset_index(drop=True)[["osm_id","geometry"]]
    try:
        _ = stations_geom.sindex
    except Exception:
        pass

    gdf_out = gdf_munis.merge(agg_basic, on="SOG_ID", how="left")
    gdf_out = gdf_out.merge(tag_agg, on="SOG_ID", how="left")
    gdf_out = gdf_out.merge(nearest[["SOG_ID","nearest_station_osm_id","nearest_station_name","min_distance_km_to_station"]], on="SOG_ID", how="left")

    gdf_out["stations_count"] = gdf_out["stations_count"].fillna(0).astype(int)
    gdf_out["stations_unique"] = gdf_out["stations_unique"].fillna(0).astype(int)
    gdf_out["accessible_count"] = gdf_out["accessible_count"].fillna(0).astype(int)
    gdf_out["operator_count"] = gdf_out["operator_count"].fillna(0).astype(int)
    gdf_out["has_platforms_count"] = gdf_out["has_platforms_count"].fillna(0).astype(int)
    gdf_out["stations_density_km2"] = gdf_out["stations_count"] / gdf_out["AREA_KM2"].astype(float)

    gdf_out["stations_with_operator_share"] = gdf_out.apply(lambda r: (r["operator_count"] / r["stations_count"]) if r["stations_count"]>0 else 0.0, axis=1)

    if "POP_2023" in gdf_out.columns:
        gdf_out["stations_per_10k_pop"] = gdf_out.apply(lambda r: (r["stations_count"]/(r["POP_2023"]/10000.0)) if (pd.notna(r.get("POP_2023")) and r["POP_2023"]>0) else None, axis=1)
    else:
        gdf_out["stations_per_10k_pop"] = None

    def count_within_radius_centroid(row, radius_m):
        c = row.geometry.centroid
        if c is None or c.is_empty:
            return 0
        buf = c.buffer(radius_m)
        possible_idx = list(stations_geom.sindex.intersection(buf.bounds))
        if not possible_idx:
            return 0
        possible = stations_geom.iloc[possible_idx]
        count = int(possible[possible.geometry.within(buf)].shape[0])
        return count

    gdf_out["stations_within_1km_count"] = gdf_out.apply(lambda r: count_within_radius_centroid(r, 1000), axis=1)
    gdf_out["stations_within_5km_count"] = gdf_out.apply(lambda r: count_within_radius_centroid(r, 5000), axis=1)

    def count_within_muni_buffer(row, stations_gdf, radius_m):
        poly = row.geometry
        if poly is None or poly.is_empty:
            return 0
        buf = poly.buffer(radius_m)
        possible_idx = list(stations_gdf.sindex.intersection(buf.bounds))
        if not possible_idx:
            return 0
        possible = stations_gdf.iloc[possible_idx]
        count = int(possible[possible.geometry.within(buf)].shape[0])
        return count

    gdf_out["stations_in_muni_plus_1km_count"] = gdf_out.apply(lambda r: count_within_muni_buffer(r, stations_geom, 1000), axis=1)
    gdf_out["stations_in_muni_plus_5km_count"] = gdf_out.apply(lambda r: count_within_muni_buffer(r, stations_geom, 5000), axis=1)

    # mean distance usando centroid + buffer determinista
    gdf_out["mean_distance_km_to_station"] = gdf_out.apply(
        lambda r: mean_dist_for_muni_centroid(r, stations_geom, search_radius_m=20000)
                  if (r["stations_count"]>0 or (pd.notna(r.get("min_distance_km_to_station")) and r.get("min_distance_km_to_station")<200)) else None,
        axis=1
    )

    st_pts_wgs = gdf_st.to_crs(epsg=4326).set_index("osm_id")
    def get_nearest_coords(osm_id):
        try:
            geom = st_pts_wgs.loc[osm_id].geometry
            if hasattr(geom, "iloc"):
                geom = geom.iloc[0]
            return geom.x, geom.y
        except Exception:
            return (None, None)
    gdf_out["nearest_station_lon"], gdf_out["nearest_station_lat"] = zip(*gdf_out["nearest_station_osm_id"].apply(lambda x: get_nearest_coords(x) if pd.notna(x) else (None,None)))

    gdf_out["accessible_share"] = gdf_out.apply(lambda r: (r["accessible_count"] / r["stations_count"]) if r["stations_count"]>0 else 0.0, axis=1)

    explicit_drops = [c for c in ["nearest_station_osm_id","nearest_station_name","nearest_station_lat","nearest_station_lon","stations_source_count","query_timestamp","has_platforms_count"] if c in gdf_out.columns]
    if explicit_drops:
        gdf_out = gdf_out.drop(columns=explicit_drops)

    gdf_out, auto_dropped = drop_empty_and_constant_columns(gdf_out)

    if "centroid_geom" in gdf_out.columns:
        try:
            gdf_out = gdf_out.drop(columns=["centroid_geom"])
        except Exception:
            pass

    gdf_out["category_connectivity"] = gdf_out.apply(categorize_connectivity, axis=1)

    if "geometry" not in gdf_out.columns and "geometry" in gdf_munis.columns:
        gdf_out = gdf_out.merge(gdf_munis[["SOG_ID","geometry"]], on="SOG_ID", how="left")
    gdf_out = gdf_out.set_geometry("geometry")

    out_cols = ["SOG_ID","LAU_ID","LAU_NAME","AREA_KM2","POP_2023",
                "stations_count","stations_unique","stations_density_km2",
                "stations_with_operator_share","operator_count","stations_per_10k_pop",
                "stations_within_1km_count","stations_within_5km_count",
                "stations_in_muni_plus_1km_count","stations_in_muni_plus_5km_count",
                "min_distance_km_to_station","mean_distance_km_to_station",
                "accessible_count","accessible_share",
                "category_connectivity"]

    for c in out_cols:
        if c not in gdf_out.columns:
            gdf_out[c] = None

    try:
        export_gdf = gdf_out.to_crs(epsg=4326)
    except Exception:
        export_gdf = gdf_out.copy()

    gpkg = f"{out_prefix}.gpkg"
    csvf = f"{out_prefix}.csv"
    export_gdf.to_file(gpkg, layer="municipal_metrics", driver="GPKG")
    export_gdf[out_cols].to_csv(csvf, index=False)

    print("Columnas eliminadas automáticamente (vacías/constantes):", auto_dropped)
    print("Columnas eliminadas explícitamente (trazabilidad):", explicit_drops)
    return gdf_out

def main():
    gdf_munis = read_munis(MUNIS_PATH)
    print("Municipios leídos:", len(gdf_munis))
    df_st = fetch_all_stations(gdf_munis, max_tile_area_deg2=MAX_TILE_AREA_DEG2)
    print("Estaciones OSM obtenidas:", len(df_st))
    gdf_st = df_to_gdf(df_st)
    if gdf_st.empty:
        gdf_st = gpd.GeoDataFrame(columns=["osm_id","source_type","name","lat","lon","tags","geometry"], geometry="geometry", crs="EPSG:4326").to_crs(epsg=3857)
    out = compute_metrics_and_export(gdf_st, gdf_munis, out_prefix=OUT_PREFIX)
    print("Exportado CSV y GPKG con prefijo:", OUT_PREFIX)
    print("Filas:", len(out))

if __name__ == "__main__":
    main()
