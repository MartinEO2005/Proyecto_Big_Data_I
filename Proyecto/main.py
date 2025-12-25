#!/usr/bin/env python3
# main.py
"""
Orquestador principal: integra OSM dentro de run_all() (sin subprocess).
"""

import os
import importlib
from pathlib import Path
import pandas as pd
import geopandas as gpd

from config import OUTDIR, COLLECTION_S2, COLLECTION_S1, DATE_FROM, DATE_TO, MAX_CLOUD, TOP, AOI_WKT, VIIRS_URL_TEMPLATE
from catalog import build_filter, query_catalog, items_to_df
from storage import save_df_to_theme
from neo_lumina_copernicus_downloader import run as downloader_run
from tqdm import tqdm
import time
import ee
import osm_muni_metrics as osm_metrics 
import viirs
import demografiaProvincias
import demografiaciudades
import viirs_provincias_gaul
import consumo_electrico_gas  
import consumo_renta_media_pib
import migracion_downloader
import empresas_transporte_downloader

# ROOT unificado para todos los outputs de datos
BASE_DIR = "data"
LUZ_DIR = os.path.join(BASE_DIR, "luz_nocturna")
PROV_DIR = os.path.join(LUZ_DIR, "provincias")
MUN_DIR  = os.path.join(LUZ_DIR, "municipios")
TRANS_DIR = os.path.join(BASE_DIR, "transporte")
DEFAULT_PROJECT = "bubbly-reducer-477312-d0"

def ensure_outdir(path: str):
    os.makedirs(path, exist_ok=True)

def init_ee_orchestrator(project: str | None = DEFAULT_PROJECT):
    try:
        ee.Initialize(project=project)
        print(f"✅ Earth Engine inicializado desde orquestador (project={project})")
    except Exception:
        print("🔑 Autenticando Earth Engine desde orquestador...")
        ee.Authenticate()
        ee.Initialize(project=project)
        print(f"✅ Earth Engine autenticado e inicializado desde orquestador (project={project})")

def run_all():
    print("Orquestador: iniciando ejecución de módulos. Salida en:", BASE_DIR)
    ensure_outdir(BASE_DIR)
    ensure_outdir(LUZ_DIR)
    ensure_outdir(PROV_DIR)
    ensure_outdir(MUN_DIR)

    # --- Empresas transporte (INE t=4721) ---
    try:
        print("\n🏭 -> Descargando empresas por municipio y provincia (INE t=4721)...")
        path_empresas = empresas_transporte_downloader.procesar()
        print("  ✅ Empresas guardadas en:", path_empresas)
    except Exception as e:
        print("  ❌ Error al ejecutar empresas_transporte_downloader:", type(e), e)

        # --- Migración interior (INE t=69743) ---
    try:
        print("\n🚚 -> Descargando migración interior municipal (INE)...")
        path_migracion = migracion_downloader.fetch_migracion_interior_and_save(base_outdir=BASE_DIR)
        print("  ✅ Migración interior guardada en:", path_migracion)
    except Exception as e: 
        print("  ❌ Error al ejecutar migracion_downloader:", type(e), e)

        # --- Renta municipal (INE Atlas) ---
    try:
        print("\n💶 -> Descargando renta municipal (INE Atlas)...")
        path_renta = consumo_renta_media_pib.fetch_renta_municipios_and_save(base_outdir=BASE_DIR)
        print("  ✅ Renta municipal guardada en:", path_renta)
    except Exception as e:
        print("  ❌ Error al ejecutar renta_municipios:", type(e), e)

    # --- Consumo Energía (CNMC) ---
    try:
        print("\n⚡ -> Descargando consumo energético provincial (CNMC)...")
        path_energia = consumo_electrico_gas.fetch_energy_consumption_and_save(base_outdir=BASE_DIR)
        if path_energia:
            print("  ✅ Datos de energía guardados en:", path_energia)
        else:
            print("  ⚠️ No se pudieron obtener datos de energía.")
    except Exception as e:
        print("  ❌ Error al ejecutar energia_cnmc:", type(e), e)

     # --- Demografía provincias ---
    try:
        print("-> Descargando datos demográficos (Eurostat, provincias)...")
        path_demografia = demografiaProvincias.fetch_population_and_save(base_outdir=BASE_DIR)
        if path_demografia is not None:
            print("  ✅ Demografía guardada en:", path_demografia)
        else:
            print("  ⚠️ No se pudieron obtener datos demográficos (DataFrame vacío).")
    except Exception as e:
        print("  ❌ Error al ejecutar demografiaProvincias.fetch_population_and_save:", type(e), e)

    # 1) Metricas OSM municipios
    try:
        print("\n🚆 -> Generando métricas municipales de conectividad ferroviaria (OSM)")

        # directorios y paths
        ensure_outdir(TRANS_DIR)

        # localiza el GeoJSON de municipios (primero en data/, luego en cwd)
        munis_candidates = [
            os.path.join(BASE_DIR, "municipios_es.geojson"),
            "municipios_es.geojson",
            os.path.join(MUN_DIR, "municipios_es.geojson")
        ]
        munis_path = None
        for p in munis_candidates:
            if os.path.exists(p):
                munis_path = p
                break

        if munis_path is None:
            raise FileNotFoundError(
                "Fichero de municipios no encontrado. Buscado en: "
                f"{munis_candidates}"
            )

        print(f"  -> Usando fichero de municipios: {munis_path}")

        # lectura y sanity checks
        gdf_munis = osm_metrics.read_munis(munis_path)
        print(f"  -> Municipios leídos: {len(gdf_munis)}")

        # fetch de estaciones OSM (puede ser costoso/time-consuming)
        print("  -> Consultando Overpass para estaciones (puede tardar)...")
        # usamos la constante del módulo si está definida, si no, caemos a 1.0
        max_tile_area = getattr(osm_metrics, "MAX_TILE_AREA_DEG2", 1.0)
        df_st = osm_metrics.fetch_all_stations(gdf_munis, max_tile_area_deg2=max_tile_area)
        print(f"  -> Estaciones OSM obtenidas (rows): {len(df_st)}")

        # convertir a GeoDataFrame (EPSG:3857 esperado por compute)
        gdf_st = osm_metrics.df_to_gdf(df_st)
        if gdf_st.empty:
            # si no hay estaciones, creamos GeoDataFrame vacío con el CRS que espera compute
            gdf_st = gpd.GeoDataFrame(
                columns=["osm_id","source_type","name","lat","lon","tags","geometry"],
                geometry="geometry",
                crs="EPSG:4326"
            ).to_crs(epsg=3857)

        # prefijo de salida garantizando el directorio TRANS_DIR
        out_prefix = os.path.join(TRANS_DIR, getattr(osm_metrics, "OUT_PREFIX", "muni_station_metrics_reduced"))

        # Ejecuta cálculo y exporta (GPKG + CSV) — el propio módulo hace la exportación
        out_gdf = osm_metrics.compute_metrics_and_export(gdf_st, gdf_munis, out_prefix=out_prefix)
        print("  ✅ Métricas OSM exportadas con prefijo:", out_prefix)
        print("     -> filas resultado:", len(out_gdf))

    except FileNotFoundError as e:
        print("  ❌ No se pudo ejecutar osm_muni_metrics:", type(e), e)
    except Exception as e:
        # captura errores en Overpass / geopandas IO / procesamiento
        print("  ❌ Error al ejecutar módulo osm_muni_metrics:", type(e), e)
        # intento de fallback mínimo: crear CSV de trazabilidad vacío para que el pipeline no se rompa
        try:
            fallback_csv = os.path.join(TRANS_DIR, "muni_station_metrics_failed.csv")
            pd.DataFrame([{
                "error": str(e),
                "timestamp": pd.Timestamp.now()
            }]).to_csv(fallback_csv, index=False)
            print("  ⚠️ Fallback: creado CSV de fallo en:", fallback_csv)
        except Exception as e2:
            print("  ❌ No se pudo crear el CSV de fallback:", type(e2), e2)


    # --- VIIRS por provincias (prioridad) ---
    try:
        print("\n🌙 -> Generando VIIRS por provincias (EE) [PRIORIDAD]")
        csv_path = viirs_provincias_gaul.main(outdir=PROV_DIR, project=DEFAULT_PROJECT)
        if csv_path:
            df_viirs_prov = pd.read_csv(csv_path)
            saved = save_df_to_theme(
                df_viirs_prov,
                theme="luz_nocturna",
                filename=f"provincias/{os.path.basename(csv_path)}",
                base_outdir=BASE_DIR
            )
            print("  ✅ VIIRS provincias guardado en:", saved)
        else:
            print("  ⚠️ El módulo de VIIRS no devolvió ruta al CSV final.")
    except Exception as e:
        print("  ❌ Error al ejecutar módulo VIIRS provincias:", type(e), e)

    # --- Sentinel-2 ---
    try:
        print("-> Consultando catálogo Copernicus (Sentinel-2)")
        filt_s2 = build_filter(COLLECTION_S2, DATE_FROM, DATE_TO, aoi_wkt=AOI_WKT, cloud=MAX_CLOUD)
        items_s2 = query_catalog(filt_s2, top=TOP)
        df_s2 = items_to_df(items_s2)
        p = save_df_to_theme(df_s2, "satelital", "sentinel2_products.csv", base_outdir=BASE_DIR)
        print("  ✅ Sentinel-2 CSV guardado en:", p)
    except Exception as e:
        print("  ❌ Error al generar CSV Sentinel-2:", type(e), e)

    # --- Sentinel-1 ---
    try:
        print("-> Consultando catálogo Copernicus (Sentinel-1)")
        filt_s1 = build_filter(COLLECTION_S1, DATE_FROM, DATE_TO, aoi_wkt=AOI_WKT)
        items_s1 = query_catalog(filt_s1, top=TOP)
        df_s1 = items_to_df(items_s1)
        p = save_df_to_theme(df_s1, "satelital", "sentinel1_products.csv", base_outdir=BASE_DIR)
        print("  ✅ Sentinel-1 CSV guardado en:", p)
    except Exception as e:
        print("  ❌ Error al generar CSV Sentinel-1:", type(e), e)
        
     # --- Downloader Copernicus: descarga real de imágenes Sentinel-2 ---
    try:
        print("\n📡 -> Iniciando downloader Copernicus (descarga de imágenes)...")

        downloader_run(
            collection="SENTINEL-2",
            aoi="config",
            download=True,     # activa la descarga
            convert=True,      # genera TIFF + PNG
            top=5,             # baja SOLO 5 imágenes
            workers=2,         # procesamiento paralelo
            asset="tci"        # True color TCI
        )

        print("  ✅ Downloader Copernicus completado: imágenes disponibles en data/satelital/copernicus/")

    except Exception as e:
        print("  ❌ Error ejecutando downloader Copernicus:", type(e), e)

    # --- Demografía municipios ---
    try:
        print("-> Descargando población por municipio (demografiaciudades)...")
        df_cities = demografiaciudades.fetch_population_by_municipality(years=30)
        if df_cities is not None and not df_cities.empty:
            p = save_df_to_theme(df_cities, "demografia", "demografia_poblacion_municipios.csv", base_outdir=BASE_DIR)
            print("  ✅ Demografía municipales guardada en:", p)
        else:
            print("  ⚠️ demografiaciudades no devolvió datos (vacío)")
    except Exception as e:
        print("  ❌ Error al ejecutar demografiaciudades:", type(e), e)


    # --- VIIRS municipios ---
    try:
        print("\n🌙 -> Descargando VIIRS por municipios (NOAA, raster pipeline)")
        viirs.fetch_viirs_and_save(
            geojson_path="municipios_es.geojson",
            anio_ini=2018,
            anio_fin=2019,
            base_outdir=MUN_DIR
        )
        print("  ✅ VIIRS municipales escritos en:", MUN_DIR)
    except Exception as e:
        print("  ❌ Error al ejecutar módulo VIIRS municipios:", type(e), e)

    # Inicializar Earth Engine centralmente
    try:
        init_ee_orchestrator(project=DEFAULT_PROJECT)
    except Exception as e:
        print("  ❌ Error inicializando Earth Engine en orquestador:", type(e), e)

if __name__ == "__main__":
    run_all()