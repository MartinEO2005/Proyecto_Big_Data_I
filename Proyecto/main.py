#!/usr/bin/env python3
# main.py

import os
import pandas as pd
import geopandas as gpd
import time
import ee
from datetime import datetime

# Importaciones de configuración y utilidades
from extraction.config import (
    OUTDIR, COLLECTION_S2, COLLECTION_S1, DATE_FROM, DATE_TO, 
    MAX_CLOUD, TOP, AOI_WKT
)
from extraction.catalog import build_filter, query_catalog, items_to_df
from extraction.storage import save_df_to_theme
from extraction.neo_lumina_copernicus_downloader import run as downloader_run

# Importaciones de módulos de datos
from extraction import (
    osm_muni_metrics as osm_metrics,
    viirs,
    demografiaProvincias,
    demografiaciudades,
    viirs_provincias_gaul,
    consumo_electrico,
    consumo_renta_media_pib,
    migracion_downloader,
    empresas_transporte_downloader,
    conectividad_downloader
)

# --- 🔹 Configuración de Rutas y Proyecto ---
BASE_DIR = "data"
LUZ_DIR = os.path.join(BASE_DIR, "luz_nocturna")
PROV_DIR = os.path.join(LUZ_DIR, "provincias")
MUN_DIR  = os.path.join(LUZ_DIR, "municipios")
TRANS_DIR = os.path.join(BASE_DIR, "transporte")
DEFAULT_PROJECT = "bubbly-reducer-477312-d0"

def ensure_dirs():
    """Crea la estructura de directorios necesaria."""
    for d in [BASE_DIR, LUZ_DIR, PROV_DIR, MUN_DIR, TRANS_DIR]:
        os.makedirs(d, exist_ok=True)

def init_ee_orchestrator(project=DEFAULT_PROJECT):
    """Inicializa Earth Engine una sola vez para todo el flujo."""
    try:
        ee.Initialize(project=project)
        print(f"✅ Earth Engine inicializado (Proyecto: {project})")
    except Exception:
        print("🔑 Requiere autenticación manual de Earth Engine...")
        ee.Authenticate()
        ee.Initialize(project=project)

def run_all(num_images=None):
    print(f"🚀 Iniciando Orquestador GeoLúmica | Salida: {BASE_DIR}")
    start_total = time.time()
    ensure_dirs()
    
    # 0. Inicialización Central de Servicios
    init_ee_orchestrator()

    # --- 1. LUZ NOCTURNA (VIIRS) [Actualizado con lógica incremental] ---
    print("\n--- 🌙 SECCIÓN: LUZ NOCTURNA (VIIRS) ---")
    try:
        # VIIRS Municipios
        print("-> Procesando VIIRS por municipios...")
        viirs.fetch_viirs_and_save(
            geojson_path="municipios_es.geojson",
            base_outdir=BASE_DIR  # El módulo internamente gestiona la subcarpeta luz_nocturna
        )
        
        # VIIRS Provincias (Prioridad GAUL)
        print("-> Procesando VIIRS por provincias (EE)...")
        csv_path_prov = viirs_provincias_gaul.main(outdir=PROV_DIR, project=DEFAULT_PROJECT)
        if csv_path_prov:
            print(f"  ✅ VIIRS provincias listo en: {csv_path_prov}")
    except Exception as e:
        print(f"  ❌ Error en módulo VIIRS: {e}")

    # --- 2. ECONOMÍA Y CONSUMO (INE) ---
    print("\n--- ⚡ SECCIÓN: ECONOMÍA Y CONSUMO ---")
    try:
        print("-> Descargando intensidad de consumo eléctrico...")
        path_en = consumo_electrico.fetch_viviendas_uso_ine(base_outdir=BASE_DIR)
        
        print("-> Descargando empresas de transporte (INE t=4721)...")
        path_emp = empresas_transporte_downloader.procesar()
        
        print("-> Descargando renta municipal (INE Atlas)...")
        path_renta = consumo_renta_media_pib.fetch_renta_municipios_and_save(base_outdir=BASE_DIR)
        
        print("-> Descargando migración interior (INE)...")
        path_mig = migracion_downloader.fetch_migracion_interior_and_save(base_outdir=BASE_DIR)
        
        print(f"  ✅ Bloque económico completado.")
    except Exception as e:
        print(f"  ❌ Error en bloque económico: {e}")

    # --- 3. DEMOGRAFÍA ---
    print("\n--- 👥 SECCIÓN: DEMOGRAFÍA ---")
    try:
        print("-> Descargando demografía provincial (INE)...")
        demografiaProvincias.fetch_population_and_save(base_outdir=BASE_DIR)
        
        print("-> Descargando población municipal (30 años)...")
        df_cities = demografiaciudades.fetch_population_by_municipality(years=30)
        if df_cities is not None:
            save_df_to_theme(df_cities, "demografia", "demografia_poblacion_municipios.csv", base_outdir=BASE_DIR)
    except Exception as e:
        print(f"  ❌ Error en bloque demográfico: {e}")

    # --- 4. INFRAESTRUCTURA Y MOVILIDAD ---
    print("\n--- 🚆 SECCIÓN: MOVILIDAD Y OSM ---")
    try:
        # Conectividad Histórica
        munis_path = "municipios_es.geojson"
        if os.path.exists(munis_path):
            print("-> Generando serie histórica de conectividad (2010-2025)...")
            conectividad_downloader.fetch_conectividad_historica_and_save(munis_path, base_outdir=BASE_DIR)
        
        # Métricas OSM (Ferrocarril)
        print("-> Consultando Overpass para estaciones OSM (puede tardar)...")
        gdf_munis = osm_metrics.read_munis(munis_path)
        df_st = osm_metrics.fetch_all_stations(gdf_munis, max_tile_area_deg2=getattr(osm_metrics, "MAX_TILE_AREA_DEG2", 1.0))
        gdf_st = osm_metrics.df_to_gdf(df_st)
        out_prefix = os.path.join(TRANS_DIR, "muni_station_metrics")
        osm_metrics.compute_metrics_and_export(gdf_st, gdf_munis, out_prefix=out_prefix)
        print("  ✅ Métricas OSM exportadas.")
    except Exception as e:
        print(f"  ❌ Error en bloque movilidad/OSM: {e}")

    # --- 5. SATELITAL (COPERNICUS) ---
    print("\n--- 📡 SECCIÓN: SATELITAL (Sentinel) ---")
    try:
        # Catálogo
        print("-> Consultando catálogos Sentinel-1 y Sentinel-2...")
        filt_s2 = build_filter(COLLECTION_S2, DATE_FROM, DATE_TO, aoi_wkt=AOI_WKT, cloud=MAX_CLOUD)
        df_s2 = items_to_df(query_catalog(filt_s2, top=TOP))
        save_df_to_theme(df_s2, "satelital", "sentinel2_products.csv", base_outdir=BASE_DIR)

        # Descarga Real de Imágenes
        target = num_images if num_images is not None else TOP
        print(f"-> Iniciando descarga de {target} imágenes Sentinel-2...")
        downloader_run(top=target)
    except Exception as e:
        print(f"  ❌ Error en bloque satelital: {e}")

    total_time = (time.time() - start_total) / 60
    print(f"\n✨ --- ORQUESTACIÓN FINALIZADA --- ✨")
    print(f"⏱️ Tiempo total de ejecución: {total_time:.2f} minutos")

if __name__ == "__main__":
    # Para la prueba de descarga, limitamos a 5 imágenes Sentinel
    run_all(num_images=5)