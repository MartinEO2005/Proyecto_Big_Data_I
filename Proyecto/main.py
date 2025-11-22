# main.py
from config import OUTDIR, COLLECTION_S2, COLLECTION_S1, DATE_FROM, DATE_TO, MAX_CLOUD, TOP, AOI_WKT, VIIRS_URL_TEMPLATE
from catalog import build_filter, query_catalog, items_to_df
from osm import fetch_rail_stations
from storage import save_df_to_theme
from tqdm import tqdm
import os
import time
import pandas as pd
import ee

import viirs
import demografiaProvincias
import demografiaciudades
import viirs_provincias_gaul  # módulo provincial (main(outdir, project))

# ROOT unificado para todos los outputs de datos
BASE_DIR = "data"
LUZ_DIR = os.path.join(BASE_DIR, "luz_nocturna")
PROV_DIR = os.path.join(LUZ_DIR, "provincias")
MUN_DIR  = os.path.join(LUZ_DIR, "municipios")
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

    # VIIRS por provincias (prioridad)
    try:
        print("\n🌙 -> Generando VIIRS por provincias (EE) [PRIORIDAD]")
        csv_path = viirs_provincias_gaul.main(outdir=PROV_DIR, project=DEFAULT_PROJECT)
        if csv_path:
            df_viirs_prov = pd.read_csv(csv_path)
            # guardar en data/luz_nocturna/provincias/ usando filename con subcarpeta
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

    # 1) Sentinel-2
    try:
        print("-> Consultando catálogo Copernicus (Sentinel-2)")
        filt_s2 = build_filter(COLLECTION_S2, DATE_FROM, DATE_TO, aoi_wkt=AOI_WKT, cloud=MAX_CLOUD)
        items_s2 = query_catalog(filt_s2, top=TOP)
        df_s2 = items_to_df(items_s2)
        p = save_df_to_theme(df_s2, "satelital", "sentinel2_products.csv", base_outdir=BASE_DIR)
        print("  ✅ Sentinel-2 CSV guardado en:", p)
    except Exception as e:
        print("  ❌ Error al generar CSV Sentinel-2:", type(e), e)

    # 2) Sentinel-1
    try:
        print("-> Consultando catálogo Copernicus (Sentinel-1)")
        filt_s1 = build_filter(COLLECTION_S1, DATE_FROM, DATE_TO, aoi_wkt=AOI_WKT)
        items_s1 = query_catalog(filt_s1, top=TOP)
        df_s1 = items_to_df(items_s1)
        p = save_df_to_theme(df_s1, "satelital", "sentinel1_products.csv", base_outdir=BASE_DIR)
        print("  ✅ Sentinel-1 CSV guardado en:", p)
    except Exception as e:
        print("  ❌ Error al generar CSV Sentinel-1:", type(e), e)

    # 3) Transporte (OSM)
    try:
        print("-> Descargando estaciones ferroviarias desde OSM (Overpass)")
        df_trans = fetch_rail_stations(AOI_WKT)
        if df_trans is not None and not df_trans.empty:
            p = save_df_to_theme(df_trans, "transporte", "rail_stations.csv", base_outdir=BASE_DIR)
            print("  ✅ Rail stations guardado en:", p)
        else:
            print("  ⚠️ No se obtuvieron estaciones ferroviarias (DataFrame vacío)")
    except Exception as e:
        print("  ❌ Error al descargar estaciones OSM:", type(e), e)

    # 4) Demografía (Eurostat - provincias)
    try:
        print("-> Descargando datos demográficos (Eurostat, provincias)...")
        path_demografia = demografiaProvincias.fetch_population_and_save(base_outdir=BASE_DIR)
        if path_demografia is not None:
            print("  ✅ Demografía guardada en:", path_demografia)
        else:
            print("  ⚠️ No se pudieron obtener datos demográficos (DataFrame vacío).")
    except Exception as e:
        print("  ❌ Error al ejecutar demografiaProvincias.fetch_population_and_save:", type(e), e)

    # 5) Demografía por municipios (INE alternativa)
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
    
     # 6) VIIRS por municipios (raster pipeline) -> pedir que escriba en MUN_DIR si acepta base_outdir
    try:
        print("\n🌙 -> Descargando VIIRS por municipios (NOAA, raster pipeline)")
        try:
            viirs.fetch_viirs_and_save(
                geojson_path="municipios_es.geojson",
                anio_ini=2018,
                anio_fin=2019,
                base_outdir=MUN_DIR,
            )
            print("  ✅ VIIRS municipales escritos en:", MUN_DIR)
        except TypeError:
            viirs.fetch_viirs_and_save(
                geojson_path="municipios_es.geojson",
                anio_ini=2018,
                anio_fin=2019,
            )
            print("  ⚠️ viirs.fetch_viirs_and_save no admite base_outdir; comprueba dónde escribe de forma predeterminada.")
    except Exception as e:
        print("  ❌ Error al ejecutar módulo VIIRS municipios:", type(e), e)
        
    # Inicializar Earth Engine centralmente
    try:
        init_ee_orchestrator(project=DEFAULT_PROJECT)
    except Exception as e:
        print("  ❌ Error inicializando Earth Engine en orquestador:", type(e), e)
        return

    

if __name__ == "__main__":
    run_all()
