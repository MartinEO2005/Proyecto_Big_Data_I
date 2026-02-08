#!/usr/bin/env python3
# main.py

import os
import importlib
from pathlib import Path
import pandas as pd
import geopandas as gpd

from extraction.config import OUTDIR, COLLECTION_S2, COLLECTION_S1, DATE_FROM, DATE_TO, MAX_CLOUD, TOP, AOI_WKT, VIIRS_URL_TEMPLATE
from extraction.catalog import build_filter, query_catalog, items_to_df
from extraction.storage import save_df_to_theme
from extraction.neo_lumina_copernicus_downloader import run as downloader_run
from tqdm import tqdm
import time
import ee
from extraction import osm_muni_metrics as osm_metrics 
from extraction import viirs
from extraction import demografiaProvincias
from extraction import demografiaciudades
from extraction import viirs_provincias_gaul
from extraction  import consumo_electrico_gas  
from extraction  import consumo_renta_media_pib
from extraction  import migracion_downloader
from extraction import empresas_transporte_downloader
from extraction.neo_lumina_copernicus_downloader import run as downloader_run
from extraction import conectividad_downloader 

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

def run_all(num_images=None):
    print("Orquestador: iniciando ejecución de módulos. Salida en:", BASE_DIR)
    ensure_outdir(BASE_DIR)
    ensure_outdir(LUZ_DIR)
    ensure_outdir(PROV_DIR)
    ensure_outdir(MUN_DIR)

# --- Serie Histórica de Conectividad y Movilidad (2010-2025) ---
    try:
        print("\n🚗 -> Generando serie histórica de conectividad (2010-2025)...")
        
        munis_candidates = [
            os.path.join(BASE_DIR, "municipios_es.geojson"),
            "municipios_es.geojson",
            os.path.join(MUN_DIR, "municipios_es.geojson")
        ]
        munis_path = next((p for p in munis_candidates if os.path.exists(p)), None)

        if munis_path:
            path_movilidad = conectividad_downloader.fetch_conectividad_historica_and_save(
                geojson_path=munis_path, 
                base_outdir=BASE_DIR
            )
            print(f"  ✅ Datos de conectividad guardados en: {path_movilidad}")
        else:
            print("  ⚠️ No se encontró municipios_es.geojson, saltando módulo de conectividad.")

    except Exception as e:
        print(f"  ❌ Error al ejecutar conectividad_downloader: {type(e)} {e}")

# --- 3. SECCIÓN COPERNICUS: DESCARGA REAL (IMÁGENES) ---
    try:
        # Aquí es donde ocurre la descarga, extracción y creación de PNGs
        # El parámetro 'top' del downloader_run ahora manda sobre el config.py
        target = num_images if num_images is not None else TOP
        print(f"\n📡 -> Iniciando descarga de {target} imágenes Sentinel-2...")
        
        downloader_run(top=target)

        print(f"   ✅ Imágenes y metadatos listos en {BASE_DIR}/satelital/copernicus/")
    except Exception as e:
        print(f"   ❌ Error crítico en Downloader Copernicus: {e}")


    # --- VIIRS municipios ---
    try:
        print("\n🌙 -> Descargando VIIRS por municipios (NOAA, raster pipeline)")
        viirs.fetch_viirs_and_save(
            geojson_path="municipios_es.geojson",
            anio_ini=2018,
            anio_fin=2023,
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
    # --- EXTRACCIÓN DE DATOS DEMOGRÁFICOS ---
    try:
        # Nota: Ahora conectamos con la API del INE (Padrón Continuo, Tabla 2852)
        print("-> Descargando datos demográficos oficiales (INE, provincias)...")
        
        path_demografia = demografiaProvincias.fetch_population_and_save(base_outdir=BASE_DIR)
        
        if path_demografia:
            print(f"  ✅ Demografía guardada exitosamente en: {path_demografia}")
            
            # Verificación opcional de carga para el log del main
            import pandas as pd
            df_check = pd.read_csv(path_demografia)
            print(f"     [Log] Registros: {len(df_check)} | Último año: {df_check['year'].max()}")
        else:
            print("  ⚠️ Advertencia: El proceso terminó sin generar el archivo (DataFrame vacío).")
            
    except Exception as e:
        print(f"  ❌ Error crítico en la fase de demografía: {type(e).__name__} -> {e}")

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

if __name__ == "__main__":
    run_all(num_images=5)
