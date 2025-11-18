# main.py
from config import OUTDIR, COLLECTION_S2, COLLECTION_S1, DATE_FROM, DATE_TO, MAX_CLOUD, TOP, AOI_WKT, VIIRS_URL_TEMPLATE
from catalog import build_filter, query_catalog, items_to_df
from osm import fetch_rail_stations
from storage import save_df_to_theme
from tqdm import tqdm
import os
import time
import pandas as pd

import viirs
import demografiaProvincias
import demografiaciudades
import viirs_provincial_gaul # módulo que genera CSV provincial (debe exponer main())

def ensure_outdir(path: str):
    os.makedirs(path, exist_ok=True)

def run_all():
    """Orquestador principal: ejecuta módulos y guarda CSVs en OUTDIR."""
    print("Orquestador: iniciando ejecución de módulos. Salida en:", OUTDIR)
    ensure_outdir(OUTDIR)

    # 0) VIIRS por provincias (EE) -> ahora ejecutamos primero para que puedas verlo pronto
    try:
        print("\n🌙 -> Generando VIIRS por provincias (EE) [PRIORIDAD]")
        csv_path = None
        try:
            csv_path = viirs_provincial_gaul.main()
        except Exception as e:
            print("  ⚠️ Error ejecutando viirs_provincias_gaul.main():", type(e), e)
            raise

        if csv_path:
            try:
                df_viirs_prov = pd.read_csv(csv_path)
                saved = save_df_to_theme(df_viirs_prov, theme="luz_nocturna", filename=os.path.basename(csv_path), base_outdir=OUTDIR)
                print("  ✅ VIIRS provincias guardado en:", saved)
            except Exception as e:
                print("  ⚠️ No se pudo guardar VIIRS provincias con save_df_to_theme:", type(e), e)
                print("  -> El CSV está en:", csv_path)
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
        p = save_df_to_theme(df_s2, "satelital", "sentinel2_products.csv", base_outdir=OUTDIR)
        print("  ✅ Sentinel-2 CSV guardado en:", p)
    except Exception as e:
        print("  ❌ Error al generar CSV Sentinel-2:", type(e), e)

    # 2) Sentinel-1
    try:
        print("-> Consultando catálogo Copernicus (Sentinel-1)")
        filt_s1 = build_filter(COLLECTION_S1, DATE_FROM, DATE_TO, aoi_wkt=AOI_WKT)
        items_s1 = query_catalog(filt_s1, top=TOP)
        df_s1 = items_to_df(items_s1)
        p = save_df_to_theme(df_s1, "satelital", "sentinel1_products.csv", base_outdir=OUTDIR)
        print("  ✅ Sentinel-1 CSV guardado en:", p)
    except Exception as e:
        print("  ❌ Error al generar CSV Sentinel-1:", type(e), e)

    # 3) Transporte (OSM)
    try:
        print("-> Descargando estaciones ferroviarias desde OSM (Overpass)")
        df_trans = fetch_rail_stations(AOI_WKT)
        if df_trans is not None and not df_trans.empty:
            p = save_df_to_theme(df_trans, "transporte", "rail_stations.csv", base_outdir=OUTDIR)
            print("  ✅ Rail stations guardado en:", p)
        else:
            print("  ⚠️ No se obtuvieron estaciones ferroviarias (DataFrame vacío)")
    except Exception as e:
        print("  ❌ Error al descargar estaciones OSM:", type(e), e)

    # 4) Demografía (Eurostat - provincias)
    try:
        print("-> Descargando datos demográficos (Eurostat, provincias)...")
        path_demografia = demografiaProvincias.fetch_population_and_save(base_outdir=OUTDIR)
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
            p = save_df_to_theme(df_cities, "demografia", "demografia_poblacion_municipios.csv", base_outdir=OUTDIR)
            print("  ✅ Demografía municipales guardada en:", p)
        else:
            print("  ⚠️ demografiaciudades no devolvió datos (vacío)")
    except Exception as e:
        print("  ❌ Error al ejecutar demografiaciudades:", type(e), e)

    # 6) VIIRS por municipios (local / rasterio pipeline)
    try:
        print("\n🌙 -> Descargando VIIRS por municipios (NOAA, raster pipeline)")
        start_time = time.time()
        viirs.fetch_viirs_and_save(
            geojson_path="municipios_es.geojson",
            anio_ini=2018,
            anio_fin=2019,
            base_outdir=OUTDIR,
        )
        elapsed = time.time() - start_time
        print(f"\n⏱️ Tiempo total VIIRS municipios: {elapsed/60:.2f} minutos")
    except Exception as e:
        print("  ❌ Error al ejecutar módulo VIIRS municipios:", type(e), e)

if __name__ == "__main__":
    run_all()
