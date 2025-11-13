from config import OUTDIR, COLLECTION_S2, COLLECTION_S1, DATE_FROM, DATE_TO, MAX_CLOUD, TOP, AOI_WKT, VIIRS_URL_TEMPLATE
from catalog import build_filter, query_catalog, items_to_df
from osm import fetch_rail_stations
from storage import save_df_to_theme
from tqdm import tqdm
import os
import viirs
import time

# módulos de demografía
import demografiaProvincias
import demografiaciudades

import os


def ensure_outdir(path: str):
    os.makedirs(path, exist_ok=True)


def run_all():
    """Orquestador principal: intenta ejecutar los módulos independientes y guardar CSVs en `OUTDIR`.

    Cada bloque se ejecuta con try/except para evitar que un fallo detenga el resto.
    """
    print("Orquestador: iniciando ejecución de módulos. Salida en:", OUTDIR)
    ensure_outdir(OUTDIR)

    # 1) Productos satelitales (catalog)
    try:
        print("-> Consultando catálogo Copernicus (Sentinel-2)")
        filt_s2 = build_filter(COLLECTION_S2, DATE_FROM, DATE_TO, aoi_wkt=AOI_WKT, cloud=MAX_CLOUD)
        items_s2 = query_catalog(filt_s2, top=TOP)
        df_s2 = items_to_df(items_s2)
        p = save_df_to_theme(df_s2, "satelital", "sentinel2_products.csv", base_outdir=OUTDIR)
        print("  ✅ Sentinel-2 CSV guardado en:", p)
    except Exception as e:
        print("  ❌ Error al generar CSV Sentinel-2:", type(e), e)

    try:
        print("-> Consultando catálogo Copernicus (Sentinel-1)")
        filt_s1 = build_filter(COLLECTION_S1, DATE_FROM, DATE_TO, aoi_wkt=AOI_WKT)
        items_s1 = query_catalog(filt_s1, top=TOP)
        df_s1 = items_to_df(items_s1)
        p = save_df_to_theme(df_s1, "satelital", "sentinel1_products.csv", base_outdir=OUTDIR)
        print("  ✅ Sentinel-1 CSV guardado en:", p)
    except Exception as e:
        print("  ❌ Error al generar CSV Sentinel-1:", type(e), e)

    # 2) Transporte (estaciones ferroviarias OSM)
    try:
        print("-> Descargando estaciones ferroviarias desde OSM (Overpass)")
        df_trans = fetch_rail_stations(AOI_WKT)
        if not df_trans.empty:
            p = save_df_to_theme(df_trans, "transporte", "rail_stations.csv", base_outdir=OUTDIR)
            print("  ✅ Rail stations guardado en:", p)
        else:
            print("  ⚠️ No se obtuvieron estaciones ferroviarias (DataFrame vacío)")
    except Exception as e:
        print("  ❌ Error al descargar estaciones OSM:", type(e), e)

     # 3) Demografía (Eurostat)
        try:
            print("-> Descargando datos demográficos (Eurostat, provincias)...")
            import demografiaProvincias as demografia_prov

            path_demografia = demografia_prov.fetch_population_and_save(base_outdir=OUTDIR)
            if path_demografia is not None:
                print("  ✅ Demografía guardada en:", path_demografia)
            else:
                print("  ⚠️ No se pudieron obtener datos demográficos (DataFrame vacío).")
        except Exception as e:
            print("  ❌ Error al ejecutar demografiaProvincias.fetch_population_and_save:", type(e), e)


    # 4) Demografía por ciudades (INE alternativa)
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
 
    # 5) VIIRS (descarga y limpieza)
try:
    print("\n🌙 -> Descargando datos de luz nocturna VIIRS (NOAA)...")

        # Barra de progreso simple para envolver toda la descarga
    start_time = time.time()
    for _ in tqdm(range(1), desc="Descargando VIIRS", ncols=80, colour="cyan"):
            viirs.fetch_viirs_and_save(
                geojson_path="municipios_es.geojson",
                anio_ini=2018,
                anio_fin=2019,
                base_outdir=OUTDIR,
            )

    elapsed = time.time() - start_time
    print(f"\n⏱️ Tiempo total VIIRS: {elapsed/60:.2f} minutos")

except Exception as e:
    print("  ❌ Error al ejecutar módulo VIIRS:", type(e), e)


if __name__ == "__main__":
    run_all()