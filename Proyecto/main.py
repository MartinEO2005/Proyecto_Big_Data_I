from config import *
from catalog import build_filter, query_catalog, items_to_df
from osm import fetch_rail_stations
from viirs import create_viirs_template
from demografia import fetch_population_total_nuts3
from storage import save_df_to_theme


def run():
    print("NeoLumina: iniciando generación CSV temáticos")

    # Sentinel-2
    filt_s2 = build_filter(COLLECTION_S2, DATE_FROM, DATE_TO, aoi_wkt=AOI_WKT, cloud=MAX_CLOUD)
    items_s2 = query_catalog(filt_s2, top=TOP)
    df_s2 = items_to_df(items_s2)
    save_df_to_theme(df_s2, "satelital", "sentinel2_products.csv", base_outdir=OUTDIR)

    # Sentinel-1
    filt_s1 = build_filter(COLLECTION_S1, DATE_FROM, DATE_TO, aoi_wkt=AOI_WKT)
    items_s1 = query_catalog(filt_s1, top=TOP)
    df_s1 = items_to_df(items_s1)
    save_df_to_theme(df_s1, "satelital", "sentinel1_products.csv", base_outdir=OUTDIR)

    print("Usando AOI_WKT (desde config):", AOI_WKT)
    # Transporte (estaciones ferroviarias OSM)
    df_transporte = fetch_rail_stations(AOI_WKT)
    if not df_transporte.empty:
        path = save_df_to_theme(df_transporte, "transporte", "rail_stations.csv", base_outdir=OUTDIR)
        print("CSV guardado en:", path)
    else:
        print("No se generó el CSV de transporte.")

    df_viirs = create_viirs_template(DATE_FROM, DATE_TO, AOI_WKT)
    save_df_to_theme(df_viirs, "luz_nocturna", "viirs_requests.csv", base_outdir=OUTDIR)
    #Eurostat Demografía
    df_demo = fetch_population_total_nuts3()
    if not df_demo.empty:
        save_df_to_theme(df_demo, "demografia", "poblacion_total.csv", base_outdir=OUTDIR)
        print("✅ CSV de población guardado.")
    else:
        print("⚠️ No se generó CSV de demografía.")

    print("✅ Todos los CSV temáticos generados correctamente en:", OUTDIR)


if __name__ == "__main__":
    run()
