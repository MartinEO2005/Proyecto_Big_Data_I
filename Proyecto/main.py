from config import OUTDIR, COLLECTION_S2, COLLECTION_S1, DATE_FROM, DATE_TO, MAX_CLOUD, TOP, AOI_WKT, VIIRS_URL_TEMPLATE
from catalog import build_filter, query_catalog, items_to_df
from osm import fetch_rail_stations
from storage import save_df_to_theme

# módulos de demografía
import demografia
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

    # 3) Demografía (INE)
    try:
        print("-> Descargando datos demográficos (INE)...")
        # demografia.fetch_population_and_save guarda dos CSVs y devuelve las rutas
        if hasattr(demografia, "fetch_population_and_save"):
            path_mun, path_prov = demografia.fetch_population_and_save(filename_municipal="population_municipal.csv",
                                                                        filename_province="population_by_province.csv",
                                                                        base_outdir=OUTDIR)
            print("  ✅ Demografía guardada:", path_mun, path_prov)
        else:
            # Fallback: intentar obtener el dataframe vía fetch_population_ine_api()
            print("  ⚠️ demografia.fetch_population_and_save no disponible; usando fallback fetch_population_ine_api()")
            if hasattr(demografia, "fetch_population_ine_api"):
                df_demo = demografia.fetch_population_ine_api()
                if df_demo is not None and not df_demo.empty:
                    p1 = save_df_to_theme(df_demo, "demografia", "population_municipal.csv", base_outdir=OUTDIR)
                    df_prov = df_demo.groupby(["province", "year"], as_index=False)["population"].sum().rename(columns={"population": "population_total"})
                    p2 = save_df_to_theme(df_prov, "demografia", "population_by_province.csv", base_outdir=OUTDIR)
                    print("  ✅ Demografía guardada (fallback):", p1, p2)
                else:
                    print("  ⚠️ Fallback demografia no devolvió datos.")
            else:
                print("  ❌ demografia no expone funciones utilizables en este entorno.")
    except Exception as e:
        print("  ❌ Error al ejecutar demografia.fetch_population_and_save:", type(e), e)

    # 4) Demografía por ciudades (INE alternativa)
    try:
        print("-> Descargando población por municipio (demografiaciudades)...")
        df_cities = demografiaciudades.fetch_population_by_municipality(years=1)
        if df_cities is not None and not df_cities.empty:
            p = save_df_to_theme(df_cities, "demografia", "demografia_poblacion_municipios.csv", base_outdir=OUTDIR)
            print("  ✅ Demografía municipales guardada en:", p)
        else:
            print("  ⚠️ demografiaciudades no devolvió datos (vacío)")
    except Exception as e:
        print("  ❌ Error al ejecutar demografiaciudades:", type(e), e)

    # 5) Intentar ejecutar viirs (Earth Engine). IMPORTANTE: puede requerir autenticación/EE)
    try:
        print("-> Intentando obtener VIIRS via mirror público (sin Earth Engine)")
        import viirs as vi
        out_csv = os.path.join(OUTDIR, "luz_nocturna", "viirs_spain_sample.csv")
        # Si el usuario definió la variable de entorno VIIRS_PERIODS, usamos la función
        # que descarga series temporales y agrega por provincia.
        # Formatos admitidos en VIIRS_PERIODS:
        #  - Lista de meses: 2023-03,2023-04
        #  - Rango inclusivo: 2022-01:2022-03
        periods_env = os.getenv("VIIRS_PERIODS")
        if periods_env:
            def _months_between(start_y, start_m, end_y, end_m):
                ym = start_y * 12 + (start_m - 1)
                end_ym = end_y * 12 + (end_m - 1)
                months = []
                for t in range(ym, end_ym + 1):
                    y = t // 12
                    m = t % 12 + 1
                    months.append((y, m))
                return months

            periods = []
            for token in [t.strip() for t in periods_env.split(',') if t.strip()]:
                if ':' in token:
                    left, right = token.split(':', 1)
                    sy, sm = [int(x) for x in left.split('-')]
                    ey, em = [int(x) for x in right.split('-')]
                    periods.extend(_months_between(sy, sm, ey, em))
                else:
                    y, m = [int(x) for x in token.split('-')]
                    periods.append((y, m))

            print(f"→ VIIRS: descargando/agregando períodos: {periods}")
            # llamar a la función que descarga y agrega por provincia
            try:
                out = vi.export_viirs_periods(periods, aggregate_level='province', url_template=VIIRS_URL_TEMPLATE, out_dir=os.path.join(OUTDIR, "luz_nocturna", "viirs_tifs"), out_csv=os.path.join(OUTDIR, "luz_nocturna", "viirs_by_province.csv"))
                if out:
                    print("  ✅ VIIRS timeseries agregada en:", out)
                else:
                    print("  ⚠️ VIIRS timeseries no se pudo generar. Revisa logs.")
            except Exception as e:
                print("  ❌ Error al generar timeseries VIIRS:", type(e), e)
        else:
            # Si no hay VIIRS_PERIODS, descargamos/agrupamos el mes anterior
            from datetime import date
            today = date.today()
            y = today.year
            m = today.month - 1
            if m == 0:
                m = 12; y -= 1
            periods = [(y, m)]
            try:
                out = vi.export_viirs_periods(periods, aggregate_level='province', url_template=VIIRS_URL_TEMPLATE, out_dir=os.path.join(OUTDIR, "luz_nocturna", "viirs_tifs"), out_csv=os.path.join(OUTDIR, "luz_nocturna", "viirs_by_province.csv"))
                if out:
                    print("  ✅ VIIRS descargado y agregado en:", out)
                else:
                    print("  ⚠️ VIIRS no se pudo descargar/muestrear. Revisa la variable VIIRS_URL_TEMPLATE o descarga manualmente.")
            except Exception as e:
                print("  ❌ Error al generar VIIRS (fallback mes anterior):", type(e), e)
    except Exception as e:
        print("  ⚠️ viirs no pudo ejecutarse/importarse:", type(e), e)

    print("Orquestador: ejecución terminada.")


if __name__ == "__main__":
    run_all()
