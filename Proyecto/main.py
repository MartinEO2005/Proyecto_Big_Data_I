from config import OUTDIR, COLLECTION_S2, COLLECTION_S1, DATE_FROM, DATE_TO, MAX_CLOUD, TOP, AOI_WKT, VIIRS_URL_TEMPLATE
from catalog import build_filter, query_catalog, items_to_df
from osm import fetch_rail_stations
from storage import save_df_to_theme
from viirs import tqdm, pd

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

        # 5) VIIRS desde Earth Engine (dos bloques de 5 años)
    try:
        print("-> Descargando VIIRS desde Earth Engine (municipios España)")
        import ee, geemap
        ee.Authenticate()
        ee.Initialize(opt_project='bubbly-reducer-477312-d0')

        municipios_raw = ee.FeatureCollection(
            "projects/bubbly-reducer-477312-d0/assets/LAU_RG_01M_2024_3035"
        ).filter(ee.Filter.eq('CNTR_CODE', 'ES'))

        def disolver_por_municipio(f):
            gid = f.get('GISCO_ID')
            nombre = f.get('LAU_NAME')
            geom = municipios_raw.filter(ee.Filter.eq('GISCO_ID', gid)).geometry().dissolve()
            return ee.Feature(geom).set({'GISCO_ID': gid, 'LAU_NAME': nombre})

        municipios_unicos = municipios_raw.distinct('GISCO_ID').map(disolver_por_municipio)

        def viirs_mes(fecha_iso):
            return ee.ImageCollection("NOAA/VIIRS/DNB/MONTHLY_V1/VCMCFG") \
                .filterDate(fecha_iso, ee.Date(fecha_iso).advance(1, 'month')) \
                .select('avg_rad') \
                .first()

        reducer = ee.Reducer.mean() \
            .combine(ee.Reducer.min(), sharedInputs=True) \
            .combine(ee.Reducer.max(), sharedInputs=True) \
            .combine(ee.Reducer.stdDev(), sharedInputs=True)

        def zonal_stats(img, fecha_iso):
            return img.reduceRegions(
                collection=municipios_unicos,
                reducer=reducer,
                scale=500,
                tileScale=8
            ).map(lambda f: f.set('date', ee.Date(fecha_iso).format('YYYY-MM')))

        def descargar_bloque(inicio, fin, nombre_csv):
            meses = pd.date_range(inicio, fin, freq="MS")
            dfs = []
            for fecha in tqdm(meses, desc=f"VIIRS {inicio} a {fin}"):
                img = viirs_mes(str(fecha.date()))
                if img is None:
                    continue
                stats = zonal_stats(img, str(fecha.date()))
                df_mes = geemap.ee_to_df(stats)
                dfs.append(df_mes)
            df = pd.concat(dfs, ignore_index=True)
            p = save_df_to_theme(df, "luz_nocturna", nombre_csv, base_outdir=OUTDIR)
            print("  ✅ VIIRS bloque guardado en:", p)
            return df

        # Descargar dos bloques de 5 años
        df1 = descargar_bloque("2013-01-01", "2018-01-01", "viirs_municipios_2013_2017.csv")
        df2 = descargar_bloque("2018-01-01", "2023-01-01", "viirs_municipios_2018_2022.csv")

        # Unir ambos
        df_all = pd.concat([df1, df2], ignore_index=True)
        p_final = save_df_to_theme(df_all, "luz_nocturna", "viirs_municipios_2013_2022.csv", base_outdir=OUTDIR)
        print("  ✅ VIIRS final guardado en:", p_final)

    except Exception as e:
        print("  ❌ Error al descargar VIIRS:", type(e), e)


if __name__ == "__main__":
    run_all()
