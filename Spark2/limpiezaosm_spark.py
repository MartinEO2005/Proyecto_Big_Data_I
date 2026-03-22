from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    lit,
    trim,
    lpad,
    substring,
    when,
    round as spark_round,
    lower,
    initcap,
    regexp_replace
)
from pyspark.sql.types import StringType, DoubleType
import os
import shutil

INPUT_CSV = "/app/data/transporte/muni_station_metrics_reduced.csv"
OUTPUT_CSV = "/app/data/clean/muni_station_osm_limpio.csv"


INE_PROV_MAP = {
    "01": "Álava", "02": "Albacete", "03": "Alicante", "04": "Almería", "05": "Ávila",
    "06": "Badajoz", "07": "Islas Baleares", "08": "Barcelona", "09": "Burgos", "10": "Cáceres",
    "11": "Cádiz", "12": "Castellón", "13": "Ciudad Real", "14": "Córdoba", "15": "A Coruña",
    "16": "Cuenca", "17": "Girona", "18": "Granada", "19": "Guadalajara", "20": "Gipuzkoa",
    "21": "Huelva", "22": "Huesca", "23": "Jaén", "24": "León", "25": "Lleida",
    "26": "La Rioja", "27": "Lugo", "28": "Madrid", "29": "Málaga", "30": "Murcia",
    "31": "Navarra", "32": "Ourense", "33": "Asturias", "34": "Palencia", "35": "Las Palmas",
    "36": "Pontevedra", "37": "Salamanca", "38": "Santa Cruz de Tenerife", "39": "Cantabria",
    "40": "Segovia", "41": "Sevilla", "42": "Soria", "43": "Tarragona", "44": "Teruel",
    "45": "Toledo", "46": "Valencia", "47": "Valladolid", "48": "Bizkaia", "49": "Zamora",
    "50": "Zaragoza", "51": "Ceuta", "52": "Melilla"
}


def guardar_csv_unico(df, output_path: str):
    temp_dir = output_path + "_tmp"

    if os.path.exists(temp_dir):
        shutil.rmtree(temp_dir)

    if os.path.exists(output_path):
        if os.path.isdir(output_path):
            shutil.rmtree(output_path)
        else:
            os.remove(output_path)

    (
        df.coalesce(1)
        .write
        .mode("overwrite")
        .option("header", True)
        .option("encoding", "utf-8")
        .csv(temp_dir)
    )

    part_file = None
    for file_name in os.listdir(temp_dir):
        if file_name.startswith("part-") and file_name.endswith(".csv"):
            part_file = os.path.join(temp_dir, file_name)
            break

    if part_file is None:
        raise FileNotFoundError("No se encontró ningún archivo part-*.csv en la salida temporal")

    shutil.move(part_file, output_path)
    shutil.rmtree(temp_dir)


def normalizar_nombre_municipio(df, col_name):
    df = df.withColumn(col_name, trim(col(col_name)))
    df = df.withColumn(col_name, initcap(lower(col(col_name))))
    df = df.withColumn(col_name, regexp_replace(col(col_name), " De ", " de "))
    df = df.withColumn(col_name, regexp_replace(col(col_name), " Del ", " del "))
    df = df.withColumn(col_name, regexp_replace(col(col_name), " La ", " la "))
    df = df.withColumn(col_name, regexp_replace(col(col_name), " Las ", " las "))
    df = df.withColumn(col_name, regexp_replace(col(col_name), " El ", " el "))
    df = df.withColumn(col_name, regexp_replace(col(col_name), " Y ", " y "))
    df = df.withColumn(col_name, regexp_replace(col(col_name), " A ", " a "))
    return df


def main():
    spark = SparkSession.builder.appName("limpiezaosm_spark").getOrCreate()

    print("📥 Cargando métricas OSM...")
    df = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(INPUT_CSV)
    )

    n_rows = df.count()
    print(f"📊 Filas: {n_rows}, Columnas: {len(df.columns)}")

    print("🔎 Columnas detectadas:")
    print(df.columns)

    # 1) Derivar PROV_NAME si no existe o si está vacío
    if "PROV_NAME" not in df.columns:
        if "LAU_ID" in df.columns:
            df = df.withColumn("LAU_ID", lpad(col("LAU_ID").cast(StringType()), 5, "0"))
            df = df.withColumn("INE_PROV_CODE", substring(col("LAU_ID"), 1, 2))

            expr_prov = None
            for codigo, nombre in INE_PROV_MAP.items():
                if expr_prov is None:
                    expr_prov = when(col("INE_PROV_CODE") == codigo, lit(nombre))
                else:
                    expr_prov = expr_prov.when(col("INE_PROV_CODE") == codigo, lit(nombre))

            df = df.withColumn("PROV_NAME", expr_prov.otherwise(lit(None)))
        else:
            df = df.withColumn("PROV_NAME", lit(None))

    # 2) Normalizar LAU_NAME
    if "LAU_NAME" in df.columns:
        df = normalizar_nombre_municipio(df, "LAU_NAME")

    # 3) Seleccionar columnas útiles
    columnas_preferidas = [
        "PROV_NAME",
        "SOG_ID",
        "LAU_ID",
        "LAU_NAME",
        "AREA_KM2",
        "POP_2023",
        "stations_count",
        "stations_unique",
        "stations_density_km2",
        "stations_with_operator_share",
        "operator_count",
        "stations_per_10k_pop",
        "stations_within_1km_count",
        "stations_within_5km_count",
        "stations_in_muni_plus_1km_count",
        "stations_in_muni_plus_5km_count",
        "min_distance_km_to_station",
        "mean_distance_km_to_station",
        "accessible_count",
        "accessible_share",
        "category_connectivity",
    ]

    columnas_finales = [c for c in columnas_preferidas if c in df.columns]
    df = df.select(*columnas_finales)

    # 4) Renombrado final
    renombres = {
        "PROV_NAME": "nombre_provincia",
        "SOG_ID": "sog_id",
        "LAU_ID": "lau_id",
        "LAU_NAME": "nombre_municipio",
        "AREA_KM2": "area_km2",
        "POP_2023": "poblacion_2023",
        "stations_count": "num_estaciones",
        "stations_unique": "num_estaciones_unicas",
        "stations_density_km2": "densidad_estaciones_km2",
        "stations_with_operator_share": "share_estaciones_con_operador",
        "operator_count": "num_estaciones_con_operador",
        "stations_per_10k_pop": "estaciones_por_10k_hab",
        "stations_within_1km_count": "estaciones_a_1km",
        "stations_within_5km_count": "estaciones_a_5km",
        "stations_in_muni_plus_1km_count": "estaciones_muni_mas_1km",
        "stations_in_muni_plus_5km_count": "estaciones_muni_mas_5km",
        "min_distance_km_to_station": "distancia_min_km_estacion",
        "mean_distance_km_to_station": "distancia_media_km_estacion",
        "accessible_count": "num_estaciones_accesibles",
        "accessible_share": "share_estaciones_accesibles",
        "category_connectivity": "categoria_conectividad",
    }

    for viejo, nuevo in renombres.items():
        if viejo in df.columns:
            df = df.withColumnRenamed(viejo, nuevo)

    # 5) Redondear columnas numéricas a 3 decimales
    columnas_numericas = [
        field.name for field in df.schema.fields
        if field.dataType.typeName() in ["integer", "long", "double", "float", "decimal", "short"]
    ]

    for c in columnas_numericas:
        df = df.withColumn(c, spark_round(col(c).cast(DoubleType()), 3))

    # 6) Resumen de ceros en columnas numéricas
    print("\n--- Ceros por columna numérica ---")
    print(f"{'columna':40s} {'ceros':>8s} {'% total':>10s}")

    for c in columnas_numericas:
        zeros = df.filter(col(c) == 0).count()
        pct = (zeros / n_rows * 100) if n_rows else 0.0
        print(f"{c:40s} {zeros:8d} {pct:10.3f}")

    # 7) Resumen de nulos por columna
    print("\n--- Nulos por columna ---")
    print(f"{'columna':40s} {'nulos':>8s} {'% total':>10s}")

    for c in df.columns:
        nulos = df.filter(col(c).isNull()).count()
        pct = (nulos / n_rows * 100) if n_rows else 0.0
        print(f"{c:40s} {nulos:8d} {pct:10.3f}")

    # 8) Rellenar nulos con 0
    df = df.fillna(0)

    # 9) Ordenar por nombre de municipio si existe
    if "nombre_municipio" in df.columns:
        df = df.orderBy("nombre_municipio")

    guardar_csv_unico(df, OUTPUT_CSV)

    print(f"\n✅ CSV guardado en: {OUTPUT_CSV}")
    print(f"📊 Filas finales: {df.count()}")
    print("🧾 Columnas finales:")
    print(df.columns)

    spark.stop()


if __name__ == "__main__":
    main()