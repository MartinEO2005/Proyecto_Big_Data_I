from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType
from pyspark.sql import Window
import pandas as pd
import re
import json
import unicodedata
import difflib
import os
from utils_spark import save_output


MUNI_RAW = "hdfs://namenode:9000/data/raw/demografia/demografia_poblacion_municipios.csv"
PROV_CSV = "/app/data/demografia/demografia_poblacion_provincias.csv"
MIGRACIONES_CSV = "hdfs://namenode:9000/data/raw/migracion/migracion_interior_municipios.csv"
OUTPUT = "hdfs://namenode:9000/data/clean/demografia_municipios_final"

if os.path.exists("/app/municipios_es.geojson"):
    GEOJSON = "/app/municipios_es.geojson"
else:
    GEOJSON = "municipios_es.geojson"


CORRECCIONES_MANUALES = {
    "oza dos rios": "15",
    "cesuras": "15",
    "cotobade": "36",
    "atez atetz": "31",
    "novetle novele": "46"
}


def normalize_py(s):
    if s is None or pd.isna(s):
        return ""
    t = str(s).lower().strip()
    t = unicodedata.normalize("NFKD", t)
    t = "".join(c for c in t if not unicodedata.combining(c))
    t = t.replace("ñ", "n")
    t = re.sub(r"[^a-z0-9\s]", " ", t)
    t = re.sub(r"\s{2,}", " ", t).strip()
    return t


def clean_municipio_string_py(text):
    if text is None or pd.isna(text):
        return (None, "Total")

    t = str(text).strip()
    cat = "Total"

    if re.search(r"\b(Hombres?)\b", t, re.IGNORECASE):
        cat = "Hombres"
    elif re.search(r"\b(Mujeres?)\b", t, re.IGNORECASE):
        cat = "Mujeres"

    patterns = [
        r"\.?\s*Total\.\s*Total habitantes\.\s*Personas\.?$",
        r"\.?\s*Total habitantes\.\s*Personas\.?$",
        r"\.?\s*Personas\.?$",
        r"\.?\s*Total\s*$"
    ]

    clean_name = t
    for pat in patterns:
        clean_name = re.sub(pat, "", clean_name, flags=re.IGNORECASE)

    clean_name = re.sub(r"\.?\s*(Hombres?|Mujeres?|Total)\s*$", "", clean_name, flags=re.IGNORECASE)
    clean_name = clean_name.strip(" .")

    return (clean_name, cat)


def main():
    spark = SparkSession.builder.appName("limpiezaDemografia_spark").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    print("📥 Cargando municipios de demografía...")

    df = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(MUNI_RAW)
    )

    print("🧹 Aplicando transformaciones iniciales...")

    df = df.withColumn("population", F.col("population").cast("double"))
    df = df.withColumn("year", F.col("year").cast("int"))
    df = df.filter(F.col("population").isNotNull())

    clean_udf = F.udf(clean_municipio_string_py, "struct<municipio_clean:string,categoria:string>")
    normalize_udf = F.udf(normalize_py, StringType())

    df = df.withColumn("cleaned", clean_udf(F.col("municipio")))
    df = df.withColumn("municipio_clean", F.col("cleaned.municipio_clean"))
    df = df.withColumn("categoria", F.col("cleaned.categoria"))
    df = df.drop("cleaned")
    df = df.withColumn("municipio_norm", normalize_udf(F.col("municipio_clean")))

    df_pivot = (
        df.groupBy("municipio_clean", "municipio_norm", "year")
        .pivot("categoria", ["Total", "Hombres", "Mujeres"])
        .sum("population")
        .fillna(0)
    )

    print("📚 Construyendo diccionarios de referencia...")

    df_migra_spark = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(MIGRACIONES_CSV)
    )

    df_migra = df_migra_spark.toPandas()
    df_migra["nom_norm"] = df_migra["nombre_municipio"].apply(normalize_py)
    master_mapping = (
        df_migra.drop_duplicates("nom_norm")
        .set_index("nom_norm")["codigo_provincia"]
        .to_dict()
    )
    print(f"ℹ️ Referencia Migraciones: {len(master_mapping)} municipios.")

    with open(GEOJSON, encoding="utf-8") as f:
        gj = json.load(f)

    geo_props = []
    for feat in gj.get("features", []):
        p = feat.get("properties", {})
        lid = str(p.get("LAU_ID", ""))
        if len(lid) >= 2:
            geo_props.append({
                "n": normalize_py(p.get("LAU_NAME", "")),
                "c": lid[:2]
            })

    mapping_geojson = (
        pd.DataFrame(geo_props)
        .drop_duplicates("n")
        .set_index("n")["c"]
        .to_dict()
    )

    print("🗺️ Asignando provincias...")

    pdf = df_pivot.toPandas()

    pdf["region_code"] = pdf["municipio_norm"].map(master_mapping)

    mask = pdf["region_code"].isna()
    pdf.loc[mask, "region_code"] = pdf.loc[mask, "municipio_norm"].map(mapping_geojson)

    mask = pdf["region_code"].isna()
    pdf.loc[mask, "region_code"] = pdf.loc[mask, "municipio_norm"].map(CORRECCIONES_MANUALES)

    mask = pdf["region_code"].isna()
    missing_names = pdf.loc[mask, "municipio_norm"].dropna().unique()

    if len(missing_names) > 0:
        all_refs = {**mapping_geojson, **master_mapping}
        choices = list(all_refs.keys())
        fuzzy_map = {}

        for name in missing_names:
            if not name:
                continue
            m = difflib.get_close_matches(name, choices, n=1, cutoff=0.75)
            if m:
                fuzzy_map[name] = all_refs[m[0]]

        pdf.loc[mask, "region_code"] = pdf.loc[mask, "municipio_norm"].map(fuzzy_map)

    pdf["region_code"] = pdf["region_code"].astype(str).str.replace(r"\.0$", "", regex=True).str.zfill(2)
    pdf.loc[pdf["region_code"].isin(["nan", "None", ""]), "region_code"] = pd.NA

    pdf = pdf.fillna("").astype(str)
    df_pivot_spark = spark.createDataFrame(pdf)

    print("🔗 Uniendo con población provincial...")

    df_p = pd.read_csv(PROV_CSV, dtype=str)
    df_p["region_code"] = df_p["region_code"].str.zfill(2)
    df_p["year"] = pd.to_numeric(df_p["year"], errors="coerce")
    df_p["population"] = pd.to_numeric(df_p["population"], errors="coerce")
    df_p = df_p.dropna(subset=["region_code", "year"])

    prov_names = df_p.drop_duplicates("region_code").set_index("region_code")["region_name"].to_dict()
    df_p_spark = spark.createDataFrame(df_p[["region_code", "year", "population"]])

    df_final = df_pivot_spark.join(
        df_p_spark,
        on=["region_code", "year"],
        how="left"
    )

    prov_map_expr = F.create_map([F.lit(x) for kv in prov_names.items() for x in kv])
    df_final = df_final.withColumn("region_name", prov_map_expr[F.col("region_code")])

    w = Window.partitionBy("region_code").orderBy("year").rowsBetween(Window.unboundedPreceding, 0)
    w2 = Window.partitionBy("region_code").orderBy(F.col("year").desc()).rowsBetween(Window.unboundedPreceding, 0)

    df_final = df_final.withColumn("population_ffill", F.last("population", ignorenulls=True).over(w))
    df_final = df_final.withColumn("population_filled", F.last("population_ffill", ignorenulls=True).over(w2))
    df_final = df_final.drop("population", "population_ffill")
    df_final = df_final.withColumnRenamed("population_filled", "provincia_population")
    df_final = df_final.withColumnRenamed("municipio_clean", "municipio")

    cols_finales = [
        "region_code", "region_name", "year", "provincia_population",
        "municipio", "Total", "Hombres", "Mujeres"
    ]

    df_export = df_final.select(*[c for c in cols_finales if c in df_final.columns])

    print("💾 Guardando resultado...")
    save_output(df_export, OUTPUT)

    print(f"📊 Filas finales: {df_export.count()}")
    print("🧾 Columnas finales:")
    print(df_export.columns)

    spark.stop()


if __name__ == "__main__":
    main()