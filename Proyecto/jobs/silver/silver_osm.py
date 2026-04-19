from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lpad, lower, trim, regexp_replace

def process_osm(spark: SparkSession, input_raw_path: str, output_silver_path: str):
    print(f"📥 [SILVER] Procesando Transporte (OSM) desde: {input_raw_path}")
    
    df = spark.read.option("header", True).option("inferSchema", True).csv(input_raw_path)
    
    df_clean = df \
        .withColumn("lau_id", lpad(col("LAU_ID").cast("string"), 5, "0")) \
        .withColumn("nombre_municipio", trim(lower(col("LAU_NAME")))) \
        .withColumn("num_estaciones", col("stations_count").cast("int")) \
        .withColumn("densidad_estaciones_km2", col("stations_density_km2").cast("double"))
        
    df_clean = df_clean.filter(col("lau_id").isNotNull())
    
    # Seleccionamos las columnas clave para el clustering
    cols_plata = ["lau_id", "nombre_municipio", "num_estaciones", "densidad_estaciones_km2", "accessible_share"]
    
    df_clean.select(*[col(c) for c in cols_plata if c in df_clean.columns]).write \
        .mode("overwrite") \
        .parquet(output_silver_path)
        
    print(f"✅ [SILVER] OSM guardado en Parquet: {output_silver_path}")