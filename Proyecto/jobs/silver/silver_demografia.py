from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, lower, lpad, regexp_replace

def process_demography(spark: SparkSession, input_raw_path: str, output_silver_path: str):
    print(f"📥 [SILVER] Procesando Demografía desde: {input_raw_path}")
    
    # 1. Lectura
    df = spark.read.option("header", True).option("inferSchema", True).csv(input_raw_path)
    
    # 2. Limpieza (Equivalente a tu Feature Engineering básico, pero en Spark)
    df_clean = df \
        .withColumn("lau_id", lpad(col("codigo_municipio").cast("string"), 5, "0")) \
        .withColumn("provincia_id", lpad(col("codigo_provincia").cast("string"), 2, "0")) \
        .withColumn("nombre_municipio", trim(lower(col("nombre_municipio")))) \
        .withColumn("poblacion", col("poblacion").cast("double")) \
        .withColumn("year", col("anio").cast("int"))

    # Filtro de nulos en identificadores clave
    df_clean = df_clean.filter(col("lau_id").isNotNull())

    # Selección final
    cols_plata = ["lau_id", "provincia_id", "nombre_municipio", "poblacion", "year"]
    df_silver = df_clean.select(*cols_plata)
    
    # 3. Escritura particionada por año en formato Parquet
    df_silver.write \
        .mode("overwrite") \
        .partitionBy("year") \
        .parquet(output_silver_path)
        
    print(f"✅ [SILVER] Demografía guardada en Parquet: {output_silver_path}")