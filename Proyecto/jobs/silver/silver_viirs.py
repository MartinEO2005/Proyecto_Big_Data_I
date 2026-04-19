from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lpad

def process_viirs(spark: SparkSession, input_raw_path: str, output_silver_path: str):
    print(f"📥 [SILVER] Procesando Luces Nocturnas (VIIRS) desde: {input_raw_path}")
    
    df = spark.read.option("header", True).option("inferSchema", True).csv(input_raw_path)
    
    df_clean = df \
        .withColumn("lau_id", lpad(col("lau_id").cast("string"), 5, "0")) \
        .withColumn("mean_rad", col("mean").cast("double")) \
        .withColumn("sum_rad", col("sum").cast("double")) \
        .withColumn("year", col("year").cast("int"))
        
    df_clean = df_clean.filter(col("lau_id").isNotNull() & col("mean_rad").isNotNull())
    
    cols_plata = ["lau_id", "mean_rad", "sum_rad", "year"]
    
    df_clean.select(*cols_plata).write \
        .mode("overwrite") \
        .partitionBy("year") \
        .parquet(output_silver_path)
        
    print(f"✅ [SILVER] VIIRS guardado en Parquet: {output_silver_path}")