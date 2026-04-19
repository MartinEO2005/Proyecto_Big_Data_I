from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lpad, trim

def process_renta(spark: SparkSession, input_path: str, output_path: str):
    df = spark.read.option("header", True).option("inferSchema", True).csv(input_path)
    df_clean = df \
        .withColumn("lau_id", lpad(col("codigo_municipio").cast("string"), 5, "0")) \
        .withColumn("year", col("anio").cast("int")) \
        .withColumn("pib", col("valor").cast("double")) \
        .filter(col("lau_id").isNotNull() & col("pib").isNotNull())
        
    df_clean.select("lau_id", "year", "pib").write.mode("overwrite").partitionBy("year").parquet(output_path)

def process_migracion(spark: SparkSession, input_path: str, output_path: str):
    df = spark.read.option("header", True).option("inferSchema", True).csv(input_path)
    df_clean = df \
        .withColumn("lau_id", lpad(col("codigo_municipio").cast("string"), 5, "0")) \
        .withColumn("year", col("anio").cast("int")) \
        .withColumn("cantidad_personas", col("valor").cast("double")) \
        .filter(col("lau_id").isNotNull())
        
    df_clean.select("lau_id", "year", "cantidad_personas").write.mode("overwrite").partitionBy("year").parquet(output_path)

def process_consumo(spark: SparkSession, input_path: str, output_path: str):
    # El consumo en tu script usaba separador ';'
    df = spark.read.option("header", True).option("sep", ";").option("inferSchema", True).csv(input_path)
    df_clean = df \
        .withColumn("lau_id", lpad(trim(col("Codigo").cast("string")), 5, "0")) \
        .withColumn("consumo_total", col("Total").cast("double")) \
        .filter(col("lau_id").isNotNull())
        
    # Nota: Si el consumo no tiene año, no particionamos
    df_clean.select("lau_id", "consumo_total").write.mode("overwrite").parquet(output_path)

def run_socioeconomico(spark: SparkSession, raw_paths: dict, silver_paths: dict):
    print("📥 [SILVER] Procesando bloque Socioeconómico...")
    process_renta(spark, raw_paths["renta"], silver_paths["renta"])
    process_migracion(spark, raw_paths["migracion"], silver_paths["migracion"])
    process_consumo(spark, raw_paths["consumo"], silver_paths["consumo"])
    print("✅ [SILVER] Renta, Migración y Consumo guardados en Parquet.")