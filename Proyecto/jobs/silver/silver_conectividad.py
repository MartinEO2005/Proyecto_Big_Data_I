from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lpad

def process_conectividad(spark: SparkSession, input_raw_path: str, output_silver_path: str):
    print(f"📥 [SILVER] Procesando Conectividad desde: {input_raw_path}")
    
    df = spark.read.option("header", True).option("inferSchema", True).csv(input_raw_path)
    
    df_clean = df \
        .withColumn("lau_id", lpad(col("LAU_ID").cast("string"), 5, "0")) \
        .withColumn("year", col("Anio").cast("int")) \
        .withColumn("vehiculos_oficial", col("Vehiculos_Oficial").cast("double")) \
        .withColumn("indice_conectividad", col("Indice_Conectividad").cast("double"))
        
    df_clean = df_clean.filter(col("lau_id").isNotNull() & col("year").isNotNull())
    
    cols_plata = ["lau_id", "year", "vehiculos_oficial", "indice_conectividad"]
    
    df_clean.select(*cols_plata).write \
        .mode("overwrite") \
        .partitionBy("year") \
        .parquet(output_silver_path)
        
    print(f"✅ [SILVER] Conectividad guardada en Parquet: {output_silver_path}")