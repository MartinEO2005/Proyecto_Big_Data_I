from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lpad, expr

def process_empresas(spark: SparkSession, input_raw_path: str, output_silver_path: str):
    print(f"📥 [SILVER] Procesando Empresas Logísticas desde: {input_raw_path}")
    
    df = spark.read.option("header", True).option("inferSchema", True).csv(input_raw_path)
    
    # 1. Identificar columnas de años dinámicamente
    cols_anios = [c for c in df.columns if c.isdigit()]
    
    if not cols_anios:
        print("⚠️ No se detectaron columnas de años. Revisa el formato RAW.")
        return

    # 2. Convertir de formato ANCHO a LARGO (Equivalente distribuido de pd.melt)
    stack_expr = f"stack({len(cols_anios)}, " + ", ".join([f"'{c}', `{c}`" for c in cols_anios]) + ") as (year, num_empresas)"
    
    df_long = df.select(
        lpad(col("codigo_municipio").cast("string"), 5, "0").alias("lau_id"),
        expr(stack_expr)
    )
    
    df_clean = df_long \
        .withColumn("year", col("year").cast("int")) \
        .withColumn("num_empresas", col("num_empresas").cast("double")) \
        .filter(col("lau_id").isNotNull() & col("year").isNotNull())
    
    df_clean.write \
        .mode("overwrite") \
        .partitionBy("year") \
        .parquet(output_silver_path)
        
    print(f"✅ [SILVER] Empresas Logísticas guardadas en Parquet: {output_silver_path}")