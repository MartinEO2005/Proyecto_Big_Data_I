from pyspark.sql import SparkSession

def build_master_features(spark: SparkSession, silver_paths: dict, output_gold_path: str):
    print("🥇 [GOLD] Ensamblando la matriz de características (Feature Store)...")
    
    # 1. Leer los Parquet de la capa Silver
    df_demo = spark.read.parquet(silver_paths["demografia"])
    df_viirs = spark.read.parquet(silver_paths["viirs"])
    
    # 2. Cruce (Join) distribuido
    # Unimos demografía y luces usando lau_id y year como claves maestras
    df_master = df_demo.join(
        df_viirs,
        on=["lau_id", "year"],
        how="left" # Mantenemos todos los municipios aunque no tengan luz un año
    )
    
    # Aquí puedes añadir cruces para rentas, OSM, transporte, etc.
    # df_renta = spark.read.parquet(silver_paths["renta"])
    # df_master = df_master.join(df_renta, on=["lau_id", "year"], how="left")
    
    # 3. Rellenar nulos matemáticos (evita que K-Means explote luego)
    df_master = df_master.fillna({
        "poblacion": 0.0,
        "mean_rad": 0.0,
        "sum_rad": 0.0
    })
    
    # 4. Guardar la matriz aplanada en Parquet
    df_master.write \
        .mode("overwrite") \
        .parquet(output_gold_path)
        
    print(f"✅ [GOLD] Matriz maestra creada y guardada en: {output_gold_path}")