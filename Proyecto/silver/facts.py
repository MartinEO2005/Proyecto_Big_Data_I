"""
Generación de fact tables para Silver layer - Versión simplificada.
Evita UDFs complejas que causen crash en workers distribuidos.
"""

import logging
import os as _os
from pyspark.sql import SparkSession, functions as F, Window
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

logger = logging.getLogger(__name__)


def _local(path):
    """Prefija file:// a un path local para que Spark no lo resuelva como HDFS."""
    abs_path = _os.path.abspath(path)
    return f"file://{abs_path}"


# ==================== FACT_DEMOGRAFIA ====================

def create_fact_demografia(spark, raw_path, dim_muni_path, output_path):
    """Genera fact_demografia desde el CSV limpio.

    El CSV limpio tiene: region_code, region_name, year, provincia_population,
    municipio, Total, Hombres, Mujeres.
    Se hace JOIN con dim_municipio para obtener muni_id.
    Salida: muni_id, year, poblacion_total
    """
    logger.info("Generando fact_demografia...")

    try:
        # Intentar CSV limpio primero, luego raw como fallback
        clean_csv = _os.path.join(raw_path, "..", "clean", "demografia_municipios_final.csv")
        raw_csv   = _os.path.join(raw_path, "demografia", "demografia_poblacion_municipios.csv")

        if _os.path.exists(clean_csv):
            logger.info(f"Leyendo demographics clean CSV: {clean_csv}")
            demo = spark.read.csv(_local(clean_csv), header=True)
            # Columnas: region_code, region_name, year, provincia_population, municipio, Total, Hombres, Mujeres
            demo_prep = demo.select(
                F.col("region_code").alias("prov_id"),
                F.col("municipio").alias("muni_name"),
                F.col("year").cast(IntegerType()).alias("year"),
                F.col("Total").cast(DoubleType()).alias("poblacion_total")
            ).filter(F.col("prov_id").isNotNull() & F.col("muni_name").isNotNull())

            # JOIN con dim_municipio para obtener muni_id
            dim_muni = spark.read.parquet(dim_muni_path) \
                .select("muni_id", "muni_name", "prov_id")

            demo_clean = demo_prep.join(
                dim_muni,
                on=["prov_id", "muni_name"],
                how="left"
            ).select("muni_id", "year", "poblacion_total") \
             .filter(F.col("muni_id").isNotNull())

        else:
            logger.warning(f"Clean CSV no encontrado en {clean_csv}, usando raw")
            demo = spark.read.csv(_local(raw_csv), header=True)
            cols = demo.columns
            # Raw CSV: cod_prov, cod_muni, municipio, year, population
            id_col   = next((c for c in ["cod_muni", "muni_id", "LAU_ID"] if c in cols), None)
            year_col = next((c for c in ["year", "anio", "Anio"] if c in cols), None)
            val_col  = next((c for c in ["population", "Total", "poblacion"] if c in cols), None)

            if not all([id_col, year_col, val_col]):
                logger.warning(f"Columnas raw no encontradas: {cols}")
                demo.write.mode("overwrite").parquet(output_path)
                return demo

            demo_clean = demo.select(
                F.col(id_col).alias("muni_id"),
                F.col(year_col).cast(IntegerType()).alias("year"),
                F.col(val_col).cast(DoubleType()).alias("poblacion_total")
            ).filter(F.col("muni_id").isNotNull())

        demo_clean.write.mode("overwrite").parquet(output_path)
        count = demo_clean.count()
        logger.info(f"fact_demografia: {count} filas")
        return demo_clean

    except Exception as e:
        logger.error(f"Error en fact_demografia: {e}", exc_info=True)
        return spark.createDataFrame([], StructType([
            StructField("muni_id", StringType(), True),
            StructField("year", IntegerType(), True),
            StructField("poblacion_total", DoubleType(), True),
        ]))


# ==================== FACT_ENERGIA ====================

def create_fact_energia(spark, raw_path, dim_muni_path, output_path):
    """Genera fact_energia desde el CSV limpio (snapshot sin año).

    CSV limpio: nombre_provincia, Nombre (muni), Codigo (INE), Mediana consumo anual, ...
    Salida: muni_id, consumo_kwh_total  (snapshot — sin year)
    """
    logger.info("Generando fact_energia...")

    try:
        clean_csv = _os.path.join(raw_path, "..", "clean", "consumo_electrico_final_limpio.csv")

        if _os.path.exists(clean_csv):
            logger.info(f"Leyendo energia clean CSV: {clean_csv}")
            df = spark.read.csv(_local(clean_csv), header=True)
            cols = df.columns
            logger.info(f"fact_energia cols: {cols}")

            # Código INE sin cero inicial → zero-pad a 5 dígitos
            id_col  = next((c for c in ["Codigo", "codigo", "muni_id"] if c in cols), None)
            val_col = next((c for c in ["Mediana consumo anual", "consumo_kwh_total", "consumo"] if c in cols), None)

            if not id_col or not val_col:
                logger.warning(f"fact_energia: columnas clave no encontradas. Cols: {cols}")
                df.write.mode("overwrite").parquet(output_path)
                return df

            energia_clean = df.select(
                F.lpad(F.col(id_col).cast("int").cast("string"), 5, "0").alias("muni_id"),
                F.col(val_col).cast(DoubleType()).alias("consumo_kwh_total")
            ).filter(F.col("muni_id").isNotNull())

        else:
            logger.warning(f"Clean CSV de energía no encontrado en {clean_csv}, parquet vacío")
            energia_clean = spark.createDataFrame([], StructType([
                StructField("muni_id", StringType(), True),
                StructField("consumo_kwh_total", DoubleType(), True),
            ]))

        energia_clean.write.mode("overwrite").parquet(output_path)
        logger.info(f"fact_energia: {energia_clean.count()} filas")
        return energia_clean

    except Exception as e:
        logger.error(f"Error en fact_energia: {e}", exc_info=True)
        return spark.createDataFrame([], StructType([
            StructField("muni_id", StringType(), True),
            StructField("consumo_kwh_total", DoubleType(), True),
        ]))


# ==================== FACT_RENTA ====================

def create_fact_renta(spark, raw_path, dim_muni_path, dim_prov_path, output_path):
    """Versión simplificada de renta."""
    logger.info("Generando fact_renta...")
    
    try:
        # Buscar el CSV de renta (puede variar el nombre)
        import glob
        renta_files = glob.glob(f"{raw_path}/renta/*.csv")
        if not renta_files:
            logger.warning("No hay CSV de renta")
            return spark.createDataFrame([], StructType([]))
        
        renta = spark.read.csv(_local(renta_files[0]), header=True)
        cols = renta.columns
        logger.info(f"fact_renta cols: {cols}")
        
        # Mapear columnas reales
        id_col = next((c for c in ["codigo_municipio", "muni_id", "Codigo", "LAU_ID"] if c in cols), None)
        year_col = next((c for c in ["anio", "year", "Anio", "Year"] if c in cols), None)
        # "valor" = raw INE; "pib" = CSV limpio de renta
        val_col = next((c for c in ["valor", "renta_neta_media_euros", "Valor", "renta", "pib"] if c in cols), None)
        
        if not all([id_col, year_col, val_col]):
            logger.warning(f"fact_renta: no se encontraron columnas clave. Cols: {cols}")
            renta.write.mode("overwrite").parquet(output_path)
            return renta
        
        renta_clean = renta.select(
            F.col(id_col).alias("muni_id"),
            F.col(year_col).cast(IntegerType()).alias("year"),
            F.col(val_col).cast(DoubleType()).alias("renta_neta_media_euros")
        )
        
        renta_clean.write.mode("overwrite").parquet(output_path)
        logger.info(f"fact_renta: {renta_clean.count()} filas")
        return renta_clean
    
    except Exception as e:
        logger.error(f"Error en fact_renta: {e}")
        return spark.createDataFrame([], StructType([]))


# ==================== FACT_MIGRACION_NETA ====================

def create_fact_migracion_neta(spark, raw_path, dim_muni_path, output_path):
    """Versión simplificada de migración."""
    logger.info("Generando fact_migracion_neta...")
    
    try:
        import glob
        mig_files = glob.glob(f"{raw_path}/migracion/*.csv")
        if not mig_files:
            logger.warning("No hay CSV de migración")
            return spark.createDataFrame([], StructType([]))
        
        migracion = spark.read.csv(_local(mig_files[0]), header=True)
        cols = migracion.columns
        logger.info(f"fact_migracion cols: {cols}")
        
        id_col = next((c for c in ["codigo_municipio", "muni_id", "LAU_ID"] if c in cols), None)
        year_col = next((c for c in ["anio", "year", "Anio"] if c in cols), None)
        # "cantidad (personas)" es el nombre real del CSV de migraciones INE
        val_col = next((c for c in ["cantidad (personas)", "valor", "total", "num_migraciones"] if c in cols), None)
        
        if not all([id_col, year_col]):
            logger.warning(f"fact_migracion: columnas no encontradas. Cols: {cols}")
            migracion.write.mode("overwrite").parquet(output_path)
            return migracion
        
        # El CSV INE tiene una fila por (municipio, año, nacionalidad) — hay que agregar
        if val_col:
            migracion_clean = migracion.select(
                F.lpad(F.col(id_col).cast("string"), 5, "0").alias("muni_id"),
                F.col(year_col).cast(IntegerType()).alias("year"),
                F.col(val_col).cast(DoubleType()).alias("_val")
            ).groupBy("muni_id", "year").agg(
                F.sum("_val").cast(IntegerType()).alias("saldo_migratorio_neto")
            )
        else:
            migracion_clean = migracion.select(
                F.lpad(F.col(id_col).cast("string"), 5, "0").alias("muni_id"),
                F.col(year_col).cast(IntegerType()).alias("year")
            ).dropDuplicates(["muni_id", "year"])
        
        migracion_clean.write.mode("overwrite").parquet(output_path)
        logger.info(f"fact_migracion_neta: {migracion_clean.count()} filas")
        return migracion_clean
    
    except Exception as e:
        logger.error(f"Error en fact_migracion_neta: {e}")
        return spark.createDataFrame([], StructType([]))


# ==================== FACT_CONECTIVIDAD ====================

def create_fact_conectividad(spark, raw_path, dim_muni_path, output_path):
    """Versión simplificada de conectividad."""
    logger.info("Generando fact_conectividad...")
    
    try:
        conectividad = spark.read.csv(
            _local(f"{raw_path}/transporte/conectividad_municipal_2010_2025.csv"),
            header=True
        )
        cols = conectividad.columns
        logger.info(f"fact_conectividad cols: {cols}")
        
        # Cols reales: LAU_ID, LAU_NAME, Anio, Vehiculos_Oficial, Indice_Conectividad, Poblacion_Est
        id_col = next((c for c in ["LAU_ID", "muni_id", "codigo_municipio"] if c in cols), None)
        year_col = next((c for c in ["Anio", "anio", "year", "Year"] if c in cols), None)
        idx_col = next((c for c in ["Indice_Conectividad", "indice_conectividad"] if c in cols), None)
        veh_col = next((c for c in ["Vehiculos_Oficial", "num_vehiculos", "vehiculos"] if c in cols), None)
        
        select_cols = []
        if id_col: select_cols.append(F.col(id_col).alias("muni_id"))
        if year_col: select_cols.append(F.col(year_col).cast(IntegerType()).alias("year"))
        if idx_col: select_cols.append(F.col(idx_col).cast(DoubleType()).alias("indice_conectividad"))
        if veh_col: select_cols.append(F.col(veh_col).cast(IntegerType()).alias("num_vehiculos"))
        
        if not select_cols:
            conectividad.write.mode("overwrite").parquet(output_path)
            return conectividad
        
        conectividad_clean = conectividad.select(*select_cols)
        conectividad_clean.write.mode("overwrite").parquet(output_path)
        logger.info(f"fact_conectividad: {conectividad_clean.count()} filas")
        return conectividad_clean
    
    except Exception as e:
        logger.error(f"Error en fact_conectividad: {e}")
        return spark.createDataFrame([], StructType([]))


# ==================== FACT_EMPRESAS_TRANSPORTE ====================

def create_fact_empresas_transporte(spark, raw_path, dim_muni_path, output_path):
    """Versión simplificada de empresas transporte."""
    logger.info("Generando fact_empresas_transporte...")
    
    try:
        empresas = spark.read.csv(
            _local(f"{raw_path}/empresas_transporte/empresas_transporte_prov_mun_anchos.csv"),
            header=True
        )
        cols = empresas.columns
        logger.info(f"fact_empresas cols: {cols}")
        
        # Cols reales: codigo, nombre, tipo, 2012, 2013, ..., 2025
        id_col = next((c for c in ["codigo", "muni_id", "LAU_ID", "codigo_municipio"] if c in cols), None)
        year_cols = [c for c in cols if c.isdigit()]
        
        if not id_col or not year_cols:
            empresas.write.mode("overwrite").parquet(output_path)
            return empresas
        
        # Unpivot: una fila por municipio-año
        from functools import reduce
        from pyspark.sql import DataFrame
        rows = []
        for year in year_cols:
            rows.append(
                empresas.select(
                    F.lpad(F.col(id_col).cast("string"), 5, "0").alias("muni_id"),
                    F.lit(int(year)).cast(IntegerType()).alias("year"),
                    F.col(year).cast(IntegerType()).alias("num_empresas_total")
                )
            )
        empresas_clean = reduce(DataFrame.unionAll, rows)
        
        empresas_clean.write.mode("overwrite").parquet(output_path)
        logger.info(f"fact_empresas_transporte: {empresas_clean.count()} filas")
        return empresas_clean
    
    except Exception as e:
        logger.error(f"Error en fact_empresas_transporte: {e}")
        return spark.createDataFrame([], StructType([]))


# ==================== FACT_OSM_LOGISTICA ====================

def create_fact_osm_logistica(spark, raw_path, dim_muni_path, output_path):
    """Versión simplificada de OSM logística."""
    logger.info("Generando fact_osm_logistica...")
    
    try:
        osm = spark.read.csv(
            _local(f"{raw_path}/transporte/muni_station_metrics_reduced.csv"),
            header=True
        )
        
        cols = osm.columns
        id_col = next((c for c in ["LAU_ID", "muni_id", "municipio_id"] if c in cols), None)
        if id_col is None:
            raise ValueError(f"No se encontró columna de ID en fact_osm. Columnas: {cols}")

        osm_clean = osm.select(
            F.col(id_col).alias("muni_id"),
            F.col("stations_count").cast(IntegerType()).alias("num_estaciones"),
            F.col("operator_count").cast(IntegerType()).alias("num_operadores"),
            F.col("accessible_count").cast(IntegerType()).alias("estaciones_accesibles"),
            F.col("min_distance_km_to_station").cast(DoubleType()).alias("distancia_min_km"),
            F.col("mean_distance_km_to_station").cast(DoubleType()).alias("distancia_media_km"),
            F.col("stations_density_km2").cast(DoubleType()).alias("densidad_estaciones_km2"),
            F.col("accessible_share").cast(DoubleType()).alias("ratio_accesibilidad")
        )
        
        osm_clean.write.mode("overwrite").parquet(output_path)
        logger.info(f"fact_osm_logistica: {osm_clean.count()} filas")
        return osm_clean
    
    except Exception as e:
        logger.error(f"Error en fact_osm_logistica: {e}")
        return spark.createDataFrame([], StructType([]))


# ==================== FACT_VIIRS ====================

def create_fact_viirs(spark, raw_path, dim_muni_path, output_path):
    """Genera fact_viirs desde el CSV limpio (viirsFinal_limpio.csv).

    CSV limpio: PROV_NAME, date (YYYY-MM), mean_prov, AREA_KM2, CNTR_CODE,
                GISCO_ID, LAU_ID, LAU_NAME, max, mean, min, stdDev
    Salida: muni_id, year, month, radiancia_media, radiancia_max,
            radiancia_min, radiancia_stddev
    """
    logger.info("Generando fact_viirs...")

    clean_csv = _os.path.join(raw_path, "..", "clean", "viirsFinal_limpio.csv")
    raw_csv   = _os.path.join(raw_path, "luz_nocturna", "viirs_luz_nocturna.csv")

    try:
        if _os.path.exists(clean_csv):
            logger.info(f"Leyendo VIIRS clean CSV: {clean_csv}")
            viirs = spark.read.csv(_local(clean_csv), header=True)
            cols  = viirs.columns
            logger.info(f"fact_viirs cols: {cols}")

            id_col   = next((c for c in ["LAU_ID", "muni_id"] if c in cols), None)
            date_col = next((c for c in ["date"] if c in cols), None)

            if not id_col or not date_col:
                raise ValueError(f"Columnas clave no encontradas: {cols}")

            # date tiene formato YYYY-MM  →  extraer year y month
            viirs_clean = viirs.select(
                F.lpad(F.col(id_col).cast("string"), 5, "0").alias("muni_id"),
                F.substring(F.col(date_col), 1, 4).cast(IntegerType()).alias("year"),
                F.substring(F.col(date_col), 6, 2).cast(IntegerType()).alias("month"),
                F.col("mean").cast(DoubleType()).alias("radiancia_media"),
                F.col("max").cast(DoubleType()).alias("radiancia_max"),
                F.col("min").cast(DoubleType()).alias("radiancia_min"),
                F.col("stdDev").cast(DoubleType()).alias("radiancia_stddev"),
            ).filter(F.col("muni_id").isNotNull())

        elif _os.path.exists(raw_csv):
            logger.info(f"Leyendo VIIRS raw CSV: {raw_csv}")
            viirs = spark.read.csv(_local(raw_csv), header=True)
            viirs_clean = viirs.select(
                F.lpad(F.col("muni_id").cast("string"), 5, "0").alias("muni_id"),
                F.col("year").cast(IntegerType()),
                F.col("month").cast(IntegerType()),
                F.col("radiancia_media").cast(DoubleType()),
            ).filter(F.col("muni_id").isNotNull())
        else:
            logger.warning("VIIRS: no se encontró ningún CSV — se omite")
            return spark.createDataFrame([], StructType([
                StructField("muni_id",        StringType(),  True),
                StructField("year",           IntegerType(), True),
                StructField("month",          IntegerType(), True),
                StructField("radiancia_media",DoubleType(),  True),
            ]))

        viirs_clean.write.mode("overwrite").parquet(output_path)
        logger.info(f"fact_viirs: {viirs_clean.count()} filas")
        return viirs_clean

    except Exception as e:
        logger.error(f"Error en fact_viirs: {e}", exc_info=True)
        return spark.createDataFrame([], StructType([
            StructField("muni_id",        StringType(),  True),
            StructField("year",           IntegerType(), True),
            StructField("month",          IntegerType(), True),
            StructField("radiancia_media",DoubleType(),  True),
        ]))


# ==================== MAIN ====================

def main_facts(spark, raw_base_path, dim_base_path, fact_base_path):
    """Orquestador: genera todas las fact tables."""
    
    logger.info("="*60)
    logger.info("INICIANDO GENERACIÓN DE FACT TABLES (Silver)")
    logger.info("="*60)
    
    dim_muni_path = f"{dim_base_path}/dim_municipio.parquet"
    dim_prov_path = f"{dim_base_path}/dim_provincia.parquet"
    
    facts = {}
    
    # Generar todas las facts
    facts['demografia'] = create_fact_demografia(
        spark, raw_base_path, dim_muni_path,
        f"{fact_base_path}/fact_demografia.parquet"
    )
    
    facts['energia'] = create_fact_energia(
        spark, raw_base_path, dim_muni_path,
        f"{fact_base_path}/fact_energia.parquet"
    )
    
    facts['renta'] = create_fact_renta(
        spark, raw_base_path, dim_muni_path, dim_prov_path,
        f"{fact_base_path}/fact_renta.parquet"
    )
    
    facts['migracion_neta'] = create_fact_migracion_neta(
        spark, raw_base_path, dim_muni_path,
        f"{fact_base_path}/fact_migracion_neta.parquet"
    )
    
    facts['conectividad'] = create_fact_conectividad(
        spark, raw_base_path, dim_muni_path,
        f"{fact_base_path}/fact_conectividad.parquet"
    )
    
    facts['empresas_transporte'] = create_fact_empresas_transporte(
        spark, raw_base_path, dim_muni_path,
        f"{fact_base_path}/fact_empresas_transporte.parquet"
    )
    
    facts['osm_logistica'] = create_fact_osm_logistica(
        spark, raw_base_path, dim_muni_path,
        f"{fact_base_path}/fact_osm_logistica.parquet"
    )
    
    facts['viirs'] = create_fact_viirs(
        spark, raw_base_path, dim_muni_path,
        f"{fact_base_path}/fact_viirs.parquet"
    )
    
    logger.info("="*60)
    logger.info("GENERACIÓN DE FACT TABLES COMPLETADA")
    logger.info("="*60)
    
    return facts
