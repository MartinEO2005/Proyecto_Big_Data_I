"""
Generación de fact tables para Silver layer - Versión simplificada.
Evita UDFs complejas que causen crash en workers distribuidos.
"""

import glob
import logging
import os as _os
from functools import reduce
from pyspark.sql import DataFrame, functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

logger = logging.getLogger(__name__)


def _local(path):
    """Prefija file:// a un path local para que Spark no lo resuelva como HDFS."""
    abs_path = _os.path.abspath(path)
    return f"file://{abs_path}"


# ==================== FACT_DEMOGRAFIA ====================

def create_fact_demografia(spark, raw_path, dim_muni_path, output_path):
    """Genera fact_demografia desde el CSV del INE.

    El CSV tiene: municipio (nombre limpio), year, population
    Se hace JOIN con dim_municipio por nombre normalizado para obtener muni_id.
    Salida: muni_id, year, poblacion_total
    """
    logger.info("Generando fact_demografia...")

    try:
        raw_csv = _os.path.join(raw_path, "demografia", "demografia_poblacion_municipios.csv")
        if not _os.path.exists(raw_csv):
            logger.warning(f"fact_demografia: CSV no encontrado en {raw_csv}")
            return spark.createDataFrame([], StructType([
                StructField("muni_id", StringType(), True),
                StructField("year", IntegerType(), True),
                StructField("poblacion_total", DoubleType(), True),
            ]))

        demo = spark.read.csv(_local(raw_csv), header=True)
        cols = demo.columns
        logger.info(f"fact_demografia cols: {cols}")

        year_col = next((c for c in ["year", "anio", "Anio"] if c in cols), None)
        val_col  = next((c for c in ["population", "Total", "poblacion"] if c in cols), None)
        name_col = next((c for c in ["municipio", "muni_name", "nombre"] if c in cols), None)
        id_col   = next((c for c in ["cod_muni", "muni_id", "LAU_ID"] if c in cols), None)

        if not all([year_col, val_col]):
            logger.warning(f"fact_demografia: columnas year/val no encontradas. Cols: {cols}")
            return spark.createDataFrame([], StructType([
                StructField("muni_id", StringType(), True),
                StructField("year", IntegerType(), True),
                StructField("poblacion_total", DoubleType(), True),
            ]))

        # Caso 1: tenemos cod_muni directamente
        if id_col:
            demo_clean = demo.select(
                F.lpad(F.col(id_col).cast("string"), 5, "0").alias("muni_id"),
                F.col(year_col).cast(IntegerType()).alias("year"),
                F.col(val_col).cast(DoubleType()).alias("poblacion_total")
            ).filter(F.col("muni_id").isNotNull())

        # Caso 2: solo nombre → JOIN con dim_municipio por nombre normalizado
        elif name_col:
            dim_muni = spark.read.parquet(dim_muni_path)
            demo_norm = demo.withColumn(
                "_muni_norm", F.lower(F.trim(F.col(name_col)))
            )
            dim_norm = dim_muni.withColumn(
                "_muni_norm", F.lower(F.trim(F.col("muni_name")))
            ).select("muni_id", "_muni_norm")
            demo_clean = demo_norm.join(dim_norm, on="_muni_norm", how="inner") \
                .select(
                    "muni_id",
                    F.col(year_col).cast(IntegerType()).alias("year"),
                    F.col(val_col).cast(DoubleType()).alias("poblacion_total")
                ).filter(F.col("muni_id").isNotNull()) \
                .groupBy("muni_id", "year") \
                .agg(F.max("poblacion_total").alias("poblacion_total"))
        else:
            logger.warning("fact_demografia: sin cod_muni ni dim_municipio disponible")
            return spark.createDataFrame([], StructType([
                StructField("muni_id", StringType(), True),
                StructField("year", IntegerType(), True),
                StructField("poblacion_total", DoubleType(), True),
            ]))

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

def create_fact_energia(spark, raw_path, _dim_muni_path, output_path):
    """Genera fact_energia desde el CSV limpio (snapshot sin año).

    CSV limpio: nombre_provincia, Nombre (muni), Codigo (INE), Mediana consumo anual, ...
    Salida: muni_id, consumo_kwh_total  (snapshot — sin year)
    """
    logger.info("Generando fact_energia...")

    try:
        raw_csv = _os.path.join(raw_path, "energia", "consumo_electrico.csv")
        if not _os.path.exists(raw_csv):
            logger.warning(f"fact_energia: CSV no encontrado en {raw_csv}")
            return spark.createDataFrame([], StructType([
                StructField("muni_id", StringType(), True),
                StructField("consumo_kwh_total", DoubleType(), True),
            ]))

        df = spark.read.csv(_local(raw_csv), header=True)
        cols = df.columns
        logger.info(f"fact_energia cols: {cols}")

        id_col   = next((c for c in ["Codigo", "codigo", "muni_id", "LAU_ID"] if c in cols), None)
        cat_col  = next((c for c in ["Consumo eléctrico", "categoria", "tipo"] if c in cols), None)
        val_col  = next((c for c in ["Total", "consumo_kwh_total", "consumo", "valor"] if c in cols), None)

        if not id_col or not val_col:
            logger.warning(f"fact_energia: columnas clave no encontradas. Cols: {cols}")
            empty = spark.createDataFrame([], StructType([
                StructField("muni_id", StringType(), True),
                StructField("consumo_kwh_total", DoubleType(), True),
            ]))
            empty.write.mode("overwrite").parquet(output_path)
            return empty

        # Filtrar solo "Mediana consumo anual" → 1 fila por municipio (evita fan-out en Gold)
        # Además filtrar solo códigos de 5 dígitos (municipios, no provincias)
        df_filtered = df
        if cat_col:
            df_filtered = df_filtered.filter(F.col(cat_col) == "Mediana consumo anual")

        # Filtrar códigos provinciales (< 4 dígitos) ANTES del lpad
        energia_clean = df_filtered.filter(
            F.col(id_col).isNotNull() &
            (F.length(F.trim(F.col(id_col).cast("string"))) >= 4)
        ).select(
            F.lpad(F.col(id_col).cast("string"), 5, "0").alias("muni_id"),
            F.col(val_col).cast(DoubleType()).alias("consumo_kwh_total")
        ).filter(F.col("muni_id").isNotNull())

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

def create_fact_renta(spark, raw_path, _dim_muni_path, output_path):
    """Versión simplificada de renta."""
    logger.info("Generando fact_renta...")
    
    try:
        # Buscar el CSV de renta (puede variar el nombre)
        renta_files = sorted(glob.glob(f"{raw_path}/renta/*.csv"))
        if not renta_files:
            logger.warning("No hay CSV de renta")
            return spark.createDataFrame([], StructType([
                StructField("muni_id", StringType(), True),
                StructField("year", IntegerType(), True),
                StructField("renta_neta_media_euros", DoubleType(), True),
            ]))
        
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
            empty = spark.createDataFrame([], StructType([
                StructField("muni_id", StringType(), True),
                StructField("year", IntegerType(), True),
                StructField("renta_neta_media_euros", DoubleType(), True),
            ]))
            empty.write.mode("overwrite").parquet(output_path)
            return empty
        
        renta_clean = renta.select(
            F.lpad(F.col(id_col).cast("string"), 5, "0").alias("muni_id"),
            F.col(year_col).cast(IntegerType()).alias("year"),
            F.col(val_col).cast(DoubleType()).alias("renta_neta_media_euros")
        ).filter(F.col("muni_id").isNotNull())
        
        renta_clean.write.mode("overwrite").parquet(output_path)
        logger.info(f"fact_renta: {renta_clean.count()} filas")
        return renta_clean
    
    except Exception as e:
        logger.error(f"Error en fact_renta: {e}")
        return spark.createDataFrame([], StructType([
            StructField("muni_id", StringType(), True),
            StructField("year", IntegerType(), True),
            StructField("renta_neta_media_euros", DoubleType(), True),
        ]))


# ==================== FACT_MIGRACION_NETA ====================

def create_fact_migracion_neta(spark, raw_path, _dim_muni_path, output_path):
    """Versión simplificada de migración."""
    logger.info("Generando fact_migracion_neta...")
    
    try:
        mig_files = sorted(glob.glob(f"{raw_path}/migracion/*.csv"))
        if not mig_files:
            logger.warning("No hay CSV de migración")
            return spark.createDataFrame([], StructType([
                StructField("muni_id", StringType(), True),
                StructField("year", IntegerType(), True),
                StructField("saldo_migratorio_neto", IntegerType(), True),
            ]))
        
        migracion = spark.read.csv(_local(mig_files[0]), header=True)
        cols = migracion.columns
        logger.info(f"fact_migracion cols: {cols}")
        
        id_col = next((c for c in ["codigo_municipio", "muni_id", "LAU_ID"] if c in cols), None)
        year_col = next((c for c in ["anio", "year", "Anio"] if c in cols), None)
        # "cantidad (personas)" es el nombre real del CSV de migraciones INE
        val_col = next((c for c in ["cantidad (personas)", "valor", "total", "num_migraciones"] if c in cols), None)
        
        if not all([id_col, year_col]):
            logger.warning(f"fact_migracion: columnas no encontradas. Cols: {cols}")
            empty = spark.createDataFrame([], StructType([
                StructField("muni_id", StringType(), True),
                StructField("year", IntegerType(), True),
                StructField("saldo_migratorio_neto", IntegerType(), True),
            ]))
            empty.write.mode("overwrite").parquet(output_path)
            return empty
        
        # El CSV INE tiene una fila por (municipio, año, sexo, nacionalidad).
        # Filtramos a la fila agregada (Ambos sexos / Total) para evitar doble conteo.
        sexo_col = next((c for c in ["sexo"] if c in cols), None)
        nac_col  = next((c for c in ["nacionalidad"] if c in cols), None)
        if val_col:
            df_mig = migracion
            if sexo_col:
                df_mig = df_mig.filter(F.col(sexo_col) == "Ambos sexos")
            if nac_col:
                df_mig = df_mig.filter(F.col(nac_col) == "Total")
            migracion_clean = df_mig.select(
                F.lpad(F.col(id_col).cast("string"), 5, "0").alias("muni_id"),
                F.col(year_col).cast(IntegerType()).alias("year"),
                F.col(val_col).cast(DoubleType()).alias("_val")
            ).filter(F.col("muni_id").isNotNull()).groupBy("muni_id", "year").agg(
                F.sum("_val").cast(IntegerType()).alias("saldo_migratorio_neto")
            )
        else:
            migracion_clean = migracion.select(
                F.lpad(F.col(id_col).cast("string"), 5, "0").alias("muni_id"),
                F.col(year_col).cast(IntegerType()).alias("year")
            ).filter(F.col("muni_id").isNotNull()).dropDuplicates(["muni_id", "year"])
        
        migracion_clean.write.mode("overwrite").parquet(output_path)
        logger.info(f"fact_migracion_neta: {migracion_clean.count()} filas")
        return migracion_clean
    
    except Exception as e:
        logger.error(f"Error en fact_migracion_neta: {e}")
        return spark.createDataFrame([], StructType([
            StructField("muni_id", StringType(), True),
            StructField("year", IntegerType(), True),
            StructField("saldo_migratorio_neto", IntegerType(), True),
        ]))


# ==================== FACT_CONECTIVIDAD ====================

def create_fact_conectividad(spark, raw_path, _dim_muni_path, output_path):
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
        if id_col: select_cols.append(F.lpad(F.col(id_col).cast("string"), 5, "0").alias("muni_id"))
        if year_col: select_cols.append(F.col(year_col).cast(IntegerType()).alias("year"))
        if idx_col: select_cols.append(F.col(idx_col).cast(DoubleType()).alias("indice_conectividad"))
        if veh_col: select_cols.append(F.col(veh_col).cast(IntegerType()).alias("num_vehiculos"))
        
        if not id_col or not year_col:
            logger.warning(f"fact_conectividad: columnas clave (id o year) no encontradas. Cols: {cols}")
            empty = spark.createDataFrame([], StructType([
                StructField("muni_id", StringType(), True),
                StructField("year", IntegerType(), True),
                StructField("indice_conectividad", DoubleType(), True),
                StructField("num_vehiculos", IntegerType(), True),
            ]))
            empty.write.mode("overwrite").parquet(output_path)
            return empty
        
        conectividad_clean = conectividad.select(*select_cols).filter(
            F.col("muni_id").isNotNull()
        )
        conectividad_clean.write.mode("overwrite").parquet(output_path)
        logger.info(f"fact_conectividad: {conectividad_clean.count()} filas")
        return conectividad_clean
    
    except Exception as e:
        logger.error(f"Error en fact_conectividad: {e}")
        return spark.createDataFrame([], StructType([
            StructField("muni_id", StringType(), True),
            StructField("year", IntegerType(), True),
            StructField("indice_conectividad", DoubleType(), True),
            StructField("num_vehiculos", IntegerType(), True),
        ]))


# ==================== FACT_EMPRESAS_TRANSPORTE ====================

def create_fact_empresas_transporte(spark, raw_path, _dim_muni_path, output_path):
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
        id_col   = next((c for c in ["codigo", "muni_id", "LAU_ID", "codigo_municipio"] if c in cols), None)
        tipo_col = next((c for c in ["tipo"] if c in cols), None)
        year_cols = [c for c in cols if c.isdigit()]

        # Excluir filas de provincia (tipo != "municipio")
        if tipo_col:
            empresas = empresas.filter(F.col(tipo_col) == "municipio")
        
        if not id_col or not year_cols:
            logger.warning(f"fact_empresas_transporte: columnas clave no encontradas. Cols: {cols}")
            empty = spark.createDataFrame([], StructType([
                StructField("muni_id", StringType(), True),
                StructField("year", IntegerType(), True),
                StructField("num_empresas_total", IntegerType(), True),
            ]))
            empty.write.mode("overwrite").parquet(output_path)
            return empty
        
        # Unpivot: una fila por municipio-año
        rows = []
        for year in year_cols:
            rows.append(
                empresas.select(
                    F.lpad(F.col(id_col).cast("string"), 5, "0").alias("muni_id"),
                    F.lit(int(year)).cast(IntegerType()).alias("year"),
                    F.col(year).cast(IntegerType()).alias("num_empresas_total")
                )
            )
        empresas_clean = reduce(DataFrame.unionAll, rows).filter(
            F.col("muni_id").isNotNull()
        )
        
        empresas_clean.write.mode("overwrite").parquet(output_path)
        logger.info(f"fact_empresas_transporte: {empresas_clean.count()} filas")
        return empresas_clean
    
    except Exception as e:
        logger.error(f"Error en fact_empresas_transporte: {e}")
        return spark.createDataFrame([], StructType([
            StructField("muni_id", StringType(), True),
            StructField("year", IntegerType(), True),
            StructField("num_empresas_total", IntegerType(), True),
        ]))


# ==================== FACT_OSM_LOGISTICA ====================

def create_fact_osm_logistica(spark, raw_path, _dim_muni_path, output_path):
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
            F.lpad(F.col(id_col).cast("string"), 5, "0").alias("muni_id"),
            F.col("stations_count").cast(IntegerType()).alias("num_estaciones"),
            F.col("operator_count").cast(IntegerType()).alias("num_operadores"),
            F.col("min_distance_km_to_station").cast(DoubleType()).alias("distancia_min_km"),
            F.col("mean_distance_km_to_station").cast(DoubleType()).alias("distancia_media_km"),
            F.col("stations_density_km2").cast(DoubleType()).alias("densidad_estaciones_km2"),
            F.col("accessible_share").cast(DoubleType()).alias("ratio_accesibilidad")
        ).filter(F.col("muni_id").isNotNull())
        
        osm_clean.write.mode("overwrite").parquet(output_path)
        logger.info(f"fact_osm_logistica: {osm_clean.count()} filas")
        return osm_clean
    
    except Exception as e:
        logger.error(f"Error en fact_osm_logistica: {e}")
        return spark.createDataFrame([], StructType([
            StructField("muni_id", StringType(), True),
            StructField("num_estaciones", IntegerType(), True),
            StructField("num_operadores", IntegerType(), True),
            StructField("distancia_min_km", DoubleType(), True),
            StructField("distancia_media_km", DoubleType(), True),
            StructField("densidad_estaciones_km2", DoubleType(), True),
            StructField("ratio_accesibilidad", DoubleType(), True),
        ]))


# ==================== FACT_VIIRS ====================

def create_fact_viirs(spark, raw_path, _dim_muni_path, output_path):
    """Genera fact_viirs desde el CSV limpio (viirsFinal_limpio.csv).

    CSV limpio: PROV_NAME, date (YYYY-MM), mean_prov, AREA_KM2, CNTR_CODE,
                GISCO_ID, LAU_ID, LAU_NAME, max, mean, min, stdDev
    Salida: muni_id, year, month, radiancia_media, radiancia_max,
            radiancia_min, radiancia_stddev
    """
    logger.info("Generando fact_viirs...")

    raw_csv = _os.path.join(raw_path, "luz_nocturna", "viirs_luz_nocturna.csv")

    try:
        if not _os.path.exists(raw_csv):
            logger.warning(f"VIIRS: CSV no encontrado en {raw_csv} — se omite")
            return spark.createDataFrame([], StructType([
                StructField("muni_id",        StringType(),  True),
                StructField("year",           IntegerType(), True),
                StructField("month",          IntegerType(), True),
                StructField("radiancia_media",DoubleType(),  True),
            ]))

        logger.info(f"Leyendo VIIRS raw CSV: {raw_csv}")
        viirs = spark.read.csv(_local(raw_csv), header=True)
        cols  = viirs.columns
        logger.info(f"fact_viirs cols: {cols}")

        id_col   = next((c for c in ["LAU_ID", "muni_id"] if c in cols), None)
        date_col = next((c for c in ["date"] if c in cols), None)

        if id_col and date_col:
            # date tiene formato YYYY-MM → extraer year y month
            viirs_clean = viirs.select(
                F.lpad(F.col(id_col).cast("string"), 5, "0").alias("muni_id"),
                F.substring(F.col(date_col), 1, 4).cast(IntegerType()).alias("year"),
                F.substring(F.col(date_col), 6, 2).cast(IntegerType()).alias("month"),
                F.col("mean").cast(DoubleType()).alias("radiancia_media"),
                F.col("max").cast(DoubleType()).alias("radiancia_max"),
                F.col("min").cast(DoubleType()).alias("radiancia_min"),
                F.col("stdDev").cast(DoubleType()).alias("radiancia_stddev"),
            ).filter(F.col("muni_id").isNotNull())
        else:
            logger.warning(f"fact_viirs: columnas id/date no encontradas — se omite. Cols: {cols}")
            viirs_clean = spark.createDataFrame([], StructType([
                StructField("muni_id",          StringType(),  True),
                StructField("year",             IntegerType(), True),
                StructField("month",            IntegerType(), True),
                StructField("radiancia_media",  DoubleType(),  True),
                StructField("radiancia_max",    DoubleType(),  True),
                StructField("radiancia_min",    DoubleType(),  True),
                StructField("radiancia_stddev", DoubleType(),  True),
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
        spark, raw_base_path, dim_muni_path,
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
