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
    """Genera fact_demografia con el esquema del referente.

    CSV limpio: region_code, region_name, year, provincia_population,
                municipio, Total, Hombres, Mujeres
    Salida (8 cols): muni_id, year, municipio, region_name,
                     poblacion_muni, poblacion_prov, Hombres, Mujeres
    """
    logger.info("Generando fact_demografia...")

    candidates = [
        _os.path.join(raw_path, "..", "clean", "demografia_municipios_final.csv"),
        _os.path.join(raw_path, "demografia", "demografia_poblacion_municipios.csv"),
    ]
    raw_csv = next((p for p in candidates if _os.path.exists(p)), None)

    try:
        if raw_csv is None:
            raise FileNotFoundError(f"Demografía CSV no encontrado. Probados: {candidates}")

        demo = spark.read.option("header", True).option("inferSchema", True).csv(_local(raw_csv))
        cols = demo.columns
        logger.info(f"fact_demografia cols: {cols}")

        # Esquema del CSV limpio (prioritario)
        if "region_code" in cols:
            demo_clean = (
                demo
                .withColumn("muni_id", F.lpad(F.col("region_code").cast("string"), 5, "0"))
                .withColumn("year", F.col("year").cast(IntegerType()))
                .withColumn("poblacion_muni", F.col("Total").cast(DoubleType()))
                .withColumn("poblacion_prov", F.col("provincia_population").cast(DoubleType()))
                .filter(F.col("muni_id").isNotNull())
                .select("muni_id", "year", "municipio", "region_name",
                        "poblacion_muni", "poblacion_prov", "Hombres", "Mujeres")
            )
        else:
            # Fallback: esquema raw INE
            id_col   = next((c for c in ["cod_muni", "muni_id", "LAU_ID"] if c in cols), None)
            year_col = next((c for c in ["year", "anio", "Anio"] if c in cols), None)
            val_col  = next((c for c in ["population", "Total", "poblacion"] if c in cols), None)
            if not all([id_col, year_col, val_col]):
                raise ValueError(f"fact_demografia: columnas clave no encontradas. Cols: {cols}")
            demo_clean = (
                demo
                .withColumn("muni_id", F.lpad(F.col(id_col).cast("string"), 5, "0"))
                .withColumn("year", F.col(year_col).cast(IntegerType()))
                .withColumn("poblacion_muni", F.col(val_col).cast(DoubleType()))
                .filter(F.col("muni_id").isNotNull())
                .select("muni_id", "year", "poblacion_muni")
            )

        demo_clean.write.mode("overwrite").parquet(output_path)
        logger.info(f"fact_demografia: {demo_clean.count()} filas, {len(demo_clean.columns)} cols")
        return demo_clean

    except Exception as e:
        logger.error(f"Error en fact_demografia: {e}", exc_info=True)
        return spark.createDataFrame([], StructType([
            StructField("muni_id", StringType(), True),
            StructField("year", IntegerType(), True),
            StructField("poblacion_muni", DoubleType(), True),
        ]))


# ==================== FACT_ENERGIA ====================

def create_fact_energia(spark, raw_path, _dim_muni_path, output_path):
    """Genera fact_energia conservando TODAS las columnas del CSV.

    Patrón referente (silver_socioeconomico.py): añade muni_id de 5 dígitos,
    elimina la columna original del ID y conserva el resto íntegramente.
    Snapshot sin year (un registro por municipio).
    """
    logger.info("Generando fact_energia (todas las columnas)...")

    # Prioriza el CSV limpio; como fallback usa el raw
    candidates = [
        _os.path.join(raw_path, "..", "clean", "consumo_electrico_final_limpio.csv"),
        _os.path.join(raw_path, "energia", "consumo_electrico.csv"),
    ]
    raw_csv = next((p for p in candidates if _os.path.exists(p)), None)

    try:
        if raw_csv is None:
            raise FileNotFoundError(f"Consumo CSV no encontrado. Probados: {candidates}")

        df = spark.read.option("header", True).option("inferSchema", True).csv(_local(raw_csv))
        cols = df.columns
        logger.info(f"fact_energia cols ({len(cols)}): {cols}")

        id_col = next((c for c in ["Codigo", "codigo", "muni_id", "LAU_ID"] if c in cols), None)
        if not id_col:
            raise ValueError(f"fact_energia: columna ID no encontrada. Cols: {cols}")

        # Filtrar filas con código de municipio válido (≥4 dígitos)
        energia_clean = (
            df
            .filter(F.col(id_col).isNotNull() & (F.length(F.trim(F.col(id_col).cast("string"))) >= 4))
            .withColumn("muni_id", F.lpad(F.col(id_col).cast("string"), 5, "0"))
            .filter(F.col("muni_id").isNotNull())
            .drop(id_col)
        )

        energia_clean.write.mode("overwrite").parquet(output_path)
        logger.info(f"fact_energia: {energia_clean.count()} filas, {len(energia_clean.columns)} cols")
        return energia_clean

    except Exception as e:
        logger.error(f"Error en fact_energia: {e}", exc_info=True)
        return spark.createDataFrame([], StructType([
            StructField("muni_id", StringType(), True),
        ]))


# ==================== FACT_RENTA ====================

def create_fact_renta(spark, raw_path, _dim_muni_path, output_path):
    """Genera fact_renta con el esquema del referente.

    CSV limpio: provincia, nombre_municipio, codigo_municipio, anio, pib
    Salida (3 cols): muni_id, year, renta_media
    """
    logger.info("Generando fact_renta...")

    candidates = [
        _os.path.join(raw_path, "..", "clean", "rentamedia_municipios_final_limpio.csv"),
    ] + sorted(glob.glob(f"{raw_path}/renta/*.csv"))
    raw_csv = next((p for p in candidates if _os.path.exists(p)), None)

    try:
        if raw_csv is None:
            raise FileNotFoundError("Renta CSV no encontrado.")

        renta = spark.read.option("header", True).option("inferSchema", True).csv(_local(raw_csv))
        cols = renta.columns
        logger.info(f"fact_renta cols: {cols}")

        id_col   = next((c for c in ["codigo_municipio", "muni_id", "Codigo", "LAU_ID"] if c in cols), None)
        year_col = next((c for c in ["anio", "year", "Anio", "Year"] if c in cols), None)
        val_col  = next((c for c in ["pib", "renta", "renta_media", "valor", "renta_neta_media_euros"] if c in cols), None)

        if not all([id_col, year_col, val_col]):
            raise ValueError(f"fact_renta: columnas clave no encontradas. Cols: {cols}")

        renta_clean = (
            renta
            .withColumn("muni_id", F.lpad(F.col(id_col).cast("string"), 5, "0"))
            .withColumn("year", F.col(year_col).cast(IntegerType()))
            .withColumn("renta_media", F.col(val_col).cast(DoubleType()))
            .filter(F.col("muni_id").isNotNull())
            .select("muni_id", "year", "renta_media")
        )

        renta_clean.write.mode("overwrite").parquet(output_path)
        logger.info(f"fact_renta: {renta_clean.count()} filas")
        return renta_clean

    except Exception as e:
        logger.error(f"Error en fact_renta: {e}")
        return spark.createDataFrame([], StructType([
            StructField("muni_id", StringType(), True),
            StructField("year", IntegerType(), True),
            StructField("renta_media", DoubleType(), True),
        ]))


# ==================== FACT_MIGRACION_NETA ====================

def create_fact_migracion_neta(spark, raw_path, _dim_muni_path, output_path):
    """Genera fact_migracion_neta con el esquema del referente.

    CSV limpio: provincia, nombre_municipio, codigo_municipio, anio,
                nacionalidad, cantidad (personas)
    Salida (4 cols): muni_id, year, nacionalidad, migrantes
    """
    logger.info("Generando fact_migracion_neta...")

    candidates = [
        _os.path.join(raw_path, "..", "clean", "migracion_municipios_final_limpio.csv"),
    ] + sorted(glob.glob(f"{raw_path}/migracion/*.csv"))
    raw_csv = next((p for p in candidates if _os.path.exists(p)), None)

    try:
        if raw_csv is None:
            raise FileNotFoundError(f"Migración CSV no encontrado.")

        migracion = spark.read.option("header", True).option("inferSchema", True).csv(_local(raw_csv))
        cols = migracion.columns
        logger.info(f"fact_migracion cols: {cols}")

        id_col  = next((c for c in ["codigo_municipio", "muni_id", "LAU_ID"] if c in cols), None)
        year_col = next((c for c in ["anio", "year", "Anio"] if c in cols), None)
        val_col  = next((c for c in migracion.columns if "cantidad" in c.lower() or "valor" in c.lower()), cols[-1])
        nac_col  = next((c for c in ["nacionalidad"] if c in cols), None)

        if not all([id_col, year_col]):
            raise ValueError(f"fact_migracion: columnas clave no encontradas. Cols: {cols}")

        sel = [
            F.lpad(F.col(id_col).cast("string"), 5, "0").alias("muni_id"),
            F.col(year_col).cast(IntegerType()).alias("year"),
        ]
        if nac_col:
            sel.append(F.col(nac_col).alias("nacionalidad"))
        sel.append(F.col(val_col).cast(DoubleType()).alias("migrantes"))

        mig_clean = migracion.select(*sel).filter(F.col("muni_id").isNotNull())

        mig_clean.write.mode("overwrite").parquet(output_path)
        logger.info(f"fact_migracion_neta: {mig_clean.count()} filas, {len(mig_clean.columns)} cols")
        return mig_clean

    except Exception as e:
        logger.error(f"Error en fact_migracion_neta: {e}")
        return spark.createDataFrame([], StructType([
            StructField("muni_id", StringType(), True),
            StructField("year", IntegerType(), True),
            StructField("migrantes", DoubleType(), True),
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
        # Conservar el nombre original "Vehiculos_Oficial" igual que el referente
        if veh_col: select_cols.append(F.col(veh_col).alias("Vehiculos_Oficial"))
        
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
    """Genera fact_empresas_transporte con el esquema del referente.

    CSV limpio: codigo, nombre, tipo, 2012, 2013, ..., 2025
    Salida (4 cols): muni_id, tipo, year, total_empresas  (unpivot)
    """
    logger.info("Generando fact_empresas_transporte (esquema referente)...")

    candidates = [
        _os.path.join(raw_path, "..", "clean", "empresas_transporte_final_limpio.csv"),
        _os.path.join(raw_path, "empresas_transporte", "empresas_transporte_prov_mun_anchos.csv"),
    ]
    raw_csv = next((p for p in candidates if _os.path.exists(p)), None)

    try:
        if raw_csv is None:
            raise FileNotFoundError(f"Empresas CSV no encontrado. Probados: {candidates}")

        empresas = spark.read.option("header", True).option("inferSchema", True).csv(_local(raw_csv))
        cols = empresas.columns
        logger.info(f"fact_empresas cols: {cols}")

        id_col   = next((c for c in ["codigo", "muni_id", "LAU_ID", "codigo_municipio"] if c in cols), None)
        year_cols = [c for c in cols if c.isdigit()]

        if not id_col or not year_cols:
            raise ValueError(f"fact_empresas: columnas clave no encontradas. Cols: {cols}")

        stack_items = ", ".join([f"'{y}', `{y}`" for y in year_cols])
        stack_expr  = f"stack({len(year_cols)}, {stack_items}) as (year, total_empresas)"

        empresas_clean = (
            empresas
            .select(
                F.lpad(F.col(id_col).cast("string"), 5, "0").alias("muni_id"),
                F.col("tipo") if "tipo" in cols else F.lit(None).cast(StringType()),
                F.expr(stack_expr),
            )
            .filter(F.col("muni_id").isNotNull())
            .withColumn("year", F.col("year").cast(IntegerType()))
            .withColumn("total_empresas", F.col("total_empresas").cast(DoubleType()))
        )
        if "tipo" not in cols:
            empresas_clean = empresas_clean.withColumnRenamed("NULL", "tipo")

        empresas_clean.write.mode("overwrite").parquet(output_path)
        logger.info(f"fact_empresas_transporte: {empresas_clean.count()} filas, {len(empresas_clean.columns)} cols")
        return empresas_clean

    except Exception as e:
        logger.error(f"Error en fact_empresas_transporte: {e}")
        return spark.createDataFrame([], StructType([
            StructField("muni_id", StringType(), True),
            StructField("tipo", StringType(), True),
            StructField("year", IntegerType(), True),
            StructField("total_empresas", DoubleType(), True),
        ]))


# ==================== FACT_OSM_LOGISTICA ====================

def create_fact_osm_logistica(spark, raw_path, _dim_muni_path, output_path):
    """Genera fact_osm_logistica conservando TODAS las columnas del CSV.

    Patrón referente (silver_osm.py): añade muni_id de 5 dígitos y elimina la
    columna original del ID. El resto de columnas se conserva íntegramente.
    """
    logger.info("Generando fact_osm_logistica (todas las columnas)...")

    # Puede ser el CSV limpio o el raw con métricas
    candidates = [
        _os.path.join(raw_path, "..", "clean", "muni_station_osm_limpio.csv"),
        _os.path.join(raw_path, "transporte", "muni_station_metrics_reduced.csv"),
    ]
    raw_csv = next((p for p in candidates if _os.path.exists(p)), None)

    try:
        if raw_csv is None:
            raise FileNotFoundError(f"OSM CSV no encontrado. Probados: {candidates}")

        osm = spark.read.option("header", True).option("inferSchema", True).csv(_local(raw_csv))
        cols = osm.columns
        id_col = next((c for c in ["LAU_ID", "SOG_ID", "muni_id", "municipio_id"] if c in cols), None)
        if id_col is None:
            raise ValueError(f"No se encontró columna de ID en OSM. Cols: {cols}")

        # Añade muni_id de 5 dígitos y elimina la columna original
        osm_clean = (
            osm
            .withColumn("muni_id", F.lpad(F.col(id_col).cast("string"), 5, "0"))
            .filter(F.col("muni_id").isNotNull())
            .drop(id_col)
        )

        osm_clean.write.mode("overwrite").parquet(output_path)
        logger.info(f"fact_osm_logistica: {osm_clean.count()} filas, {len(osm_clean.columns)} cols")
        return osm_clean

    except Exception as e:
        logger.error(f"Error en fact_osm_logistica: {e}")
        return spark.createDataFrame([], StructType([
            StructField("muni_id", StringType(), True),
        ]))


# ==================== FACT_VIIRS ====================

def create_fact_viirs(spark, raw_path, _dim_muni_path, output_path):
    """Genera fact_viirs con el esquema del referente.

    CSV limpio: PROV_NAME, date, mean_prov, AREA_KM2, CNTR_CODE, GISCO_ID,
                LAU_ID, LAU_NAME, max, mean, min, stdDev
    Salida (6 cols): muni_id, year, fecha, intensidad_luz, max, min
    """
    logger.info("Generando fact_viirs...")

    candidates = [
        _os.path.join(raw_path, "..", "clean", "viirsFinal_limpio.csv"),
        _os.path.join(raw_path, "luz_nocturna", "viirs_luz_nocturna.csv"),
    ]
    raw_csv = next((p for p in candidates if _os.path.exists(p)), None)

    try:
        if raw_csv is None:
            logger.warning("VIIRS: CSV no encontrado — se omite")
            return spark.createDataFrame([], StructType([
                StructField("muni_id",        StringType(),  True),
                StructField("year",           IntegerType(), True),
                StructField("fecha",          StringType(),  True),
                StructField("intensidad_luz", DoubleType(),  True),
                StructField("max",            DoubleType(),  True),
                StructField("min",            DoubleType(),  True),
            ]))

        logger.info(f"Leyendo VIIRS CSV: {raw_csv}")
        viirs = spark.read.option("header", True).option("inferSchema", True).csv(_local(raw_csv))
        cols  = viirs.columns
        logger.info(f"fact_viirs cols: {cols}")

        id_col   = next((c for c in ["LAU_ID", "muni_id"] if c in cols), None)
        date_col = next((c for c in ["date"] if c in cols), None)

        if id_col and date_col:
            viirs_clean = (
                viirs
                .withColumn("muni_id", F.lpad(F.col(id_col).cast("string"), 5, "0"))
                .withColumn("fecha", F.to_date(F.col(date_col)))
                .withColumn("year", F.year(F.col("fecha")))
                .withColumn("intensidad_luz", F.col("mean").cast(DoubleType()))
                .filter(F.col("muni_id").isNotNull())
                .select("muni_id", "year", "fecha", "intensidad_luz", "max", "min")
            )
        else:
            logger.warning(f"fact_viirs: columnas id/date no encontradas — se omite. Cols: {cols}")
            viirs_clean = spark.createDataFrame([], StructType([
                StructField("muni_id",        StringType(),  True),
                StructField("year",           IntegerType(), True),
                StructField("fecha",          StringType(),  True),
                StructField("intensidad_luz", DoubleType(),  True),
                StructField("max",            DoubleType(),  True),
                StructField("min",            DoubleType(),  True),
            ]))

        viirs_clean.write.mode("overwrite").parquet(output_path)
        logger.info(f"fact_viirs: {viirs_clean.count()} filas")
        return viirs_clean

    except Exception as e:
        logger.error(f"Error en fact_viirs: {e}", exc_info=True)
        return spark.createDataFrame([], StructType([
            StructField("muni_id",        StringType(),  True),
            StructField("year",           IntegerType(), True),
            StructField("fecha",          StringType(),  True),
            StructField("intensidad_luz", DoubleType(),  True),
            StructField("max",            DoubleType(),  True),
            StructField("min",            DoubleType(),  True),
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
