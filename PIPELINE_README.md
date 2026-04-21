# GeoLúmica — Pipeline Raw → Silver → Gold

## Resumen

Pipeline de datos completo sobre **Hadoop + PySpark** que transforma 9 fuentes CSV en un DataFrame maestro listo para Machine Learning.

| Etapa | Entrada | Salida | Tiempo |
|---|---|---|---|
| Raw → Silver | 9 CSVs locales | 12 tablas Parquet en HDFS | ~5-15 min |
| Silver → Gold | 12 Parquets HDFS | 1 Parquet maestro en HDFS | ~3-10 min |
| Validación | Gold HDFS | Informe por consola | ~2-5 min |

**Resultado final:** `hdfs://localhost:9000/geolumica/gold/df_maestro.parquet` — **252.061 registros × 38 columnas**

---

## Estructura del Proyecto

```
Proyecto_Big_Data_I/
├── municipios_es.geojson
├── data/
│   ├── raw/                          ← CSVs fuente (9 datasets)
│   └── clean/                        ← CSVs pre-limpiados (VIIRS, energía)
└── Proyecto/
    ├── main_extraction.py            ← Fase 0: Descarga de datos
    ├── main_silver.py                ← Fase 1: Raw → Silver
    ├── main_gold.py                  ← Fase 2: Silver → Gold
    ├── validation_pre.py             ← Validación de entradas
    ├── validation_post.py            ← Validación de salidas
    └── silver/
        ├── dimensions.py             ← dim_municipio, dim_provincia, dim_fecha_anual
        ├── facts.py                  ← 8 facts (demografía, energía, renta, ...)
        └── satelital.py              ← fact_satelital (Sentinel-2)
```

---

## Prerrequisitos

- WSL2 Ubuntu con Hadoop 3.3.6 en `~/hadoop-3.3.6/`
- Python virtualenv en `~/spark_env` con `pyspark==3.5.1`
- Java 17: `export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64`

---

## Ejecución Completa

### 1. Arrancar HDFS (cada sesión WSL)

```bash
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
nohup ~/hadoop-3.3.6/bin/hdfs --daemon start namenode
nohup ~/hadoop-3.3.6/bin/hdfs --daemon start datanode
```

### 2. Activar entorno

```bash
source ~/spark_env/bin/activate
cd /mnt/c/Users/ferna/Documents/GitHub/Proyecto_Big_Data_I/Proyecto_Big_Data_I/Proyecto
```

### 3. Validación PRE (opcional pero recomendado)

```bash
python validation_pre.py
```

```
✅ GeoJSON válido: 8131 features, LAU_ID presente
✅ Raw layer completa: 9 datasets encontrados
✅ TODAS LAS VALIDACIONES PRE-EJECUCIÓN PASARON
```

### 4. Raw → Silver

```bash
python main_silver.py \
  --geojson ../municipios_es.geojson \
  --raw     ../data/raw \
  --dim     hdfs://localhost:9000/geolumica/silver/dim \
  --fact    hdfs://localhost:9000/geolumica/silver/fact
```

```
✅ dim_municipio.parquet       — 8.131 registros
✅ dim_provincia.parquet       — 52 registros
✅ dim_fecha_anual.parquet     — 31 registros
✅ fact_demografia.parquet     — 230.624 registros
✅ fact_energia.parquet        — 3.185 registros
✅ fact_renta.parquet          — 73.251 registros
✅ fact_migracion_neta.parquet — 32.528 registros
✅ fact_conectividad.parquet   — 130.096 registros
✅ fact_empresas_transporte.parquet — 114.632 registros
✅ fact_osm_logistica.parquet  — 8.131 registros
✅ fact_viirs.parquet          — 585.432 registros
✅ fact_satelital.parquet      — catálogo Sentinel-2
✅ PIPELINE RAW → SILVER COMPLETADO EXITOSAMENTE
```

### 5. Silver → Gold

```bash
python main_gold.py \
  --dim  hdfs://localhost:9000/geolumica/silver/dim \
  --fact hdfs://localhost:9000/geolumica/silver/fact \
  --gold hdfs://localhost:9000/geolumica/gold
```

```
✅ Gold escrita: 252.061 registros, 38 columnas
✅ PIPELINE SILVER → GOLD COMPLETADO EXITOSAMENTE
```

### 6. Validación POST

```bash
python validation_post.py \
  --dim  hdfs://localhost:9000/geolumica/silver/dim \
  --fact hdfs://localhost:9000/geolumica/silver/fact \
  --gold hdfs://localhost:9000/geolumica/gold
```

```
✅ Silver: 12 tablas presentes en HDFS
✅ Gold: sin duplicados en (muni_id, year)
✅ Gold: 252.061 registros, 38 columnas
✅ TODAS LAS VALIDACIONES POST-EJECUCIÓN PASARON
```

---

## Modelo Estrella (Silver)

```
                    dim_fecha_anual
                         │
          dim_provincia ─┤
                         │
dim_municipio ───── fact_demografia
                    fact_renta
                    fact_migracion_neta
                    fact_conectividad
                    fact_empresas_transporte
                    fact_viirs
                    fact_energia        (snapshot, sin year)
                    fact_osm_logistica  (snapshot, sin year)
                    fact_satelital      (catálogo productos)
```

---

## Gold — Columnas del DataFrame Maestro

| Bloque | Columnas |
|---|---|
| Identificadores | `muni_id`, `muni_name`, `prov_id`, `prov_name`, `region_name` |
| Temporal | `year`, `quarter` |
| Geografía | `latitude`, `longitude`, `area_km2` |
| Demografía | `poblacion_total`, `densidad_poblacion_km2`, `crecimiento_pob_yoy_pct`, `crecimiento_pob_3y_pct` |
| Economía | `renta_neta_media_euros`, `renta_vs_nacional_pct` |
| Energía | `consumo_kwh_total`, `consumo_per_capita`, `consumo_per_km2` |
| Migración | `saldo_migratorio_neto` |
| Conectividad vial | `num_vehiculos`, `indice_conectividad` |
| Conectividad ferro | `num_estaciones`, `distancia_min_km`, `distancia_media_km` |
| Empresas | `num_empresas_total`, `densidad_empresas_1000hab` |
| Luz nocturna | `radiancia_media_anual`, `radiancia_max_anual`, `radiancia_min_anual`, `radiancia_stddev_anual` |
| Clasificación | `categoria_municipio` (Gran ciudad / Ciudad mediana / Rural / Muy rural) |
| Índice | `riesgo_despoblacion_score` (0–100) |

---

## Uso en ML

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("GeoLumica-ML") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://localhost:9000") \
    .getOrCreate()

gold = spark.read.parquet("hdfs://localhost:9000/geolumica/gold/df_maestro.parquet")
df = gold.toPandas()  # 252.061 filas × 38 columnas
```

---

## HDFS — Navegación

```bash
# UI web
http://localhost:9870/explorer.html#/geolumica

# Consola
~/hadoop-3.3.6/bin/hdfs dfs -ls -R /geolumica
```
