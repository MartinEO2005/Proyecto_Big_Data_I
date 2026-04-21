# GeoLúmica — Reporte Técnico
**Proyecto Big Data I · Grupo 5 · Abril 2026**

---

## 1. Descripción del sistema

GeoLúmica es un datalake de arquitectura **Medallion** (Raw → Silver → Gold) construido sobre Hadoop 3.3.6 y PySpark 3.5.1. Integra 9 fuentes de datos socioeconómicos y geoespaciales de España a nivel municipal para su explotación en modelos de Machine Learning.

---

## 2. Arquitectura de capas

| Capa | Tecnología | Ruta | Contenido |
|------|-----------|------|-----------|
| **Raw** | CSV local | `data/raw/` | 9 ficheros fuente sin transformar |
| **Silver** | Parquet · HDFS | `hdfs://localhost:9000/geolumica/silver/` | Modelo Estrella: 3 dims + 9 facts |
| **Gold** | Parquet · HDFS | `hdfs://localhost:9000/geolumica/gold/` | DataFrame maestro desnormalizado |

---

## 3. Fuentes de datos integradas

| Dataset | Organismo | Tabla Silver |
|---------|-----------|-------------|
| Padrón Municipal de Habitantes | INE | `fact_demografia` |
| Consumo eléctrico por municipio | IDAE | `fact_energia` |
| Renta neta media declarantes | AEAT | `fact_renta` |
| Migraciones interiores | INE | `fact_migracion_neta` |
| Conectividad vial municipal | Elaboración propia | `fact_conectividad` |
| Métricas ferroviarias OSM | OpenStreetMap | `fact_osm_logistica` |
| Empresas de transporte | INE — DIRCE | `fact_empresas_transporte` |
| Luz nocturna (VIIRS) | NASA | `fact_viirs` |
| Catálogo Sentinel-2 | Copernicus / ESA | `fact_satelital` |

---

## 4. Resultados del pipeline

### 4.1 Capa Silver — 12 tablas Parquet

| Tabla | Registros | Grano |
|-------|----------:|-------|
| `dim_municipio` | 8.131 | Municipio |
| `dim_provincia` | 52 | Provincia |
| `dim_fecha_anual` | 31 | Año (1995–2025) |
| `fact_demografia` | 230.624 | Municipio × Año |
| `fact_energia` | 3.185 | Municipio (snapshot) |
| `fact_renta` | 73.251 | Municipio × Año |
| `fact_migracion_neta` | 32.528 | Municipio × Año |
| `fact_conectividad` | 130.096 | Municipio × Año |
| `fact_empresas_transporte` | 114.632 | Municipio × Año |
| `fact_osm_logistica` | 8.131 | Municipio (snapshot) |
| `fact_viirs` | 585.432 | Municipio × Año × Mes |
| `fact_satelital` | catálogo | Producto Sentinel-2 |

### 4.2 Capa Gold — DataFrame maestro

| Métrica | Valor |
|---------|-------|
| Registros | **252.061** |
| Columnas | **38** |
| Grano | Municipio × Año |
| Duplicados en (muni_id, year) | **0** |
| Validaciones post-pipeline | **✅ Todas pasadas** |

---

## 5. Stack tecnológico

| Componente | Tecnología | Versión |
|-----------|-----------|---------|
| Procesamiento distribuido | Apache Spark (PySpark) | 3.5.1 |
| Sistema de ficheros distribuido | HDFS (Hadoop) | 3.3.6 |
| Runtime | Java (OpenJDK) | 17 |
| Formato de almacenamiento | Apache Parquet | — |
| Entorno de ejecución | WSL2 Ubuntu sobre Windows | — |
| Machine Learning | scikit-learn + pandas | — |

---

## 6. Ejecución del pipeline

> **Prerrequisito:** WSL2 con Hadoop 3.3.6 en `~/hadoop-3.3.6/` y virtualenv en `~/spark_env/`

```bash
# 1. Arrancar HDFS
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
~/hadoop-3.3.6/bin/hdfs --daemon start namenode
~/hadoop-3.3.6/bin/hdfs --daemon start datanode

# 2. Activar entorno y navegar al proyecto
source ~/spark_env/bin/activate
cd /mnt/c/Users/ferna/Documents/GitHub/Proyecto_Big_Data_I/Proyecto_Big_Data_I/Proyecto

# 3. Raw → Silver
python main_silver.py \
  --geojson ../municipios_es.geojson \
  --raw     ../data/raw \
  --dim     hdfs://localhost:9000/geolumica/silver/dim \
  --fact    hdfs://localhost:9000/geolumica/silver/fact

# 4. Silver → Gold
python main_gold.py \
  --dim  hdfs://localhost:9000/geolumica/silver/dim \
  --fact hdfs://localhost:9000/geolumica/silver/fact \
  --gold hdfs://localhost:9000/geolumica/gold

# 5. Validación
python validation_post.py \
  --dim  hdfs://localhost:9000/geolumica/silver/dim \
  --fact hdfs://localhost:9000/geolumica/silver/fact \
  --gold hdfs://localhost:9000/geolumica/gold
```