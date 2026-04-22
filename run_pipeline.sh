#!/usr/bin/env bash
# =============================================================================
# run_pipeline.sh — Orquestador GeoLúmica
# Uso: bash run_pipeline.sh [--skip-extraction] [--skip-hdfs]
#
# Pasos: 0-HDFS → 1-Extracción → 2-Silver → 3-Gold → 4-Validación
# =============================================================================
set -euo pipefail

# ── Rutas ────────────────────────────────────────────────────────────────────
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROYECTO="$SCRIPT_DIR/Proyecto"
GEOJSON="$SCRIPT_DIR/municipios_es.geojson"
HDFS_DIM="hdfs://localhost:9000/geolumica/silver/dim"
HDFS_FACT="hdfs://localhost:9000/geolumica/silver/fact"
HDFS_GOLD="hdfs://localhost:9000/geolumica/gold"

# ── Entorno Hadoop/Spark ──────────────────────────────────────────────────────
export HADOOP_HOME="${HADOOP_HOME:-/home/fernaferna/hadoop-3.3.6}"
export JAVA_HOME="${JAVA_HOME:-/usr/lib/jvm/java-17-openjdk-amd64}"
export HADOOP_CONF_DIR="$HADOOP_HOME/etc/hadoop"
export SPARK_HOME="${SPARK_HOME:-/home/fernaferna/spark_env/lib/python3.12/site-packages/pyspark}"
PYTHON="${PYTHON:-/home/fernaferna/spark_env/bin/python}"
export PYSPARK_PYTHON="$PYTHON"
export PYSPARK_DRIVER_PYTHON="$PYTHON"
export PATH="$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$SPARK_HOME/bin:$PATH"

# ── Flags ─────────────────────────────────────────────────────────────────────
SKIP_EXTRACTION=false
SKIP_HDFS=false

for arg in "$@"; do
  case $arg in
    --skip-extraction) SKIP_EXTRACTION=true ;;
    --skip-hdfs)       SKIP_HDFS=true ;;
  esac
done

# ── Helpers ───────────────────────────────────────────────────────────────────
log() { echo "[$(date '+%H:%M:%S')] $*"; }
die() { echo "❌ $*" >&2; exit 1; }

# ── Paso 0: HDFS ──────────────────────────────────────────────────────────────
if [ "$SKIP_HDFS" = false ]; then
  log "Paso 0: Iniciando HDFS..."
  bash "$SCRIPT_DIR/start_hdfs.sh" || die "HDFS no arrancó"
fi

# ── Paso 1: Extracción ────────────────────────────────────────────────────────
if [ "$SKIP_EXTRACTION" = false ]; then
  log "Paso 1: Extracción de datos..."
  cd "$PROYECTO"
  "$PYTHON" main_extraction.py || die "Extracción fallida"
  cd "$SCRIPT_DIR"
fi

# ── Paso 2: Silver ────────────────────────────────────────────────────────────
log "Paso 2: Generando Silver (Spark + Parquet → HDFS)..."
cd "$PROYECTO"
"$PYTHON" main_silver.py \
  --geojson "$GEOJSON" \
  --raw     "../data/raw" \
  --dim     "$HDFS_DIM" \
  --fact    "$HDFS_FACT" || die "Silver fallido"

# ── Paso 3: Gold ──────────────────────────────────────────────────────────────
log "Paso 3: Generando Gold (df_maestro → HDFS)..."
"$PYTHON" main_gold.py \
  --dim  "$HDFS_DIM" \
  --fact "$HDFS_FACT" \
  --gold "$HDFS_GOLD" || die "Gold fallido"

# ── Paso 4: Validación ────────────────────────────────────────────────────────
log "Paso 4: Validando Silver + Gold..."
"$PYTHON" validation_post.py \
  --dim  "$HDFS_DIM" \
  --fact "$HDFS_FACT" \
  --gold "$HDFS_GOLD" || die "Validación fallida"

cd "$SCRIPT_DIR"
log "✅ Pipeline completado"
