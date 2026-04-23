#!/usr/bin/env bash
# =============================================================================
# run_pipeline.ps1 — GeoLúmica Pipeline (salida limpia)
# =============================================================================
set -euo pipefail

# ── Rutas ─────────────────────────────────────────────────────────────────────
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROYECTO="$SCRIPT_DIR/Proyecto"
GEOJSON="$SCRIPT_DIR/municipios_es.geojson"
HDFS_DIM="hdfs://localhost:9000/geolumica/silver/dim"
HDFS_FACT="hdfs://localhost:9000/geolumica/silver/fact"
HDFS_GOLD="hdfs://localhost:9000/geolumica/gold"

# ── Entorno Hadoop/Spark ──────────────────────────────────────────────────────
export HADOOP_HOME="${HADOOP_HOME:-$(ls -d "$HOME"/hadoop-3.3.* 2>/dev/null | head -1)}"
export JAVA_HOME="${JAVA_HOME:-$(dirname $(dirname $(readlink -f $(which java))))}"
export HADOOP_CONF_DIR="$HADOOP_HOME/etc/hadoop"
export SPARK_HOME="${SPARK_HOME:-$("$HOME"/spark_env/bin/python -c 'import pyspark; import os; print(os.path.dirname(pyspark.__file__))' 2>/dev/null || echo '')}"
PYTHON="${PYTHON:-$([ -f "$HOME/spark_env/bin/python" ] && echo "$HOME/spark_env/bin/python" || which python3)}"
export PYSPARK_PYTHON="$PYTHON"
export PYSPARK_DRIVER_PYTHON="$PYTHON"
export PATH="$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$SPARK_HOME/bin:$PATH"
# Suprimir logs Java/Spark/Hadoop — todo el ruido va al log
export HADOOP_ROOT_LOGGER="ERROR,console"
export SPARK_SUBMIT_OPTS="-Dlog4j.rootCategory=ERROR,console"

# ── Log file ──────────────────────────────────────────────────────────────────
mkdir -p "$SCRIPT_DIR/logs"
LOG="$SCRIPT_DIR/logs/pipeline_$(date +%Y%m%d_%H%M%S).log"
echo "Pipeline iniciado: $(date)" > "$LOG"

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
TOTAL_STEPS=5
CURRENT_STEP=0

progress_bar() {
  local pct=$1 label="$2"
  local filled=$(( pct * 30 / 100 ))
  local bar=""
  for ((i=0; i<filled; i++));   do bar+="█"; done
  for ((i=filled; i<30; i++)); do bar+="░"; done
  printf "\r  [%s] %3d%%  %s" "$bar" "$pct" "$label"
}

run_step() {
  local pct_start=$1 pct_end=$2 label="$3"; shift 3
  local frames='⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏'
  local n=${#frames}

  # Correr en background, todo el output al log
  "$@" >> "$LOG" 2>&1 &
  local pid=$!
  local i=0
  local pct=$pct_start

  while kill -0 "$pid" 2>/dev/null; do
    pct=$(( pct_start + (pct_end - pct_start) * i / 50 ))
    [ $pct -gt $pct_end ] && pct=$pct_end
    printf "\r  [" 
    local filled=$(( pct * 30 / 100 ))
    for ((j=0; j<filled; j++));   do printf "█"; done
    for ((j=filled; j<30; j++)); do printf "░"; done
    printf "] %3d%%  %s %s" "$pct" "${frames:$((i%n)):1}" "$label"
    i=$((i+1))
    sleep 0.1
  done

  wait "$pid"
  local code=$?
  if [ $code -eq 0 ]; then
    progress_bar "$pct_end" "$label"
    printf "  ✅\n"
  else
    printf "\n  ❌  Error en: %s\n" "$label"
    printf "     Log: %s\n" "$LOG"
    printf "     Últimas líneas:\n"
    tail -15 "$LOG" | sed 's/^/     /'
    exit $code
  fi
}

# ── Header ────────────────────────────────────────────────────────────────────
echo
echo "  ╔══════════════════════════════════════╗"
echo "  ║       GeoLúmica Pipeline v1.0        ║"
echo "  ╚══════════════════════════════════════╝"
printf "  Log: %s\n\n" "$LOG"

# ── Paso 0: Requirements ──────────────────────────────────────────────────────
run_step 0 10 "Instalando dependencias Python" \
  "$PYTHON" -m pip install -q -r "$SCRIPT_DIR/requirements.txt"

# ── Paso 1: HDFS ──────────────────────────────────────────────────────────────
if [ "$SKIP_HDFS" = false ]; then
  run_step 10 25 "Iniciando HDFS" \
    bash "$SCRIPT_DIR/start_hdfs.sh"
else
  progress_bar 25 "HDFS (omitido)"; printf "  ⏭\n"
fi

# ── Paso 2: Extracción ────────────────────────────────────────────────────────
if [ "$SKIP_EXTRACTION" = false ]; then
  cd "$PROYECTO"
  run_step 25 50 "Descargando datos (INE · GEE · OSM)" \
    "$PYTHON" main_extraction.py
  cd "$SCRIPT_DIR"
else
  progress_bar 50 "Extracción (omitida)"; printf "  ⏭\n"
fi

# ── Paso 3: Silver ────────────────────────────────────────────────────────────
cd "$PROYECTO"
run_step 50 70 "Generando Silver  (dim + fact → HDFS)" \
  "$PYTHON" main_silver.py \
    --geojson "$GEOJSON" \
    --raw     "../data/raw" \
    --dim     "$HDFS_DIM" \
    --fact    "$HDFS_FACT"

# ── Paso 4: Gold ──────────────────────────────────────────────────────────────
run_step 70 90 "Generando Gold    (tabla maestra → HDFS)" \
  "$PYTHON" main_gold.py \
    --dim  "$HDFS_DIM" \
    --fact "$HDFS_FACT" \
    --gold "$HDFS_GOLD"

# ── Paso 5: Validación ────────────────────────────────────────────────────────
run_step 90 100 "Validando calidad de datos" \
  "$PYTHON" validation_post.py \
    --dim  "$HDFS_DIM" \
    --fact "$HDFS_FACT" \
    --gold "$HDFS_GOLD"

cd "$SCRIPT_DIR"
echo
echo "  ╔══════════════════════════════════════╗"
echo "  ║  ✅  Pipeline completado con éxito   ║"
echo "  ╚══════════════════════════════════════╝"
printf "  Log completo: %s\n\n" "$LOG"
