#!/usr/bin/env bash
# =============================================================================
# start_hdfs.sh - Arranca NameNode y DataNode sin SSH
# Usa setsid para desanclar del shell padre (evita SIGHUP en WSL)
# =============================================================================
set -euo pipefail

HADOOP_HOME="${HADOOP_HOME:-$(ls -d "$HOME"/hadoop-3.3.* 2>/dev/null | head -1)}"
HDFS_BIN="$HADOOP_HOME/bin/hdfs"
NAMENODE_DIR="${HOME}/hadoop-data/namenode"

# Fuerza redirects de WebHDFS a localhost (evita hostnames *.localdomain no resolubles en Windows)
export HDFS_NAMENODE_OPTS="${HDFS_NAMENODE_OPTS:-} -Ddfs.client.use.datanode.hostname=true"
export HDFS_DATANODE_OPTS="${HDFS_DATANODE_OPTS:-} -Ddfs.datanode.hostname=localhost -Ddfs.datanode.use.datanode.hostname=true -Ddfs.client.use.datanode.hostname=true"

log() { echo "[$(date '+%H:%M:%S')] $*"; }
die() { echo "ERROR: $*" >&2; exit 1; }

# Formatea el namenode solo si es la primera vez
if [ ! -d "$NAMENODE_DIR/current" ]; then
  log "Primera ejecucion: formateando NameNode..."
  "$HDFS_BIN" namenode -format -force -nonInteractive
fi

log "Arrancando NameNode..."
setsid "$HDFS_BIN" --daemon start namenode

log "Arrancando DataNode..."
setsid "$HDFS_BIN" --daemon start datanode

# Espera hasta 60 s a que el NameNode este disponible
log "Esperando a que HDFS este listo..."
for i in $(seq 1 30); do
  if "$HDFS_BIN" dfsadmin -report &>/dev/null; then
    log "HDFS operativo"
    break
  fi
  if [ "$i" -eq 30 ]; then
    die "HDFS no respondio tras 60 s"
  fi
  sleep 2
done

# Crea directorios si no existen
for dir in /geolumica/silver/dim /geolumica/silver/fact /geolumica/gold; do
  "$HDFS_BIN" dfs -mkdir -p "$dir" 2>/dev/null || true
done

log "HDFS listo y directorios creados"
