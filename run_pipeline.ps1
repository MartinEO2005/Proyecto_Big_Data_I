<#
PASOS PARA ARRANCAR EL PROYECTO (PowerShell en la raíz):

1. Arranque completo (con extracción):
    Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass; .\run_pipeline.ps1

2. Sin extracción (más rápido, si ya tienes los datos):
    Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass; .\run_pipeline.ps1 -SkipExtraction

3. Solo Silver/Gold/validación (si HDFS ya está levantado):
    Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass; .\run_pipeline.ps1 -SkipExtraction -SkipHdfs
#>

param(
    [switch]$SkipExtraction,
    [switch]$SkipHdfs,
    [switch]$ExportGoldCsv,
    [switch]$InstallRequirements,
    [switch]$ResetWsl
)

$d = $PSScriptRoot.Substring(0,1).ToLower()
$r = $PSScriptRoot.Substring(2) -replace "\\", "/"
$W    = "/mnt/$d$r"
$DIM  = "hdfs://localhost:9000/geolumica/silver/dim"
$FACT = "hdfs://localhost:9000/geolumica/silver/fact"
$GOLD = "hdfs://localhost:9000/geolumica/gold"
$GEO  = "$W/municipios_es.geojson"

Write-Host "Preparando entorno WSL..." -ForegroundColor Cyan
if ($ResetWsl) {
    wsl --shutdown
    Start-Sleep -Seconds 2
}

$PY = (wsl -d Ubuntu -- sh -c 'if [ -f "$HOME/spark_env/bin/python" ]; then echo "$HOME/spark_env/bin/python"; else which python3; fi').Trim()
$HD = (wsl -d Ubuntu -- sh -c 'ls -d $HOME/hadoop-3.3.* 2>/dev/null | head -1').Trim()
if (-not $HD) { $HD = (wsl -d Ubuntu -- sh -c 'echo "${HADOOP_HOME:-}"').Trim() }
if (-not $PY) { Write-Host "ERROR: Python no encontrado" -ForegroundColor Red; exit 1 }
if (-not $HD) { Write-Host "ERROR: Hadoop no encontrado"  -ForegroundColor Red; exit 1 }
Write-Host "[OK] Python: $PY" -ForegroundColor Green
Write-Host "[OK] Hadoop: $HD" -ForegroundColor Green

$L = [System.Collections.Generic.List[string]]::new()
$L.Add("#!/bin/bash")
$L.Add("set -e")
$L.Add("export HADOOP_HOME=$HD")
$L.Add('export JAVA_HOME=$(dirname $(dirname $(readlink -f $(which java))))')
$L.Add("export PATH=$HD/bin:$HD/sbin:" + '$PATH')
$L.Add("export PYSPARK_PYTHON=$PY")
$L.Add("export PYSPARK_DRIVER_PYTHON=$PY")
$L.Add("export TQDM_DISABLE=1")
$L.Add("export HADOOP_ROOT_LOGGER=ERROR,console")
$L.Add('export HDFS_NAMENODE_OPTS="${HDFS_NAMENODE_OPTS:-} -Ddfs.client.use.datanode.hostname=true"')
$L.Add('export HDFS_DATANODE_OPTS="${HDFS_DATANODE_OPTS:-} -Ddfs.datanode.hostname=localhost -Ddfs.datanode.use.datanode.hostname=true -Ddfs.client.use.datanode.hostname=true"')
$L.Add('LOG=/tmp/geolumica_run.log')
$L.Add('> "$LOG"')
$L.Add('TOTAL_START=$SECONDS')
$L.Add('')

# Funcion helper para ejecutar pasos con log acumulado
$L.Add('run_step() {')
$L.Add('  local label="$1"')
$L.Add('  local cmd="$2"')
$L.Add('  local t=$SECONDS')
$L.Add('  printf "  %-40s INICIANDO\n" "$label"')
$L.Add('  eval "$cmd" >> "$LOG" 2>&1 &')
$L.Add('  local pid=$!')
$L.Add('  while kill -0 $pid 2>/dev/null; do')
$L.Add('    sleep 1')
$L.Add('  done')
$L.Add('  if wait $pid; then')
$L.Add('    local elapsed=$(( SECONDS - t ))')
$L.Add('    printf "  %-40s %ds  OK\n" "$label" "$elapsed"')
$L.Add('  else')
$L.Add('    printf "  %-40s FALLO\n" "$label"')
$L.Add('    echo "--- ultimas lineas del log ---"')
$L.Add('    tail -25 "$LOG"')
$L.Add('    return 1')
$L.Add('  fi')
$L.Add('  return 0')
$L.Add('}')
$L.Add('')
$L.Add('run_step_retry() {')
$L.Add('  local retries="$1"')
$L.Add('  local label="$2"')
$L.Add('  local cmd="$3"')
$L.Add('  local attempt=1')
$L.Add('  while [ $attempt -le $retries ]; do')
$L.Add('    local tag="$label"')
$L.Add('    if [ $retries -gt 1 ]; then')
$L.Add('      tag="$label (intento $attempt/$retries)"')
$L.Add('    fi')
$L.Add('    if run_step "$tag" "$cmd"; then')
$L.Add('      return 0')
$L.Add('    fi')
$L.Add('    attempt=$(( attempt + 1 ))')
$L.Add('    if [ $attempt -le $retries ]; then')
$L.Add('      echo "Reintentando en 3s..."')
$L.Add('      sleep 3')
$L.Add('    fi')
$L.Add('  done')
$L.Add('  return 1')
$L.Add('}')
$L.Add('')

if (-not $SkipHdfs) {
    $L.Add("run_step_retry 2 '[1/5] Iniciando HDFS (sin SSH)' 'if $HD/bin/hdfs dfs -ls / >/dev/null 2>&1; then echo HDFS ya activo; else nohup $HD/bin/hdfs --daemon start namenode >/tmp/geolumica_hdfs_start.log 2>&1 < /dev/null || true; nohup $HD/bin/hdfs --daemon start datanode >>/tmp/geolumica_hdfs_start.log 2>&1 < /dev/null || true; nohup $HD/bin/hdfs --daemon start secondarynamenode >>/tmp/geolumica_hdfs_start.log 2>&1 < /dev/null || true; sleep 4; $HD/bin/hdfs dfsadmin -safemode leave || true; $HD/bin/hdfs dfs -ls / >/dev/null; fi' || exit 1")
} else {
    $L.Add("run_step '[1/4] Verificando HDFS activo' '$HD/bin/hdfs dfs -ls / >/dev/null' || { echo 'HDFS no responde. Ejecuta sin -SkipHdfs o arranca HDFS antes.'; exit 1; }")
}
if ($InstallRequirements) {
    $L.Add("run_step_retry 2 '[2/5] Instalando requirements' '$PY -m pip install -q -r $W/requirements.txt' || exit 1")
}
if (-not $SkipExtraction) {
    $L.Add("run_step '[3/5] Descargando datos (~6h 47m 1s aprox.)' 'cd $W/Proyecto && $PY main_extraction.py' || exit 1")
}
$L.Add("run_step_retry 2 '[4/5] Generando Silver' 'cd $W/Proyecto && $PY main_silver.py --geojson $GEO --raw ../data/raw --dim $DIM --fact $FACT' || exit 1")
$L.Add("run_step_retry 2 '[5/5] Generando Gold' 'cd $W/Proyecto && $PY main_gold.py --fact $FACT --gold $GOLD' || exit 1")
$L.Add("run_step_retry 2 '[+]   Validando' 'cd $W/Proyecto && $PY validation_post.py --dim $DIM --fact $FACT --gold $GOLD' || exit 1")
$L.Add('echo ""')
$L.Add('echo "Pipeline completado en $(( SECONDS - TOTAL_START ))s"')

$tmpWin = "$env:TEMP\geolumica_run.sh"
$enc = New-Object System.Text.UTF8Encoding($false)
[System.IO.File]::WriteAllText($tmpWin, ($L -join "`n") + "`n", $enc)

$td = $env:TEMP.Substring(0,1).ToLower()
$tr = $env:TEMP.Substring(2) -replace "\\", "/"
$tmpWSL = "/mnt/$td$tr/geolumica_run.sh"

Write-Host ""
wsl -d Ubuntu -- bash $tmpWSL
if ($LASTEXITCODE -ne 0) { Write-Host "FALLO (codigo $LASTEXITCODE)" -ForegroundColor Red; exit 1 }

if ($ExportGoldCsv) {
    if (-not (Test-Path "$PSScriptRoot\data\gold")) { New-Item -ItemType Directory -Path "$PSScriptRoot\data\gold" | Out-Null }
    Write-Host "Exportando Gold a CSV en data/gold..." -ForegroundColor Cyan
    c:/Users/ferna/Documents/GitHub/Proyecto_Big_Data_I/Proyecto_Big_Data_I/.venv/Scripts/python.exe ./export_gold_to_csv.py
}
