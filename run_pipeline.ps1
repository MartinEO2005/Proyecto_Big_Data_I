param([switch]$SkipExtraction, [switch]$SkipHdfs)

$d = $PSScriptRoot.Substring(0,1).ToLower()
$r = $PSScriptRoot.Substring(2) -replace "\\", "/"
$W    = "/mnt/$d$r"
$DIM  = "hdfs://localhost:9000/geolumica/silver/dim"
$FACT = "hdfs://localhost:9000/geolumica/silver/fact"
$GOLD = "hdfs://localhost:9000/geolumica/gold"
$GEO  = "$W/municipios_es.geojson"

Write-Host "Iniciando WSL..." -ForegroundColor Cyan
wsl --shutdown
Start-Sleep -Seconds 2

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
$L.Add('LOG=/tmp/geolumica_run.log')
$L.Add('> "$LOG"')
$L.Add('TOTAL_START=$SECONDS')
$L.Add('')

# Funcion helper con barra de progreso animada
$L.Add('BAR_WIDTH=28')
$L.Add('run_step() {')
$L.Add('  local label="$1"')
$L.Add('  local cmd="$2"')
$L.Add('  local t=$SECONDS')
$L.Add('  local i=0')
$L.Add('  eval "$cmd" >> "$LOG" 2>&1 &')
$L.Add('  local pid=$!')
$L.Add('  while kill -0 $pid 2>/dev/null; do')
$L.Add('    local filled=$(( i % (BAR_WIDTH + 1) ))')
$L.Add('    local empty=$(( BAR_WIDTH - filled ))')
$L.Add('    local elapsed=$(( SECONDS - t ))')
$L.Add('    printf "\r  %-28s [%s%s] %ds" "$label" "$(printf "#%.0s" $(seq 1 $filled) 2>/dev/null || printf "%${filled}s" | tr " " "#")" "$(printf "%${empty}s")" "$elapsed"')
$L.Add('    i=$(( i + 1 ))')
$L.Add('    sleep 1')
$L.Add('  done')
$L.Add('  if wait $pid; then')
$L.Add('    local elapsed=$(( SECONDS - t ))')
$L.Add('    printf "\r  %-28s [%s] %ds  OK\n" "$label" "$(printf "%${BAR_WIDTH}s" | tr " " "#")" "$elapsed"')
$L.Add('  else')
$L.Add('    printf "\r  %-28s [FALLO]\n" "$label"')
$L.Add('    echo "--- ultimas lineas del log ---"')
$L.Add('    tail -25 "$LOG"')
$L.Add('    exit 1')
$L.Add('  fi')
$L.Add('}')
$L.Add('')

if (-not $SkipHdfs) {
    $L.Add("run_step '[1/5] Iniciando HDFS' '$HD/sbin/start-dfs.sh'")
}
$L.Add("run_step '[2/5] Instalando requirements' '$PY -m pip install -q -r $W/requirements.txt'")
if (-not $SkipExtraction) {
    $L.Add("run_step '[3/5] Descargando datos (~20 min)' 'cd $W/Proyecto && $PY main_extraction.py'")
}
$L.Add("run_step '[4/5] Generando Silver' 'cd $W/Proyecto && $PY main_silver.py --geojson $GEO --raw ../data/raw --dim $DIM --fact $FACT'")
$L.Add("run_step '[5/5] Generando Gold' 'cd $W/Proyecto && $PY main_gold.py --dim $DIM --fact $FACT --gold $GOLD'")
$L.Add("run_step '[+]   Validando' 'cd $W/Proyecto && $PY validation_post.py --dim $DIM --fact $FACT --gold $GOLD'")
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
