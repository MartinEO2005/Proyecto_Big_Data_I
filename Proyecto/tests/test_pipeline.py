"""
Suite de tests para GeoLúmica Pipeline.

Cubre exactamente los bugs que han aparecido en auditorías:
  1. Paths CWD-relativos (credenciales, outdir, munis path)
  2. HTTP sin raise_for_status
  3. Filtros isNotNull ausentes en facts Spark
  4. Configuración Spark inconsistente (master, shuffle.partitions)
  5. cloud filter ignorado en catalog.py
  6. Concatenar CSVs sin guard de directorio

Ejecutar:
  cd Proyecto
  pytest tests/ -v
"""

import csv
import os
import re
import sys
from unittest.mock import MagicMock, patch

import pytest

# ── Rutas ────────────────────────────────────────────────────────────────────
_TESTS_DIR = os.path.dirname(os.path.abspath(__file__))
_PROYECTO  = os.path.dirname(_TESTS_DIR)
sys.path.insert(0, _PROYECTO)


# =============================================================================
# 1. PATHS CWD-RELATIVOS
# =============================================================================

def test_viirs_credentials_path_script_relative():
    """viirs.init_ee debe usar path relativo al script, no al CWD."""
    import inspect
    import extraction.viirs as m
    src = inspect.getsource(m.init_ee)
    assert "__file__" in src, "init_ee() debe usar __file__ para localizar google_credentials.json"
    # Aseguramos que NO hay un literal CWD-relativo suelto
    assert "= 'google_credentials.json'" not in src
    assert '= "google_credentials.json"' not in src


def test_viirs_provincias_credentials_path_script_relative():
    import inspect
    import extraction.viirs_provincias_gaul as m
    src = inspect.getsource(m.init_ee)
    assert "__file__" in src
    assert "= 'google_credentials.json'" not in src
    assert '= "google_credentials.json"' not in src


def test_viirs_provincias_default_outdir_absolute():
    import extraction.viirs_provincias_gaul as m
    assert os.path.isabs(os.path.normpath(m.DEFAULT_OUTDIR)), (
        f"DEFAULT_OUTDIR es relativo al CWD: {m.DEFAULT_OUTDIR!r}"
    )


def test_osm_munis_path_absolute():
    import extraction.osm_muni_metrics as m
    assert os.path.isabs(m.MUNIS_PATH), (
        f"MUNIS_PATH es relativo al CWD: {m.MUNIS_PATH!r}"
    )


def test_osm_out_prefix_absolute():
    import extraction.osm_muni_metrics as m
    assert os.path.isabs(m.OUT_PREFIX), (
        f"OUT_PREFIX es relativo al CWD: {m.OUT_PREFIX!r}"
    )


# =============================================================================
# 2. HTTP — raise_for_status
# =============================================================================

def test_consumo_renta_raises_on_http_error():
    """descargar_tabla() debe propagar errores HTTP (raise_for_status)."""
    import extraction.consumo_renta_media_pib as m
    mock_resp = MagicMock()
    mock_resp.raise_for_status.side_effect = RuntimeError("HTTP 500")
    with patch("extraction.consumo_renta_media_pib.requests.get", return_value=mock_resp):
        with pytest.raises(RuntimeError):
            m.descargar_tabla()


# =============================================================================
# 3. catalog.py — filtro de nubosidad
# =============================================================================

def test_catalog_cloud_filter_applied():
    from extraction.catalog import build_filter
    filt = build_filter("SENTINEL-2", "2024-01-01", "2024-12-31", cloud=30)
    assert "cloudCover" in filt, "build_filter no incluye filtro de cloudCover"
    assert "30" in filt


def test_catalog_cloud_filter_not_applied_by_default():
    from extraction.catalog import build_filter
    filt = build_filter("SENTINEL-2", "2024-01-01", "2024-12-31")
    assert "cloudCover" not in filt


# =============================================================================
# 4. CONFIGURACIÓN SPARK CONSISTENTE
# =============================================================================

def _source(filename):
    with open(os.path.join(_PROYECTO, filename), encoding="utf-8") as f:
        return f.read()


def test_spark_master_in_silver():
    assert '.master("local[*]")' in _source("main_silver.py"), \
        "main_silver.py: falta .master('local[*]') en SparkSession"


def test_spark_master_in_gold():
    assert '.master("local[*]")' in _source("main_gold.py"), \
        "main_gold.py: falta .master('local[*]') en SparkSession"


def test_spark_master_in_validation():
    assert '.master("local[*]")' in _source("validation_post.py"), \
        "validation_post.py: falta .master('local[*]') en SparkSession"


@pytest.mark.parametrize("filename", ["main_silver.py", "main_gold.py", "validation_post.py"])
def test_shuffle_partitions_is_4(filename):
    src = _source(filename)
    m = re.search(r"shuffle\.partitions[\"'],\s*[\"'](\d+)[\"']", src)
    assert m, f"{filename}: no se encontró shuffle.partitions"
    assert m.group(1) == "4", (
        f"{filename}: shuffle.partitions={m.group(1)!r}, esperado '4'"
    )


# =============================================================================
# 5. viirs_provincias_gaul — concatenar_csvs guard
# =============================================================================

def test_concatenar_csvs_no_crash_if_dir_missing(tmp_path):
    import extraction.viirs_provincias_gaul as m
    # No debe explotar con FileNotFoundError si tmp_provincias no existe
    result = m.concatenar_csvs(str(tmp_path))
    assert result is None


# =============================================================================
# 6. FACTS SPARK — isNotNull en muni_id
#    Requiere PySpark. Se salta automáticamente si no está instalado.
# =============================================================================

pyspark = pytest.importorskip("pyspark", reason="pyspark no instalado — tests Spark omitidos")


@pytest.fixture(scope="module")
def spark():
    from pyspark.sql import SparkSession
    s = (
        SparkSession.builder
        .master("local[1]")
        .appName("test_geolumica")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield s
    s.stop()


def _write_csv(path, header, rows):
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(header)
        for row in rows:
            w.writerow(row)


def _count_nulls(df, col="muni_id"):
    from pyspark.sql import functions as F
    return df.filter(F.col(col).isNull()).count()


def test_fact_renta_no_null_muni_id(spark, tmp_path):
    d = tmp_path / "renta"; d.mkdir()
    _write_csv(d / "renta_municipios.csv",
               ["codigo_municipio", "anio", "valor"],
               [["28001", "2022", "15000"],
                ["",      "2022", "99999"],   # → debe descartarse
                [None,    "2022", "88888"]])  # → debe descartarse
    from silver.facts import create_fact_renta
    df = create_fact_renta(spark, str(tmp_path), None, str(tmp_path / "out_renta.parquet"))
    assert _count_nulls(df) == 0, "fact_renta emitió muni_ids nulos"


def test_fact_energia_no_null_muni_id(spark, tmp_path):
    d = tmp_path / "energia"; d.mkdir()
    _write_csv(d / "consumo_electrico.csv",
               ["Codigo", "Consumo eléctrico", "Total"],
               [["28001", "Mediana consumo anual", "5000"],
                ["",      "Mediana consumo anual", "9999"]])  # → debe descartarse
    from silver.facts import create_fact_energia
    df = create_fact_energia(spark, str(tmp_path), None, str(tmp_path / "out_energia.parquet"))
    assert _count_nulls(df) == 0, "fact_energia emitió muni_ids nulos"


def test_fact_migracion_no_null_muni_id(spark, tmp_path):
    d = tmp_path / "migracion"; d.mkdir()
    _write_csv(d / "migracion_interior_municipios.csv",
               ["codigo_municipio", "anio", "sexo", "nacionalidad", "cantidad (personas)"],
               [["28001", "2022", "Ambos sexos", "Total", "100"],
                ["",      "2022", "Ambos sexos", "Total", "50"]])  # → debe descartarse
    from silver.facts import create_fact_migracion_neta
    df = create_fact_migracion_neta(spark, str(tmp_path), None, str(tmp_path / "out_mig.parquet"))
    assert _count_nulls(df) == 0, "fact_migracion_neta emitió muni_ids nulos"


def test_fact_conectividad_no_null_muni_id(spark, tmp_path):
    d = tmp_path / "transporte"; d.mkdir()
    _write_csv(d / "conectividad_municipal_2010_2025.csv",
               ["LAU_ID", "Anio", "Indice_Conectividad", "Vehiculos_Oficial"],
               [["28001", "2022", "1.5", "500"],
                ["",      "2022", "0.0", "0"]])  # → debe descartarse
    from silver.facts import create_fact_conectividad
    df = create_fact_conectividad(spark, str(tmp_path), None, str(tmp_path / "out_con.parquet"))
    assert _count_nulls(df) == 0, "fact_conectividad emitió muni_ids nulos"


def test_fact_osm_no_null_muni_id(spark, tmp_path):
    d = tmp_path / "transporte"; d.mkdir(exist_ok=True)
    _write_csv(d / "muni_station_metrics_reduced.csv",
               ["LAU_ID", "stations_count", "operator_count",
                "min_distance_km_to_station", "mean_distance_km_to_station",
                "stations_density_km2", "accessible_share"],
               [["28001", "3", "2", "0.5", "1.2", "0.01", "0.66"],
                ["",      "0", "0", "",    "",    "0.0",  "0.0"]])  # → debe descartarse
    from silver.facts import create_fact_osm_logistica
    df = create_fact_osm_logistica(spark, str(tmp_path), None, str(tmp_path / "out_osm.parquet"))
    assert _count_nulls(df) == 0, "fact_osm_logistica emitió muni_ids nulos"


def test_fact_empresas_no_null_muni_id(spark, tmp_path):
    d = tmp_path / "empresas_transporte"; d.mkdir()
    _write_csv(d / "empresas_transporte_prov_mun_anchos.csv",
               ["codigo", "nombre", "tipo", "2022", "2023"],
               [["28001", "Madrid", "municipio", "100", "110"],
                ["",      "Vacio",  "municipio", "5",   "5"],  # → debe descartarse
                ["28",    "Prov",   "provincia", "200", "210"]])  # → excluido por tipo
    from silver.facts import create_fact_empresas_transporte
    df = create_fact_empresas_transporte(spark, str(tmp_path), None, str(tmp_path / "out_emp.parquet"))
    assert _count_nulls(df) == 0, "fact_empresas_transporte emitió muni_ids nulos"
    # Las filas de provincia no deben aparecer
    from pyspark.sql import functions as F
    prov_rows = df.filter(F.length(F.col("muni_id")) != 5).count()
    assert prov_rows == 0, "fact_empresas_transporte incluyó filas de provincia"
