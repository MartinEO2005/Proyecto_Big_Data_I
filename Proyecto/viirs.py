"""Utilities to sample VIIRS over Spain and save a local CSV.

This module avoids calling ee.Initialize() at import time. Call
`export_viirs_spain(...)` to run the sampling/export. If Earth Engine
is not authenticated on the machine, the function will return None and
print guidance on how to authenticate.
"""
from datetime import datetime
from typing import Optional
import os
import pandas as pd
import tempfile
import math
import requests
from shapely import wkt


def _download_file(url: str, out_path: str, timeout: int = 60) -> bool:
    """Download a file by streaming. Returns True on success."""
    try:
        resp = requests.get(url, stream=True, timeout=timeout)
        resp.raise_for_status()
        os.makedirs(os.path.dirname(out_path), exist_ok=True)
        with open(out_path, "wb") as f:
            for chunk in resp.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)
        return True
    except Exception as e:
        print(f"⚠️ No se pudo descargar {url}: {type(e).__name__} {e}")
        return False


def _bbox_from_wkt(aoi_wkt: str) -> tuple:
    geom = wkt.loads(aoi_wkt)
    minx, miny, maxx, maxy = geom.bounds
    return minx, miny, maxx, maxy


def _create_grid(minx, miny, maxx, maxy, spacing_km: float):
    # spacing in degrees approx
    spacing_deg = spacing_km / 111.32
    xs = list(frange(minx, maxx, spacing_deg))
    ys = list(frange(miny, maxy, spacing_deg))
    return [(x, y) for x in xs for y in ys]


def frange(start, stop, step):
    x = start
    while x <= stop:
        yield x
        x += step


def sample_geotiff_to_csv(tif_path: str, aoi_wkt: str, spacing_km: int, out_csv: str):
    """Open a GeoTIFF with rasterio, sample a regular grid inside AOI bbox and save CSV.

    Requires rasterio and numpy installed. The CSV will contain columns: lon, lat, band_0, band_1, ...
    """
    try:
        import rasterio
        from rasterio.warp import transform
    except Exception as e:
        print("❌ Para muestrear GeoTIFF necesitas instalar rasterio: pip install rasterio")
        raise

    import numpy as np

    minx, miny, maxx, maxy = _bbox_from_wkt(aoi_wkt)
    coords = _create_grid(minx, miny, maxx, maxy, spacing_km)

    ds = rasterio.open(tif_path)
    # transform coords to dataset CRS if needed
    dst_crs = ds.crs
    if dst_crs is None:
        raise RuntimeError("El GeoTIFF no tiene CRS definido.")

    # rasterio expects coords in (x, y) in the dataset CRS
    lons = [c[0] for c in coords]
    lats = [c[1] for c in coords]
    try:
        xs, ys = transform({'init': 'EPSG:4326'}, dst_crs, lons, lats)
    except Exception:
        # newer rasterio/pyproj APIs
        from pyproj import Transformer
        transformer = Transformer.from_crs('EPSG:4326', dst_crs.to_string(), always_xy=True)
        xs, ys = transformer.transform(lons, lats)

    sample_coords = [(x, y) for x, y in zip(xs, ys)]

    rows = []
    band_count = ds.count
    for lon, lat, sx, sy in zip(lons, lats, xs, ys):
        try:
            vals = list(next(ds.sample([(sx, sy)])))
        except Exception:
            vals = [None] * band_count
        row = {f'band_{i}': vals[i] if i < len(vals) else None for i in range(band_count)}
        row['lon'] = lon
        row['lat'] = lat
        rows.append(row)

    df = pd.DataFrame(rows)
    os.makedirs(os.path.dirname(out_csv), exist_ok=True)
    df.to_csv(out_csv, index=False)
    print(f"✅ VIIRS sample guardado en: {out_csv} (filas: {len(df)})")
    return out_csv


def export_viirs_spain_aws(month: int = None, year: int = None, out_csv: str = "data/luz_nocturna/viirs_spain_sample.csv", spacing_km: int = 10, aoi_wkt: str = None, url_template: str = None) -> str | None:
    """Download a VIIRS monthly GeoTIFF (from a user-provided template or env var) and sample it over Spain.

    The function does not assume a fixed remote layout. Provide `url_template` that contains
    placeholders '{year}' and '{month:02d}', for example:

      https://example.com/viirs/vcmcfg_{year}{month:02d}.tif

    If `url_template` is not provided, the function will look for environment variable
    VIIRS_URL_TEMPLATE. If neither is present, it will print instructions and return None.
    """
    # Determine URL template
    if not url_template:
        url_template = os.getenv('VIIRS_URL_TEMPLATE')
    if not url_template:
        print("❌ No hay plantilla de URL para VIIRS. Define la variable de entorno VIIRS_URL_TEMPLATE con un template que incluya {year} y {month:02d}.")
        print("Ejemplo: export VIIRS_URL_TEMPLATE='https://my-mirror/viirs/vcmcfg_{year}{month:02d}.tif'")
        return None

    # choose month/year default (last month)
    from datetime import date
    today = date.today()
    if year is None or month is None:
        # previous month
        y = today.year
        m = today.month - 1
        if m == 0:
            m = 12
            y -= 1
        year = year or y
        month = month or m

    url = url_template.format(year=year, month=month)
    print(f"→ Intentando descargar VIIRS desde: {url}")

    # download to temp
    tmpdir = tempfile.mkdtemp(prefix='viirs_')
    tif_path = os.path.join(tmpdir, f"viirs_{year}{month:02d}.tif")
    ok = _download_file(url, tif_path)
    if not ok:
        print("❌ No se pudo descargar el GeoTIFF VIIRS. Revisa la plantilla VIIRS_URL_TEMPLATE o descarga manualmente.")
        return None

    if not aoi_wkt:
        # default Spain bbox (peninsula + Baleares)
        aoi_wkt = "POLYGON((-10.7 36.0, -10.7 44.2, 4.6 44.2, 4.6 36.0, -10.7 36.0))"

    try:
        out = sample_geotiff_to_csv(tif_path, aoi_wkt=aoi_wkt, spacing_km=spacing_km, out_csv=out_csv)
        return out
    except Exception as e:
        print("❌ Error al muestrear VIIRS:", type(e), e)
        return None


def _create_point_grid_coords(bbox: list[float], spacing_km: float) -> list[tuple]:
    """Return list of (lon, lat) coordinates covering bbox with approx spacing."""
    minx, miny, maxx, maxy = bbox[0], bbox[1], bbox[2], bbox[3]
    spacing_deg = spacing_km / 111.32
    xs = []
    ys = []
    x = minx
    while x <= maxx:
        xs.append(x)
        x += spacing_deg
    y = miny
    while y <= maxy:
        ys.append(y)
        y += spacing_deg
    coords = [(xi, yi) for xi in xs for yi in ys]
    return coords


def export_viirs_spain(fecha_inicio: str = None, fecha_fin: str = None, out_csv: str = "data/luz_nocturna/viirs_spain_sample.csv", spacing_km: int = 20, scale: int = 500, project: Optional[str] = None, service_account: Optional[str] = None, key_file: Optional[str] = None) -> Optional[str]:
    """Sample VIIRS monthly product over Spain and save a CSV locally.

    Parameters:
      fecha_inicio/fecha_fin: YYYY-MM-DD strings. If None, uses last year to today.
      out_csv: output CSV path.
      spacing_km: spacing between sample points.
      scale: sampling scale in meters.

    Returns path to CSV on success, or None on failure.
    """
    try:
        import ee
    except Exception as e:
        print("❌ Earth Engine Python API not available (pip install earthengine-api):", e)
        return None

    # Initialize EE here, but handle missing authentication
    # Attempt initialization. Support three modes:
    # 1) Service account credentials (service_account + key_file or env vars)
    # 2) Project-based init (project or env var)
    # 3) Interactive user authentication (earthengine authenticate previously run)
    try:
        # read env vars if caller didn't provide
        if not project:
            project = os.getenv("EE_PROJECT")
        if not service_account:
            service_account = os.getenv("EE_SERVICE_ACCOUNT")
        if not key_file:
            # common env var for GCP credentials
            key_file = os.getenv("EE_KEY_FILE") or os.getenv("GOOGLE_APPLICATION_CREDENTIALS")

        if service_account and key_file:
            # initialize with service account key file
            try:
                creds = ee.ServiceAccountCredentials(service_account, key_file)
                if project:
                    ee.Initialize(project=project, credentials=creds)
                else:
                    ee.Initialize(credentials=creds)
            except Exception as e:
                print("❌ Falló la inicialización con ServiceAccountCredentials:", type(e), e)
                print("   Verifica que 'service_account' y 'key_file' sean correctos y que la cuenta tenga acceso a Earth Engine.")
                return None
        else:
            # try project-based or interactive initialization
            try:
                if project:
                    ee.Initialize(project=project)
                else:
                    ee.Initialize()
            except Exception as e:
                print("❌ ee.Initialize failed:", type(e), e)
                print("   Si no estás autenticado, ejecuta: `earthengine authenticate` desde tu shell o configura una cuenta de servicio.")
                return None
    except Exception as e:
        print("❌ Error durante la inicialización de EE:", type(e), e)
        return None

    # defaults
    if fecha_fin is None:
        fecha_fin = datetime.utcnow().date().isoformat()
    if fecha_inicio is None:
        fecha_inicio = (datetime.utcnow().date().replace(year=datetime.utcnow().year - 1)).isoformat()

    # Spain bbox: [minLon, minLat, maxLon, maxLat] (peninsula + Baleares, ignores Canarias for simplicity)
    bbox = [-10.7, 36.0, 4.6, 44.2]
    geometry = ee.Geometry.Rectangle(bbox)

    collection_id = 'NOAA/VIIRS/DNB/MONTHLY_V1/VCMCFG'

    start = ee.Date(fecha_inicio)
    end = ee.Date(fecha_fin)
    col = ee.ImageCollection(collection_id).filterDate(start, end).filterBounds(geometry)
    if col.size().getInfo() == 0:
        print("⚠️ No VIIRS images found for the specified period.")
        return None

    image = col.mean().clip(geometry)

    # create grid coords
    coords = _create_point_grid_coords(bbox, spacing_km)
    # limit number of points to avoid huge requests
    max_points = 2000
    if len(coords) > max_points:
        coords = coords[:: max(1, len(coords) // max_points)]

    feats = [ee.Feature(ee.Geometry.Point([lon, lat])) for lon, lat in coords]
    pts_fc = ee.FeatureCollection(feats)

    # sample
    try:
        sample = image.sampleRegions(collection=pts_fc, scale=scale, geometries=True)
        info = sample.getInfo()
    except Exception as e:
        print("❌ Error al muestrear la imagen VIIRS:", type(e), e)
        return None

    features = info.get('features', [])
    if not features:
        print("⚠️ Muestreo no devolvió features.")
        return None

    rows = []
    band_names = list(image.bandNames().getInfo())
    for f in features:
        props = f.get('properties', {})
        geom = f.get('geometry', {})
        coords = None
        if geom and geom.get('type') == 'Point':
            coords = geom.get('coordinates')
        row = {b: props.get(b) for b in band_names}
        if coords:
            row['lon'] = coords[0]
            row['lat'] = coords[1]
        rows.append(row)

    df = pd.DataFrame(rows)
    # ensure out dir exists
    out_dir = os.path.abspath(os.path.dirname(out_csv))
    os.makedirs(out_dir, exist_ok=True)
    df.to_csv(out_csv, index=False)
    print(f"✅ VIIRS sample guardado en: {out_csv} (filas: {len(df)})")
    return out_csv


if __name__ == '__main__':
    # ejemplo rápido
    export_viirs_spain()
