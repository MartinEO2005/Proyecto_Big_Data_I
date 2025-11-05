"""VIIRS downloader and aggregator (EE-free)

This module downloads monthly VIIRS GeoTIFFs from a user-provided URL template
and aggregates values by Spanish province or municipality. It avoids Earth Engine
entirely and works from public mirrors that expose monthly GeoTIFFs.

Usage summary:
  - Set env VIIRS_URL_TEMPLATE (or pass url_template). Must include {year} and {month:02d}.
  - Call export_viirs_periods(periods, aggregate_level='province'|'municipality', ...)

Notes:
  - Aggregation can use polygon masking (accurate, slower) or centroid sampling (fast).
  - For municipality-level aggregation you should supply a municipalities GeoJSON
    (or let the helper download a public one). For province-level the module downloads
    a default provinces GeoJSON if none provided.
"""
from __future__ import annotations

import os
import json
import tempfile
from typing import List, Tuple, Optional
import requests
import numpy as np
import pandas as pd
import rasterio
import rasterio.mask
from shapely.geometry import shape


def _download_file(url: str, out_path: str, timeout: int = 60) -> bool:
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


def download_viirs_timeseries(periods: List[Tuple[int, int]], url_template: Optional[str] = None, out_dir: str = "data/luz_nocturna/viirs_tifs") -> List[str]:
    """Download VIIRS GeoTIFFs for given periods.

    periods: list of (year, month) tuples
    url_template: template with {year} and {month:02d} (else read env VIIRS_URL_TEMPLATE)
    Returns list of local tif paths (downloaded or existing).
    """
    if not url_template:
        url_template = os.getenv("VIIRS_URL_TEMPLATE")
    if not url_template:
        print("❌ No hay plantilla VIIRS_URL_TEMPLATE. Define la variable de entorno o pásala al llamar.")
        return []

    os.makedirs(out_dir, exist_ok=True)
    downloaded = []
    for year, month in periods:
        try:
            url = url_template.format(year=year, month=month)
        except Exception as e:
            print("⚠️ Error formateando plantilla:", e)
            continue
        fname = f"viirs_{year}{month:02d}.tif"
        out_path = os.path.join(out_dir, fname)
        if os.path.exists(out_path):
            downloaded.append(out_path)
            print(f"→ Ya existe {out_path}")
            continue
        print(f"→ Descargando {url} -> {out_path}")
        ok = _download_file(url, out_path)
        if ok:
            downloaded.append(out_path)
        else:
            print(f"  ⚠️ Falló descarga {year}-{month:02d}")
    return downloaded


DEFAULT_PROVINCES_GEOJSON = "https://raw.githubusercontent.com/codeforgermany/click_that_hood/main/public/data/spain-provinces.geojson"
DEFAULT_MUNICIPALITIES_GEOJSON = "https://raw.githubusercontent.com/codeforgermany/click_that_hood/main/public/data/spain-municipalities.geojson"


def _ensure_geojson(local_path: Optional[str], default_url: str) -> Optional[str]:
    if local_path and os.path.exists(local_path):
        return local_path
    try:
        print(f"→ Descargando GeoJSON desde {default_url}")
        r = requests.get(default_url, timeout=60)
        r.raise_for_status()
        tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".geojson")
        tmp.write(r.content)
        tmp.flush()
        tmp.close()
        return tmp.name
    except Exception as e:
        print("❌ No se pudo obtener GeoJSON:", type(e), e)
        return None


def aggregate_by_province(tif_paths: List[str], provinces_geojson: Optional[str] = None, out_csv: str = "data/luz_nocturna/viirs_by_province.csv") -> Optional[str]:
    """Aggregate list of tifs by province using mask (accurate).
    Returns CSV path or None.
    """
    if not tif_paths:
        print("⚠️ No hay tifs")
        return None
    gj = _ensure_geojson(provinces_geojson, DEFAULT_PROVINCES_GEOJSON)
    if not gj:
        return None
    with open(gj, "r", encoding="utf-8") as f:
        data = json.load(f)
    provinces = [(feat.get("properties", {}).get("name") or feat.get("properties", {}).get("NAME"), feat.get("geometry")) for feat in data.get("features", [])]

    rows = []
    for tif in tif_paths:
        base = os.path.basename(tif)
        try:
            ym = base.replace('.tif','').split('_')[-1]
            year = int(ym[:4]); month = int(ym[4:6])
        except Exception:
            year = None; month = None
        try:
            ds = rasterio.open(tif)
        except Exception as e:
            print("❌ No se pudo abrir tif:", tif, e)
            continue
        band = ds.read(1).astype(float)
        nodata = ds.nodatavals[0] if ds.nodatavals else None
        for name, geom in provinces:
            try:
                out_image, _ = rasterio.mask.mask(ds, [geom], crop=True)
            except Exception:
                rows.append({"province": name, "year": year, "month": month, "mean": None, "count": 0, "tif": tif})
                continue
            arr = out_image[0].astype(float)
            if nodata is not None:
                arr[arr == nodata] = np.nan
            valid = np.isfinite(arr)
            if not valid.any():
                mean = None; cnt = 0
            else:
                mean = float(np.nanmean(arr)); cnt = int(np.sum(valid))
            rows.append({"province": name, "year": year, "month": month, "mean": mean, "count": cnt, "tif": tif})
        ds.close()
    df = pd.DataFrame(rows)
    os.makedirs(os.path.dirname(out_csv), exist_ok=True)
    df.to_csv(out_csv, index=False)
    print(f"✅ Guardado {out_csv}")
    return out_csv


def aggregate_by_municipality(tif_paths: List[str], municipalities_geojson: Optional[str] = None, out_csv: str = "data/luz_nocturna/viirs_by_municipality.csv", sample_mode: str = "centroid") -> Optional[str]:
    """Aggregate by municipality.

    sample_mode: 'centroid' (fast) or 'mask' (accurate but slow)
    """
    if not tif_paths:
        print("⚠️ No hay tifs")
        return None
    gj = _ensure_geojson(municipalities_geojson, DEFAULT_MUNICIPALITIES_GEOJSON)
    if not gj:
        return None
    with open(gj, "r", encoding="utf-8") as f:
        data = json.load(f)
    munis = [(feat.get("properties", {}).get("NAME" ) or feat.get("properties", {}).get("name"), shape(feat.get("geometry"))) for feat in data.get("features", [])]

    rows = []
    for tif in tif_paths:
        base = os.path.basename(tif)
        try:
            ym = base.replace('.tif','').split('_')[-1]
            year = int(ym[:4]); month = int(ym[4:6])
        except Exception:
            year = None; month = None
        try:
            ds = rasterio.open(tif)
        except Exception as e:
            print("❌ No se pudo abrir tif:", tif, e)
            continue
        for name, geom in munis:
            if sample_mode == 'centroid':
                pt = geom.centroid
                try:
                    # transform lon/lat to dataset CRS if needed
                    coords = [(pt.x, pt.y)]
                    # rasterio.sample expects coords in dataset CRS; assume tif is EPSG:4326 or small error otherwise
                    vals = list(ds.sample(coords))
                    val = float(vals[0][0]) if vals and len(vals[0])>0 else None
                    if val is None or (ds.nodatavals and val == ds.nodatavals[0]):
                        rows.append({"municipality": name, "year": year, "month": month, "mean": None, "tif": tif})
                    else:
                        rows.append({"municipality": name, "year": year, "month": month, "mean": float(val), "tif": tif})
                except Exception:
                    rows.append({"municipality": name, "year": year, "month": month, "mean": None, "tif": tif})
            else:
                # mask mode
                try:
                    out_image, _ = rasterio.mask.mask(ds, [json.loads(json.dumps(geom.__geo_interface__))], crop=True)
                except Exception:
                    rows.append({"municipality": name, "year": year, "month": month, "mean": None, "tif": tif})
                    continue
                arr = out_image[0].astype(float)
                nd = ds.nodatavals[0] if ds.nodatavals else None
                if nd is not None:
                    arr[arr == nd] = np.nan
                valid = np.isfinite(arr)
                if not valid.any():
                    rows.append({"municipality": name, "year": year, "month": month, "mean": None, "tif": tif})
                else:
                    rows.append({"municipality": name, "year": year, "month": month, "mean": float(np.nanmean(arr)), "tif": tif})
        ds.close()
    df = pd.DataFrame(rows)
    os.makedirs(os.path.dirname(out_csv), exist_ok=True)
    df.to_csv(out_csv, index=False)
    print(f"✅ Guardado {out_csv}")
    return out_csv


def export_viirs_periods(periods: List[Tuple[int,int]], aggregate_level: str = 'province', url_template: Optional[str] = None, provinces_geojson: Optional[str] = None, municipalities_geojson: Optional[str] = None, out_dir: str = "data/luz_nocturna/viirs_tifs", out_csv: Optional[str] = None, sample_mode: str = 'centroid') -> Optional[str]:
    """High level: download periods and aggregate by requested level ('province' or 'municipality').
    """
    tifs = download_viirs_timeseries(periods, url_template=url_template, out_dir=out_dir)
    if not tifs:
        return None
    if aggregate_level == 'province':
        out = out_csv or os.path.join(os.path.dirname(out_dir), 'viirs_by_province.csv')
        return aggregate_by_province(tifs, provinces_geojson=provinces_geojson, out_csv=out)
    else:
        out = out_csv or os.path.join(os.path.dirname(out_dir), 'viirs_by_municipality.csv')
        return aggregate_by_municipality(tifs, municipalities_geojson=municipalities_geojson, out_csv=out, sample_mode=sample_mode)


if __name__ == '__main__':
    print("Este módulo proporciona utilidades para descargar y agregar VIIRS. Importa las funciones desde tu script.")



def download_viirs_timeseries(year_from: int, year_to: int, months: list[int] | None = None, url_template: str = None, out_dir: str = "data/luz_nocturna/viirs_tifs") -> list:
    """Download monthly VIIRS GeoTIFFs for a range of years.

    Parameters:
      year_from, year_to: inclusive range of years
      months: list of month ints (1-12). If None, defaults to 1..12.
      url_template: template containing {year} and {month:02d}. If None, reads env var VIIRS_URL_TEMPLATE.
      out_dir: directory to store downloaded tifs.

    Returns list of local file paths downloaded (or existing).
    """
    if months is None:
        months = list(range(1, 13))
    if not url_template:
        url_template = os.getenv('VIIRS_URL_TEMPLATE')
    if not url_template:
        print("❌ No hay plantilla de URL para VIIRS. Define VIIRS_URL_TEMPLATE o pasa url_template al llamar.")
        return []

    os.makedirs(out_dir, exist_ok=True)
    downloaded = []
    for year in range(year_from, year_to + 1):
        for month in months:
            url = None
            try:
                url = url_template.format(year=year, month=month)
            except Exception as e:
                print(f"⚠️ Error formateando la plantilla con year={year} month={month}:", e)
                continue
            fname = f"viirs_{year}{month:02d}.tif"
            out_path = os.path.join(out_dir, fname)
            if os.path.exists(out_path):
                print(f"→ Ya existe: {out_path}")
                downloaded.append(out_path)
                continue
            print(f"→ Descargando {url} -> {out_path}")
            ok = _download_file(url, out_path)
            if ok:
                downloaded.append(out_path)
            else:
                print(f"  ⚠️ Falló descarga para {year}-{month:02d}")
    return downloaded


DEFAULT_PROVINCES_GEOJSON = "https://raw.githubusercontent.com/codeforgermany/click_that_hood/main/public/data/spain-provinces.geojson"


def _ensure_provinces_geojson(local_path: str | None = None, url: str = DEFAULT_PROVINCES_GEOJSON) -> str | None:
    """Return a path to a GeoJSON of Spanish provinces. If local_path exists, use it; otherwise download from url into a temp file."""
    if local_path and os.path.exists(local_path):
        return local_path
    try:
        print(f"→ Descargando GeoJSON de provincias desde: {url}")
        resp = requests.get(url, timeout=60)
        resp.raise_for_status()
        tmpf = tempfile.NamedTemporaryFile(delete=False, suffix="_provinces.geojson")
        tmpf.write(resp.content)
        tmpf.flush()
        tmpf.close()
        return tmpf.name
    except Exception as e:
        print("❌ No se pudo obtener GeoJSON de provincias:", type(e), e)
        return None


def aggregate_viirs_by_province(tif_paths: list[str], provinces_geojson: str | None = None, out_csv: str = "data/luz_nocturna/viirs_province_timeseries.csv") -> str | None:
    """Aggregate list of VIIRS GeoTIFFs by Spanish province.

    For each tif file, attempts to compute the mean value within each province polygon.
    Returns path to CSV with columns: province, year, month, mean, count
    """
    if not tif_paths:
        print("⚠️ No hay GeoTIFFs para agregar.")
        return None

    gj_path = _ensure_provinces_geojson(provinces_geojson)
    if not gj_path:
        print("❌ No hay geojson de provincias disponible. Proporciona provinces_geojson.")
        return None

    # load geojson
    with open(gj_path, 'r', encoding='utf-8') as f:
        gj = json.load(f)
    features = gj.get('features', [])
    provinces = []
    for feat in features:
        props = feat.get('properties', {})
        # heurística de nombre
        name = props.get('name') or props.get('NAME') or props.get('prov_name') or props.get('province') or props.get('ADMIN') or str(props)
        provinces.append({'name': name, 'geometry': feat.get('geometry')})

    rows = []
    for tif in tif_paths:
        try:
            base = os.path.basename(tif)
            # intentar extraer year/month del nombre viirs_YYYYMM.tif
            parts = base.replace('.tif','').split('_')
            ym = parts[-1]
            year = int(ym[:4])
            month = int(ym[4:6])
        except Exception:
            year = None
            month = None

        try:
            ds = rasterio.open(tif)
        except Exception as e:
            print(f"❌ No se pudo abrir {tif}:", type(e), e)
            continue

        band_count = ds.count
        for prov in provinces:
            geom = prov['geometry']
            try:
                out_image, out_transform = rasterio.mask.mask(ds, [geom], crop=True)
            except Exception:
                # no intersección or error
                mean_val = None
                count = 0
                rows.append({'province': prov['name'], 'year': year, 'month': month, 'mean': mean_val, 'count': count, 'tif': tif})
                continue
            # out_image shape: (bands, h, w)
            arr = out_image.astype(float)
            # set nodata to nan
            nd = ds.nodata
            if nd is not None:
                arr[arr == nd] = np.nan
            # compute mean across all bands by stacking first band only (assume radiance in band 1)
            band_arr = arr[0]
            # mask all-nans
            valid = np.isfinite(band_arr)
            if not valid.any():
                mean_val = None
                count = 0
            else:
                mean_val = float(np.nanmean(band_arr))
                count = int(np.sum(valid))
            rows.append({'province': prov['name'], 'year': year, 'month': month, 'mean': mean_val, 'count': count, 'tif': tif})
        ds.close()

    import pandas as pd
    df = pd.DataFrame(rows)
    os.makedirs(os.path.dirname(out_csv), exist_ok=True)
    df.to_csv(out_csv, index=False)
    print(f"✅ Agregación por provincia guardada en: {out_csv} (filas: {len(df)})")
    return out_csv



def export_viirs_timeseries_by_province(periods: list[tuple], url_template: str | None = None, provinces_geojson: str | None = None, out_dir: str = "data/luz_nocturna/viirs_tifs", out_csv: str = "data/luz_nocturna/viirs_province_timeseries.csv") -> str | None:
    """Download VIIRS for explicit (year, month) periods and aggregate by province.

    periods: list of (year, month) tuples, e.g. [(2023,1),(2023,2)]
    url_template: optional template (overrides env)
    provinces_geojson: optional geojson path for provinces
    out_dir: folder to store downloaded tifs
    out_csv: output CSV path

    Returns path to out_csv or None on failure.
    """
    # Prepare list of months grouped by year for the downloader convenience
    if not periods:
        print("⚠️ No se proporcionaron periodos (year,month).")
        return None

    # normalize into dict year->[months]
    years = {}
    for y, m in periods:
        years.setdefault(int(y), []).append(int(m))

    # download files
    downloaded = []
    for y, months in years.items():
        files = download_viirs_timeseries(y, y, months=sorted(list(set(months))), url_template=url_template, out_dir=out_dir)
        downloaded.extend(files)

    if not downloaded:
        print("❌ No se descargaron GeoTIFFs.")
        return None

    # aggregate by province
    result = aggregate_viirs_by_province(downloaded, provinces_geojson=provinces_geojson, out_csv=out_csv)
    return result
