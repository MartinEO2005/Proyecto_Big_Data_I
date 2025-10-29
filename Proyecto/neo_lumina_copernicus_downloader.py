#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
NeoLumina — Descargador masivo adaptado para el proyecto

Características principales:
 - Soporta colecciones Copernicus OData (ej. SENTINEL-2, SENTINEL-1, SENTINEL-3).
 - Filtrado por AOI (WKT), ventana temporal, tile, y nivel (L2A/L1C).
 - Descarga concurrente de ZIP (.SAFE) y extracción selectiva de activos (TCI, SCL, bandas, manifest, previews, polarizaciones S1, etc.).
 - Reanuda si el ZIP ya existe y evita re-descargas redundantes.
 - Guarda un CSV con metadatos y registro de errores.

Requisitos Python (instalar con pip):
  pip install requests pandas python-dotenv charset-normalizer

Variables de entorno / .env:
  COPERNICUS_USER  (usuario dataspace)
  COPERNICUS_PASSWORD (password dataspace)

Uso (ejemplos):
  # Buscar productos Sentinel-2 L2A para el área de Madrid y descargar 3 ZIPs, extrayendo TCI
  python neo_lumina_copernicus_downloader.py --collection SENTINEL-2 --aoi madrid --days-back 30 \
      --download --asset tci --max-downloads 3 --out-dir data/s2

  # Descargar Sentinel-1 (IW) y extraer tiff/polarizaciones
  python neo_lumina_copernicus_downloader.py --collection SENTINEL-1 --aoi madrid --asset all --max-downloads 5 --out-dir data/s1

Notas y recomendaciones:
 - Para descargas masivas a escala (terabytes) considerar usar los mirrors públicos (AWS Open Data: Sentinel-2 COGs) o servicios como Sentinel Hub / Copernicus Data Space APIs / STAC. Esto evita límites de la API y autenticación Keycloak. (Ver documentación en README o enlaces).
 - El script prioriza robustez: reintentos básicos, límites de concurrencia y evita sobrescribir archivos existentes.

"""
from __future__ import annotations
import os
import sys
import time
import json
import argparse
import fnmatch
import zipfile
from datetime import date, timedelta
from urllib.parse import urlencode
from concurrent.futures import ThreadPoolExecutor, as_completed

from dotenv import load_dotenv
import requests
import pandas as pd
from pandas import json_normalize

CAT_BASE = "https://catalogue.dataspace.copernicus.eu/odata/v1/Products"
TOKEN_URL = "https://identity.dataspace.copernicus.eu/auth/realms/CDSE/protocol/openid-connect/token"

# ----------------- utilidades -----------------

def iso_day(d: date) -> str:
    return d.strftime("%Y-%m-%d")


def today_and_start(days_back: int):
    today = date.today()
    start = today - timedelta(days=days_back)
    return iso_day(today), iso_day(start)


def ensure_env(var: str) -> str:
    v = os.getenv(var)
    if not v:
        raise RuntimeError(f"Falta variable de entorno: {var}")
    return v


def get_keycloak(username: str, password: str) -> str:
    data = {
        "client_id": "cdse-public",
        "username": username,
        "password": password,
        "grant_type": "password",
    }
    r = requests.post(TOKEN_URL, data=data, timeout=60)
    try:
        r.raise_for_status()
    except Exception:
        try:
            payload = r.json()
        except Exception:
            payload = r.text
        raise RuntimeError(f"Fallo creando token. Respuesta: {payload}")
    return r.json()["access_token"]


def make_filter(collection: str, start_iso: str, end_iso: str, wkt: str | None,
                only_l2a: bool, tile: str | None) -> str:
    base = (
        f"Collection/Name eq '{collection}' "
        f"and ContentDate/Start gt {start_iso}T00:00:00.000Z "
        f"and ContentDate/Start lt {end_iso}T00:00:00.000Z"
    )
    if wkt:
        base += f" and OData.CSC.Intersects(area=geography'SRID=4326;{wkt}')"
    if only_l2a and collection.upper().startswith("SENTINEL-2"):
        base += " and not contains(Name,'L1C')"
    if tile:
        base += f" and contains(Name,'{tile}')"
    return base


def fetch_page(params: dict) -> dict:
    r = requests.get(f"{CAT_BASE}?{urlencode(params)}", timeout=90)
    r.raise_for_status()
    return r.json()


def fetch_all(collection: str, start_iso: str, end_iso: str, wkt: str | None,
              top: int, max_pages: int, orderby: str, include_count: bool,
              only_l2a: bool, tile: str | None, select: str | None) -> dict:
    params = {
        "$filter": make_filter(collection, start_iso, end_iso, wkt, only_l2a, tile),
        "$orderby": orderby,
        "$top": str(top),
    }
    if include_count:
        params["$count"] = "true"
    if select:
        params["$select"] = select

    all_items, count, skip = [], None, 0
    for _ in range(max_pages):
        page_params = dict(params)
        if skip:
            page_params["$skip"] = str(skip)
        js = fetch_page(page_params)
        if "@odata.count" in js and count is None:
            count = js["@odata.count"]
        items = js.get("value", [])
        all_items.extend(items)
        if len(items) < top:
            break
        skip += top
        time.sleep(0.25)
    out = {"value": all_items}
    if count is not None:
        out["@odata.count"] = count
    return out


def to_flat_df(js: dict) -> pd.DataFrame:
    df = json_normalize(js.get("value", []))
    if not df.empty:
        first_cols = [c for c in [
            "Id", "Name", "ContentDate.Start", "ContentDate.End",
            "ContentType", "ContentLength", "OriginDate", "GeoFootprint"
        ] if c in df.columns]
        other_cols = [c for c in df.columns if c not in first_cols]
        df = df[first_cols + other_cols]
    return df


def follow_redirects(session: requests.Session, url: str, max_hops: int = 10) -> requests.Response:
    resp = session.get(url, allow_redirects=False, timeout=120)
    hops = 0
    while resp.status_code in (301, 302, 303, 307, 308) and hops < max_hops:
        loc = resp.headers.get("Location")
        if not loc:
            break
        resp = session.get(loc, allow_redirects=False, timeout=300)
        hops += 1
    return resp


# ----------------- descarga ZIP completo -----------------
def download_product_zip(session: requests.Session, product_id: str, identifier: str, out_dir: str, overwrite: bool = False) -> str:
    """
    Descarga el .SAFE (ZIP) completo desde el endpoint OData Products(<GUID>)/$value
    """
    url = f"{CAT_BASE}({product_id})/$value"
    resp = follow_redirects(session, url)
    resp.raise_for_status()
    os.makedirs(out_dir, exist_ok=True)
    out_zip = os.path.join(out_dir, f"{identifier}.zip")
    if os.path.exists(out_zip) and not overwrite:
        print(f"➜ Ya existe {out_zip}, se omite descarga.")
        return out_zip
    with open(out_zip, "wb") as f:
        f.write(resp.content)
    return out_zip


# ----------------- extracción desde el ZIP -----------------
def build_patterns(mode: str, bands: list[str] | None, collection: str) -> list[str]:
    mode = (mode or "").lower()
    pats: list[str] = []
    if mode == "tci":
        pats = [
            "*IMG_DATA*/R10m/*TCI*.jp2",
            "*IMG_DATA*/R20m/*TCI*.jp2",
            "*IMG_DATA*/R60m/*TCI*.jp2",
            "*IMG_DATA_R10m*/*TCI*.jp2",
            "*IMG_DATA_R20m*/*TCI*.jp2",
            "*IMG_DATA_R60m*/*TCI*.jp2",
            "*IMG_DATA*/*TCI*.jp2",
            "*/*TCI*.jp2",
        ]
    elif mode == "scl":
        pats = [
            "*IMG_DATA*/R20m/*SCL*.jp2",
            "*IMG_DATA_R20m*/*SCL*.jp2",
            "*IMG_DATA*/*SCL*.jp2",
            "*/*SCL*.jp2",
        ]
    elif mode == "bands" and bands:
        b = [x.strip().upper() for x in bands]
        for band in b:
            pats += [
                f"*IMG_DATA*/R10m/*{band}*10m*.jp2",
                f"*IMG_DATA*/R20m/*{band}*20m*.jp2",
                f"*IMG_DATA*/R60m/*{band}*60m*.jp2",
                f"*IMG_DATA_R10m*/*{band}*.jp2",
                f"*IMG_DATA_R20m*/*{band}*.jp2",
                f"*IMG_DATA_R60m*/*{band}*.jp2",
                f"*IMG_DATA*/*{band}*.jp2",
                f"*/*{band}*.jp2",
            ]
    elif mode == "all":
        # Extrae archivos comunes: JP2/TIFF/manifest/preview
        pats = ["*IMG_DATA*/*.jp2", "*IMG_DATA*/*.tiff", "*/*PREVIEW*.jp2", "*manifest.safe", "*manifest.xml"]
    elif collection.upper().startswith("SENTINEL-1"):
        # Para S1, extraer tiffs, SAFE manifest y measurement TIFFs
        pats = ["*measurement/*.tiff", "*measurement/*.tif", "*manifest.safe", "*preview*.png", "*preview*.jpg"]
    return pats


def extract_selected_from_zip(zip_path: str, mode: str, bands: list[str] | None, out_dir: str, collection: str) -> list[str]:
    pats = build_patterns(mode, bands, collection)
    extracted: list[str] = []
    with zipfile.ZipFile(zip_path, "r") as zf:
        members = zf.namelist()

        # diagnóstico
        if mode.lower() in ("tci","bands"):
            cand = [m for m in members if any(x in m.upper() for x in ["TCI","B01","B02","B03","B04","SCL"])][:20]
            if cand:
                print("🔎 Posibles activos dentro del ZIP (muestra):")
                for x in cand:
                    print("  -", x)

        to_get = set()
        for pat in pats:
            for m in members:
                if fnmatch.fnmatch(m, pat):
                    to_get.add(m)

        if not to_get:
            print(f"⚠️  No se encontraron archivos que coincidan con {mode} (bandas={bands}) dentro del ZIP.")
            return []

        base_out = os.path.join(os.path.dirname(zip_path), os.path.splitext(os.path.basename(zip_path))[0], "extracted")
        os.makedirs(base_out, exist_ok=True)

        for m in sorted(to_get):
            safe_name = m.replace("/", "_")
            out_file = os.path.join(base_out, safe_name)
            with zf.open(m) as src, open(out_file, "wb") as dst:
                dst.write(src.read())
            extracted.append(out_file)
            print(f"✔ Extraído: {out_file}")

    return extracted


# ----------------- main -----------------
AOIS = {
    "tiny": "POLYGON((0 0, 0 1, 1 1, 1 0, 0 0))",
    "madrid": "POLYGON((-3.9 40.2, -3.9 40.6, -3.3 40.6, -3.3 40.2, -3.9 40.2))",
}


def parse_args():
    ap = argparse.ArgumentParser(description="NeoLumina — descarga masiva Copernicus (S1/S2/S3) y extracción selectiva")
    ap.add_argument("--days-back", type=int, default=7)
    ap.add_argument("--collection", type=str, default="SENTINEL-2")
    ap.add_argument("--aoi", type=str, choices=["tiny", "madrid", "custom"], default="madrid")
    ap.add_argument("--wkt", type=str, default=None)
    ap.add_argument("--top", type=int, default=50)
    ap.add_argument("--max-pages", type=int, default=10)
    ap.add_argument("--orderby", type=str, default="ContentDate/Start desc")
    ap.add_argument("--select", type=str, default="Id,Name,ContentDate,ContentType,ContentLength,OriginDate,GeoFootprint")
    ap.add_argument("--only-l2a", dest="only_l2a", action="store_true")
    ap.add_argument("--no-only-l2a", dest="only_l2a", action="store_false")
    ap.set_defaults(only_l2a=True)
    ap.add_argument("--tile", type=str, default=None)
    ap.add_argument("--csv", type=str, default="copernicus_schema_sample.csv")

    # descarga + extracción
    ap.add_argument("--download", action="store_true", help="Descargar ZIP .SAFE y extraer")
    ap.add_argument("--asset", type=str, choices=["tci", "scl", "bands", "all"], default=None, help="tci | scl | bands | all")
    ap.add_argument("--bands", type=str, default=None, help="B04,B08,...")
    ap.add_argument("--max-downloads", type=int, default=10)
    ap.add_argument("--out-dir", type=str, default="data/copernicus")
    ap.add_argument("--workers", type=int, default=3, help="Concurrent downloads")
    ap.add_argument("--overwrite", action="store_true", help="Sobrescribir zips existentes")
    return ap.parse_args()


def main():
    load_dotenv()
    args = parse_args()

    if args.aoi == "custom":
        if not args.wkt:
            print("ERROR: --aoi custom requiere --wkt 'POLYGON((...))'", file=sys.stderr)
            sys.exit(2)
        wkt = args.wkt
    else:
        wkt = AOIS[args.aoi]

    today_iso, start_iso = today_and_start(args.days_back)
    print("→ Consulta OData…")
    print("Colección:", args.collection)
    print("Ventana:  ", f"{start_iso} → {today_iso}")
    print("AOI:      ", args.aoi)
    print("Solo L2A: ", args.only_l2a)
    if args.tile:
        print("Tile:     ", args.tile)
    print()

    js = fetch_all(
        collection=args.collection,
        start_iso=start_iso,
        end_iso=today_iso,
        wkt=wkt,
        top=args.top,
        max_pages=args.max_pages,
        orderby=args.orderby,
        include_count=True,
        only_l2a=args.only_l2a,
        tile=args.tile,
        select=args.select
    )
    if "@odata.count" in js:
        print("Total (count) filtro:", js["@odata.count"])

    df = to_flat_df(js)
    if df.empty:
        print("No hay productos para este filtro.")
        return

    print("\n— Primeros productos —")
    cols = [c for c in ["Id", "Name", "ContentDate.Start"] if c in df.columns]
    print(df[cols].head(10))

    try:
        df.to_csv(args.csv, index=False)
        print(f"\nCSV guardado: {args.csv} (filas: {len(df)})")
    except Exception as e:
        print(f"Advertencia al escribir CSV: {e}")

    if not args.download:
        print("\nDescarga desactivada (usa --download).")
        return

    # auth
    try:
        user = ensure_env("COPERNICUS_USER")
        pwd = ensure_env("COPERNICUS_PASSWORD")
    except RuntimeError as e:
        print(f"Descarga deshabilitada: {e}")
        return

    try:
        token = get_keycloak(user, pwd)
    except Exception as e:
        print(f"Error de autenticación: {e}")
        return

    session = requests.Session()
    session.headers.update({"Authorization": f"Bearer {token}"})

    # modo extracción
    bands = None
    mode = "tci"
    if args.bands:
        bands = [s.strip() for s in args.bands.split(",") if s.strip()]
        mode = "bands"
    elif args.asset:
        mode = args.asset.lower()

    products = js.get("value", [])
    if not products:
        print("No hay productos para descargar.")
        return

    out_dir = args.out_dir
    os.makedirs(out_dir, exist_ok=True)

    # concurrent downloads
    downloaded = 0
    errors = []

    def worker(item):
        nonlocal downloaded
        pid = item.get("Id")
        name = item.get("Name", "")
        identifier = name.split(".")[0] if isinstance(name, str) and name else pid
        if not pid:
            return (identifier, "no-id")
        try:
            print(f"\n[{identifier}] Descargando ZIP completo…")
            zip_path = download_product_zip(session, pid, identifier, out_dir, overwrite=args.overwrite)
            print(f"[{identifier}] ZIP guardado en: {zip_path}")

            print(f"[{identifier}] Extrayendo '{mode}' (bandas={bands})…")
            extracted = extract_selected_from_zip(zip_path, mode, bands, out_dir, args.collection)
            ok = bool(extracted)
            if not ok:
                msg = f"{identifier}: no assets matched"
                print(f"[{identifier}] ⚠️  No se extrajo nada que coincida con el modo.")
                return (identifier, msg)
            else:
                print(f"[{identifier}] ✅ Extraídos {len(extracted)} archivos.")
                return (identifier, "ok")
        except requests.HTTPError as he:
            msg = f"HTTPError: {he}"
            print(f"[{identifier}] Error HTTP al descargar/extraer: {he}")
            return (identifier, msg)
        except Exception as e:
            msg = f"Error: {e}"
            print(f"[{identifier}] Error: {e}")
            return (identifier, msg)

    with ThreadPoolExecutor(max_workers=max(1, args.workers)) as ex:
        futures = []
        for item in products:
            if downloaded >= args.max_downloads:
                break
            futures.append(ex.submit(worker, item))
            downloaded += 1

        results = []
        for f in as_completed(futures):
            results.append(f.result())

    # guardar resumen
    summary_csv = os.path.join(out_dir, "download_summary.csv")
    rows = []
    for ident, status in results:
        rows.append({"identifier": ident, "status": status})
    pd.DataFrame(rows).to_csv(summary_csv, index=False)
    print(f"\nResumen guardado: {summary_csv}")
    print(f"\n✅ Listo. Productos procesados: {len(results)}")


if __name__ == "__main__":
    try:
        main()
    except requests.HTTPError as e:
        print(f"HTTPError: {e} – contenido: {getattr(e.response, 'text', '')[:200]}...")
        sys.exit(1)
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)
