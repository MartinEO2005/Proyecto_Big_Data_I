#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
NeoLumina — Descargador final Copernicus (versión en español)

Características:
 - Usa el catálogo OData para buscar productos (SENTINEL-2 por defecto)
 - Descarga cada producto desde zipper.dataspace.copernicus.eu (sin 401)
 - AOI, fechas y directorio de salida vienen de config.py
 - Usa .env (COPERNICUS_USER / COPERNICUS_PASSWORD) para autenticación
 - Descarga ZIP .SAFE, extrae JP2 necesarios, convierte a TIFF y PNG
"""

from __future__ import annotations

import os
import time
import argparse
import fnmatch
import zipfile
from urllib.parse import urlencode
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
import hashlib
import subprocess
import shutil

import requests
from dotenv import load_dotenv
import pandas as pd
from pandas import json_normalize
import numpy as np
import rasterio
from PIL import Image

# ===============================
# Carga .env desde la raíz
# ===============================
ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
ENV_PATH = os.path.join(ROOT_DIR, ".env")
print(f"[DEBUG] Cargando .env desde: {ENV_PATH}")
load_dotenv(ENV_PATH)

# ===============================
# Config del proyecto
# ===============================
from config import AOI_WKT, DATE_FROM, DATE_TO, OUTDIR

# ===============================
# URLs Copernicus
# ===============================
CAT_BASE = "https://catalogue.dataspace.copernicus.eu/odata/v1/Products"
TOKEN_URL = "https://identity.dataspace.copernicus.eu/auth/realms/CDSE/protocol/openid-connect/token"
ZIPPER_BASE = "https://zipper.dataspace.copernicus.eu/odata/v1/Products"


# -------------------------------
# Utilidades auth
# -------------------------------
def ensure_env(var: str) -> str:
    v = os.getenv(var)
    if not v:
        raise RuntimeError(f"Falta la variable de entorno: {var}")
    return v


def get_keycloak(username: str, password: str) -> str:
    data = {
        "client_id": "cdse-public",
        "username": username,
        "password": password,
        "grant_type": "password",
    }
    r = requests.post(TOKEN_URL, data=data, timeout=60)
    r.raise_for_status()
    return r.json()["access_token"]


def get_fresh_session() -> requests.Session:
    """Crea una sesión Requests con un token recién obtenido."""
    user = ensure_env("COPERNICUS_USER")
    pwd = ensure_env("COPERNICUS_PASSWORD")
    token = get_keycloak(user, pwd)

    s = requests.Session()
    s.headers.update({"Authorization": f"Bearer {token}"})
    return s


# -------------------------------
# Filtro OData
# -------------------------------
def make_filter(
    collection: str,
    start_iso: str,
    end_iso: str,
    wkt: str | None,
    only_l2a: bool,
    tile: str | None,
) -> str:

    base = (
        f"Collection/Name eq '{collection}' "
        f"and ContentDate/Start ge {start_iso}T00:00:00.000Z "
        f"and ContentDate/Start le {end_iso}T23:59:59.999Z"
    )

    if wkt:
        base += f" and OData.CSC.Intersects(area=geography'SRID=4326;{wkt}')"

    if only_l2a and collection.upper().startswith("SENTINEL-2"):
        base += " and not contains(Name,'L1C')"

    if tile:
        base += f" and contains(Name,'{tile}')"

    return base


def fetch_page(params: dict) -> dict:
    """Pide UNA página al catálogo OData, con reintentos robustos."""
    url = f"{CAT_BASE}?{urlencode(params)}"
    max_retries = 5

    for attempt in range(1, max_retries + 1):
        try:
            r = requests.get(url, timeout=120)
            r.raise_for_status()
            return r.json()

        except (requests.exceptions.ReadTimeout, requests.exceptions.ConnectionError) as e:
            if attempt == max_retries:
                print(f"[ERROR] Catálogo sigue fallando tras {max_retries} intentos.")
                raise

            print(
                f"[WARN] Error de conexión con el catálogo "
                f"(intento {attempt}/{max_retries}): {e}"
            )
            time.sleep(5 * attempt)  # backoff 5s, 10s, 15s...


def fetch_all(
    collection: str,
    start_iso: str,
    end_iso: str,
    wkt: str | None,
    top: int,
    max_pages: int,
    orderby: str,
    include_count: bool,
    only_l2a: bool,
    tile: str | None,
    select: str | None,
) -> dict:

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
        time.sleep(0.3)

    out = {"value": all_items}
    if count is not None:
        out["@odata.count"] = count
    return out


def to_flat_df(js: dict) -> pd.DataFrame:
    df = json_normalize(js.get("value", []))
    if not df.empty:
        first = [
            c
            for c in [
                "Id",
                "Name",
                "ContentDate.Start",
                "ContentDate.End",
                "ContentType",
                "ContentLength",
                "OriginDate",
                "GeoFootprint",
            ]
            if c in df.columns
        ]
        rest = [c for c in df.columns if c not in first]
        df = df[first + rest]
    return df


# -------------------------------
# Descarga ZIP desde zipper
# -------------------------------
def download_product_zip(
    session: requests.Session,
    product_id: str,
    identifier: str,
    out_dir: str,
    overwrite: bool = False,
) -> Path:
    """
    Descarga el producto completo (.SAFE) como ZIP desde zipper.dataspace.copernicus.eu
    usando el token de la sesión.
    """

    url = f"{ZIPPER_BASE}({product_id})/$value"

    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    out_zip = out_dir / f"{identifier}.zip"

    if out_zip.exists() and not overwrite:
        print(f"✔ Ya existe {out_zip}, se omite la descarga.")
        return out_zip

    max_retries = 3
    backoff = 5
    last_exc = None

    for attempt in range(1, max_retries + 1):
        try:
            resp = session.get(url, stream=True, timeout=600, allow_redirects=True)
            resp.raise_for_status()
            with open(out_zip, "wb") as f:
                for chunk in resp.iter_content(chunk_size=1024 * 1024):
                    if chunk:
                        f.write(chunk)
            print(f"💾 ZIP guardado en: {out_zip}")
            return out_zip
        except requests.RequestException as e:
            last_exc = e
            print(f"[{identifier}] Error de descarga (intento {attempt}/{max_retries}): {e}")
            if attempt < max_retries:
                print(f"   Reintentando en {backoff} s...")
                time.sleep(backoff)
                backoff *= 2

    if last_exc:
        raise last_exc


# -------------------------------
# Selección de JP2 dentro del ZIP
# -------------------------------
def build_patterns(mode: str, bands: list[str] | None, collection: str) -> list[str]:
    mode = (mode or "").lower()
    pats: list[str] = []

    if mode == "tci":
        # Intentar primero TCI; si no hay, coger B02/B03/B04 para poder hacer RGB
        pats = [
            "*IMG_DATA*/R10m/*TCI*.jp2",
            "*IMG_DATA_R10m*/*TCI*.jp2",
            "*IMG_DATA*/R10m/*B02*.jp2",
            "*IMG_DATA*/R10m/*B03*.jp2",
            "*IMG_DATA*/R10m/*B04*.jp2",
        ]
        return pats

    if mode == "bands" and bands:
        b = [x.strip().upper() for x in bands]
        for band in b:
            pats.append(f"*IMG_DATA*/R10m/*{band}*.jp2")
        return pats

    if mode in ("rgb", "auto"):
        return [
            "*IMG_DATA*/R10m/*B02*.jp2",
            "*IMG_DATA*/R10m/*B03*.jp2",
            "*IMG_DATA*/R10m/*B04*.jp2",
        ]

    return ["*IMG_DATA*/R10m/*.jp2"]


def extract_selected_from_zip(
    zip_path: Path,
    mode: str,
    bands: list[str] | None,
    out_dir: str,
    collection: str,
) -> list[Path]:
    pats = build_patterns(mode, bands, collection)
    extracted: list[Path] = []

    with zipfile.ZipFile(zip_path, "r") as zf:
        members = zf.namelist()

        to_get = [m for m in members if any(fnmatch.fnmatch(m, p) for p in pats)]

        if not to_get:
            print(f"⚠ No se encontraron archivos que coincidan con {mode}/{bands}")
            return []

        base_out = Path(out_dir) / zip_path.stem / "extracted"
        base_out.mkdir(parents=True, exist_ok=True)

        for m in to_get:
            name = Path(m).name
            h = hashlib.sha1(m.encode()).hexdigest()[:8]
            out_file = base_out / f"{h}_{name}"

            with zf.open(m) as src, open(out_file, "wb") as dst:
                dst.write(src.read())

            extracted.append(out_file)

    return extracted


# -------------------------------
# JP2 → PNG / TIFF
# -------------------------------
def jp2_to_png(src: str, dst: str):
    with rasterio.open(src) as src_jp2:
        rgb = src_jp2.read()
        if rgb.shape[0] == 1:
            rgb = np.repeat(rgb, 3, axis=0)
        elif rgb.shape[0] >= 3:
            rgb = rgb[:3, :, :]

    rgb = rgb.astype("float32")
    m = rgb.max()
    if m <= 0:
        m = 1.0
    rgb = (255 * (rgb / m)).astype(np.uint8)
    rgb = np.transpose(rgb, (1, 2, 0))
    Image.fromarray(rgb).save(dst)
    print("PNG generado:", dst)


def jp2_to_rgb_png(b02: Path, b03: Path, b04: Path, dst_png: Path):
    with rasterio.open(b04) as r4:
        red = r4.read(1).astype("float32")
    with rasterio.open(b03) as r3:
        green = r3.read(1).astype("float32")
    with rasterio.open(b02) as r2:
        blue = r2.read(1).astype("float32")

    def stretch(band):
        mini, maxi = np.percentile(band, (2, 98))
        if maxi - mini == 0:
            return np.zeros_like(band, dtype=np.uint8)
        band = np.clip((band - mini) / (maxi - mini), 0, 1)
        return (band * 255).astype(np.uint8)

    rgb = np.dstack([stretch(red), stretch(green), stretch(blue)])
    dst_png.parent.mkdir(parents=True, exist_ok=True)
    Image.fromarray(rgb).save(dst_png)
    print(f"🖼 PNG RGB generado: {dst_png}")


def ensure_gdal_translate():
    """Asegura que exista gdal_translate en el PATH, si no lanza RuntimeError."""
    exe = shutil.which("gdal_translate")
    if exe is None:
        raise RuntimeError(
            "No se encontró 'gdal_translate' en el PATH.\n"
            "Instala GDAL (por ejemplo con conda: 'conda install gdal') "
            "y asegúrate de que 'gdal_translate' esté en las variables de entorno."
        )
    return exe


def gdal_jp2_to_tiff(src_jp2: Path, dst_tif: Path):
    """
    Usa gdal_translate para convertir JP2 -> GeoTIFF.
    Solo se llama si gdal_translate está disponible.
    """
    exe = ensure_gdal_translate()
    args = [
        exe,
        "-of",
        "GTiff",
        src_jp2.as_posix(),
        dst_tif.as_posix(),
    ]
    subprocess.check_call(args)


def batch_convert_extracted(extract_root: Path, out_dir: Path):
    """
    Para todos los JP2 en extract_root:
      1) Usar GDAL (gdal_translate) para convertir a GeoTIFF (.tif)
      2) Si hay TCI, generar PNG rápido
      3) Si existen B02/B03/B04, generar RGB_truecolor.png

    GeoTIFF -> out_dir / "tiff"
    PNG     -> out_dir / "png"
    """
    jp2s = list(extract_root.rglob("*.jp2"))
    if not jp2s:
        print("No hay archivos JP2 para convertir.")
        return 0

    tiff_dir = out_dir / "tiff"
    png_dir = out_dir / "png"
    tiff_dir.mkdir(parents=True, exist_ok=True)
    png_dir.mkdir(parents=True, exist_ok=True)

    # Comprobamos GDAL aquí (lanzará error claro si no está instalado)
    ensure_gdal_translate()

    n = 0
    b02 = b03 = b04 = None

    for jp2 in jp2s:
        # 1) JP2 -> GeoTIFF (GDAL)
        dst_tif = tiff_dir / (jp2.stem + ".tif")
        try:
            gdal_jp2_to_tiff(jp2, dst_tif)
            print(f"GeoTIFF generado: {dst_tif}")
            n += 1
        except Exception as e:
            print(f"⚠️ Error convirtiendo a GeoTIFF {jp2.name}: {e}")

        # 2) TCI -> PNG
        up = jp2.name.upper()
        if "TCI" in up:
            png = png_dir / (jp2.stem + ".png")
            try:
                jp2_to_png(jp2.as_posix(), png.as_posix())
            except Exception as e:
                print(f"⚠️ Error generando PNG para {jp2.name}: {e}")

        # 3) registrar B02/B03/B04
        if "B02" in up:
            b02 = jp2
        if "B03" in up:
            b03 = jp2
        if "B04" in up:
            b04 = jp2

    # 4) Si tenemos B02/B03/B04, componer RGB
    if b02 and b03 and b04:
        rgb_png = png_dir / "RGB_truecolor.png"
        try:
            jp2_to_rgb_png(b02, b03, b04, rgb_png)
        except Exception as e:
            print(f"⚠️ Error generando RGB_truecolor.png: {e}")
    else:
        print("⚠️ No se pudieron encontrar B02/B03/B04 para generar RGB.")

    return n


# -------------------------------
# Worker por producto
# -------------------------------
def worker(item, args):
    identifier = item.get("Name", "").split(".")[0]
    pid = item.get("Id")

    session = get_fresh_session()

    out_zip = download_product_zip(
        session=session,
        product_id=pid,
        identifier=identifier,
        out_dir=args.out_dir,
        overwrite=args.overwrite,
    )

    print(f"[{identifier}] ZIP guardado en: {out_zip}")

    extracted = extract_selected_from_zip(
        zip_path=out_zip,
        mode=args.asset,
        bands=args.bands,
        out_dir=args.out_dir,
        collection=args.collection,
    )

    if not extracted:
        return (identifier, "sin_extract")

    if args.convert:
        extract_root = Path(args.out_dir) / identifier / "extracted"
        batch_convert_extracted(extract_root, Path(args.out_dir))

    return (identifier, "ok")


# -------------------------------
# CLI args
# -------------------------------
def parse_args():
    ap = argparse.ArgumentParser(description="Descargador NeoLumina Copernicus (ES)")

    ap.add_argument("--collection", type=str, default="SENTINEL-2")

    ap.add_argument("--aoi", type=str, choices=["config", "custom"], default="config")
    ap.add_argument("--wkt", type=str, default=None)

    ap.add_argument("--top", type=int, default=20)
    ap.add_argument("--max-pages", type=int, default=5)
    ap.add_argument("--orderby", type=str, default="ContentDate/Start desc")
    ap.add_argument(
        "--select",
        type=str,
        default="Id,Name,ContentDate,ContentType,ContentLength,OriginDate,GeoFootprint",
    )

    ap.add_argument("--download", action="store_true")
    ap.add_argument("--asset", type=str, default="tci")
    ap.add_argument(
        "--bands",
        type=lambda s: [x.strip() for x in s.split(",")] if s else None,
    )
    ap.add_argument("--convert", action="store_true")

    ap.add_argument("--workers", type=int, default=1)
    ap.add_argument("--overwrite", action="store_true")

    ap.add_argument(
        "--out-dir",
        type=str,
        default=os.path.join(OUTDIR, "satelital", "copernicus"),
    )

    return ap.parse_args()


# -------------------------------
# Núcleo reutilizable
# -------------------------------
def _internal_main(args):
    DOWNLOAD_ROOT = Path(args.out_dir)
    DOWNLOAD_ROOT.mkdir(parents=True, exist_ok=True)

    if args.aoi == "config":
        wkt = AOI_WKT
    else:
        wkt = args.wkt

    start_iso = DATE_FROM
    today_iso = DATE_TO

    print("\n========= Consulta Copernicus =========")
    print("Colección:", args.collection)
    print("AOI:", str(wkt)[:50], "...")
    print("Rango:", start_iso, "→", today_iso)
    print("Directorio salida:", DOWNLOAD_ROOT.as_posix())
    print("=======================================\n")

    js = fetch_all(
        collection=args.collection,
        start_iso=start_iso,
        end_iso=today_iso,
        wkt=wkt,
        top=args.top,
        max_pages=args.max_pages,
        orderby=args.orderby,
        include_count=True,
        only_l2a=True,
        tile=None,
        select=args.select,
    )

    df = to_flat_df(js)
    print(df.head())

    if not args.download:
        print("\nConsulta realizada. Usa --download para descargar las imágenes.")
        return df

    products = js.get("value", [])
    if not products:
        print("No se encontraron productos para descargar.")
        return df

    print("\n========= Iniciando descargas =========")
    results = []

    with ThreadPoolExecutor(max_workers=args.workers) as ex:
        futures = [ex.submit(worker, item, args) for item in products]
        for f in as_completed(futures):
            results.append(f.result())

    pd.DataFrame(results, columns=["identifier", "status"]).to_csv(
        DOWNLOAD_ROOT / "download_summary.csv",
        index=False,
    )

    print("\nDescarga completada. Resumen guardado en:", DOWNLOAD_ROOT)
    return df


# -------------------------------
# main (para ejecución por terminal)
# -------------------------------
def main():
    args = parse_args()

    # Si no se especifica out_dir, usar la ruta estándar de OUTDIR
    if not args.out_dir:
        DOWNLOAD_ROOT = Path(OUTDIR) / "satelital" / "copernicus"
        DOWNLOAD_ROOT.mkdir(parents=True, exist_ok=True)
        args.out_dir = DOWNLOAD_ROOT.as_posix()

    return _internal_main(args)


# -------------------------------
# run(...) (para usar desde otros módulos / main.py)
# -------------------------------
def run(
    collection: str = "SENTINEL-2",
    aoi: str = "config",
    wkt: str | None = None,
    top: int = 20,
    max_pages: int = 5,
    orderby: str = "ContentDate/Start desc",
    select: str = "Id,Name,ContentDate,ContentType,ContentLength,OriginDate,GeoFootprint",
    download: bool = False,
    asset: str = "tci",
    bands: list[str] | None = None,
    convert: bool = False,
    workers: int = 1,
    overwrite: bool = False,
    out_dir: str | None = None,
):
    class Obj:
        """Contenedor simple para simular argparse.Namespace"""
        pass

    args = Obj()
    args.collection = collection
    args.aoi = aoi
    args.wkt = wkt
    args.top = top
    args.max_pages = max_pages
    args.orderby = orderby
    args.select = select
    args.download = download
    args.asset = asset
    args.bands = bands
    args.convert = convert
    args.workers = workers
    args.overwrite = overwrite

    if out_dir is None:
        args.out_dir = os.path.join(OUTDIR, "satelital", "copernicus")
    else:
        args.out_dir = out_dir

    return _internal_main(args)


if __name__ == "__main__":
    main()
