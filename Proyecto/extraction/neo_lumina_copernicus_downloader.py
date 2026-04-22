#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import time
import fnmatch
import zipfile
import json
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
import requests
from dotenv import load_dotenv
import numpy as np
import rasterio
from PIL import Image
# ===============================
# 1. ENTORNO Y CONFIGURACIÓN (REFORZADA)
# ===============================

# Forzamos la carga del .env al principio de todo
load_dotenv()

try:
    # Intentamos traer los valores de tu config.py
    from extraction.config import AOI_WKT, DATE_FROM, DATE_TO, OUTDIR, CDSE_USER, CDSE_PASS, TOP as CONFIG_TOP
except ImportError:
    # Si falla la importación, valores por defecto
    AOI_WKT = "POLYGON((-4.2 40.2, -4.2 40.8, -3.2 40.8, -3.2 40.2, -4.2 40.2))"
    DATE_FROM, DATE_TO = "2024-06-01", "2024-08-31"
    OUTDIR = "data"
    CONFIG_TOP = 5
    CDSE_USER = None
    CDSE_PASS = None

# SEGUNDA OPORTUNIDAD: Si las variables están vacías, las buscamos directamente en el sistema (Docker)
if not CDSE_USER:
    CDSE_USER = os.getenv("CDSE_USER")
if not CDSE_PASS:
    CDSE_PASS = os.getenv("CDSE_PASS")

# ÚLTIMO RECURSO: Intentar con nombres alternativos que a veces se usan
if not CDSE_USER:
    CDSE_USER = os.getenv("COPERNICUS_USER")
if not CDSE_PASS:
    CDSE_PASS = os.getenv("COPERNICUS_PASSWORD")

# URLs de Copernicus Data Space Ecosystem
CAT_BASE = "https://catalogue.dataspace.copernicus.eu/odata/v1/Products"
TOKEN_URL = "https://identity.dataspace.copernicus.eu/auth/realms/CDSE/protocol/openid-connect/token"
ZIPPER_BASE = "https://zipper.dataspace.copernicus.eu/odata/v1/Products"

def stretch_2_98(band):
    """Estiramiento de contraste para generar el PNG visual."""
    lower, upper = np.percentile(band, (2, 98))
    if upper <= lower: return np.zeros_like(band, dtype=np.uint8)
    return (np.clip((band - lower) / (upper - lower), 0, 1) * 255).astype(np.uint8)

def process_and_extract(zip_path, identifier, out_dir, raw_data):
    """
    Extrae las bandas R10m, genera el JSON de metadatos para la BBDD
    y crea un PNG visual. Luego borra el ZIP.
    """
    product_dir = Path(out_dir) / identifier
    product_dir.mkdir(parents=True, exist_ok=True)
    
    # Metadatos cruciales para tu entrenamiento y BBDD
    meta_info = {
        "producto_id": identifier,
        "uuid_copernicus": raw_data.get("Id"),
        "fecha_adquisicion": raw_data.get("ContentDate", {}).get("Start"),
        "geometria_wkt": raw_data.get("GeoFootprint"),
        "nivel_nubes": next((a["Value"] for a in raw_data.get("Attributes", []) if a["Name"] == "cloudCover"), "N/A"),
        "resolucion": "10m",
        "timestamp_descarga": time.ctime()
    }
    
    with open(product_dir / f"{identifier}_metadata.json", "w") as jf:
        json.dump(meta_info, jf, indent=4)

    extracted_jp2 = []
    # Buscamos bandas B02, B03, B04 en resolución 10m
    patterns = ["*IMG_DATA*/R10m/*B02*.jp2", "*IMG_DATA*/R10m/*B03*.jp2", "*IMG_DATA*/R10m/*B04*.jp2"]

    with zipfile.ZipFile(zip_path, "r") as zf:
        for member in zf.namelist():
            if any(fnmatch.fnmatch(member, p) for p in patterns):
                target_path = product_dir / Path(member).name
                with zf.open(member) as source, open(target_path, "wb") as target:
                    target.write(source.read())
                extracted_jp2.append(target_path)

    # Generación de la imagen RGB (PNG)
    try:
        b02 = next(f for f in extracted_jp2 if "B02" in f.name)
        b03 = next(f for f in extracted_jp2 if "B03" in f.name)
        b04 = next(f for f in extracted_jp2 if "B04" in f.name)
        
        with rasterio.open(b04) as r4, rasterio.open(b03) as r3, rasterio.open(b02) as r2:
            rgb = np.dstack([stretch_2_98(r4.read(1)), stretch_2_98(r3.read(1)), stretch_2_98(r2.read(1))])
            Image.fromarray(rgb).save(product_dir / f"{identifier}_visual.png")
    except Exception as e:
        print(f"  ⚠️ Aviso: No se pudo generar el PNG para {identifier}: {e}")

def worker(item, out_dir, user, password):
    """Manejador individual de descarga con reintentos."""
    ident = item["Name"].replace(".SAFE", "")
    pid = item["Id"]
    zip_path = Path(out_dir) / f"{ident}.zip"

    for retry in range(3):
        try:
            # Obtención de token fresco
            token_r = requests.post(TOKEN_URL, data={
                "client_id": "cdse-public", "username": user, "password": password, "grant_type": "password"
            }, timeout=30)
            token_r.raise_for_status()
            token = token_r.json()["access_token"]

            print(f"⬇️ Descargando {ident} (Intento {retry+1})...")
            with requests.get(f"{ZIPPER_BASE}({pid})/$value", 
                             headers={"Authorization": f"Bearer {token}"}, 
                             stream=True, timeout=600) as r:
                r.raise_for_status()
                with open(zip_path, "wb") as f:
                    for chunk in r.iter_content(chunk_size=1024*1024):
                        f.write(chunk)
            
            process_and_extract(zip_path, ident, out_dir, item)
            if zip_path.exists(): zip_path.unlink() # Limpieza de ZIP
            return f"✅ {ident}: OK"
        
        except Exception as e:
            print(f"  ❌ Error en {ident}: {e}")
            if retry == 2: return f"FAILED: {ident}"
            time.sleep(10)

def run(top=None, base_outdir=None):
    """
    Función principal llamada por el orquestador (main.py).
    Si se pasa 'top', ignora el valor de config.py.
    Si se pasa 'base_outdir', guarda en base_outdir/satelital/copernicus.
    """
    # Prioridad: 1. Argumento de función | 2. config.py | 3. Valor fijo
    limit = top if top is not None else CONFIG_TOP
    
    print(f"\n\U0001f6f0\ufe0f NeoLumina Downloader")
    print(f"   -> Objetivo: {limit} imágenes")
    print(f"   -> Periodo: {DATE_FROM} a {DATE_TO}")

    if not CDSE_USER or not CDSE_PASS:
        print("   ❌ Error: Credenciales CDSE_USER/CDSE_PASS no encontradas."); return

    if base_outdir is not None:
        dest = Path(base_outdir) / "satelital" / "copernicus"
    else:
        _script_dir = os.path.dirname(os.path.abspath(__file__))
        dest = (Path(_script_dir) / ".." / "data" / "raw" / "satelital" / "copernicus").resolve()
    dest.mkdir(parents=True, exist_ok=True)

    # Construcción de la Query OData
    query = (f"Collection/Name eq 'SENTINEL-2' and ContentDate/Start ge {DATE_FROM}T00:00:00.000Z "
             f"and ContentDate/Start le {DATE_TO}T23:59:59.999Z and OData.CSC.Intersects(area=geography'SRID=4326;{AOI_WKT}') "
             f"and Attributes/OData.CSC.DoubleAttribute/any(att:att/Name eq 'cloudCover' and att/Value lt 15.0) "
             f"and Attributes/OData.CSC.StringAttribute/any(att:att/Name eq 'productType' and att/Value eq 'S2MSI2A')")

    try:
        r = requests.get(CAT_BASE, params={"$filter": query, "$top": limit, "$orderby": "ContentDate/Start desc"})
        r.raise_for_status()
        products = r.json().get("value", [])
        print(f"   -> Encontrados {len(products)} productos para procesar.")
    except Exception as e:
        print(f"   ❌ Error consultando el catálogo: {e}"); return

    # Ejecución paralela controlada (2 workers para evitar baneos de IP)
    with ThreadPoolExecutor(max_workers=2) as executor:
        futures = [executor.submit(worker, p, dest, CDSE_USER, CDSE_PASS) for p in products]
        for f in as_completed(futures): 
            print(f"   {f.result()}")

if __name__ == "__main__":
    # Test local si se ejecuta este archivo solo
    run(top=2)