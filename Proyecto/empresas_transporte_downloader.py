#!/usr/bin/env python3
# empresas_transporte_api_completo.py
"""
Descarga t=4721 desde la API JAXI del INE, filtra por
"Comercio, transporte y hostelería", obtiene provincias y municipios,
convierte a formato ancho (años como columnas) y guarda CSV final.
Incluye depuración para inspeccionar la muestra cruda si algo falla.
"""

import os
import re
import sys
import requests
import pandas as pd
from io import StringIO

T_ID = 4721
URL = f"https://www.ine.es/jaxiT3/files/t/csv_bd/{T_ID}.csv"

OUTDIR = "data/empresas_transporte"
OUT_FINAL = "empresas_transporte_prov_mun_anchos.csv"
OUT_DEBUG = "empresas_transporte_debug_raw_sample.csv"

GRUPO_CNAE = "Comercio, transporte y hostelería"

os.makedirs(OUTDIR, exist_ok=True)

def descargar_texto(url, timeout=60):
    r = requests.get(url, timeout=timeout)
    r.raise_for_status()
    # intentar utf-8 por defecto
    r.encoding = "utf-8"
    return r.text

def detectar_separador(text):
    first = text.splitlines()[0] if text else ""
    if "\t" in first:
        return "\t"
    if ";" in first:
        return ";"
    if "," in first and first.count(",") > first.count(";"):
        return ","
    return ","  # fallback

def leer_dataframe(text):
    sep = detectar_separador(text)
    df = pd.read_csv(StringIO(text), sep=sep, low_memory=False)
    df.columns = df.columns.str.replace("\ufeff", "", regex=False).str.strip()
    return df

def limpiar_numero_valor(v):
    if pd.isna(v):
        return None
    s = str(v).strip()
    if s in ("", "-", "NA", "nan", "NaN"):
        return None
    s = re.sub(r"\s+", "", s)
    # si contiene ambos '.' y ',' asumimos '.' miles y ',' decimal
    if "." in s and "," in s:
        s = s.replace(".", "").replace(",", ".")
    else:
        # si solo puntos y parecen miles (grupos de 3), quitar puntos
        if re.match(r"^\d{1,3}(\.\d{3})+$", s):
            s = s.replace(".", "")
        # si solo comas y parecen miles, quitar comas
        if re.match(r"^\d{1,3}(,\d{3})+$", s):
            s = s.replace(",", "")
        # si solo coma y no hay punto, tratar coma como decimal
        if "," in s and "." not in s:
            s = s.replace(",", ".")
    s = re.sub(r"[^\d\.\-]", "", s)
    try:
        return float(s) if s not in ("", None) else None
    except:
        return None

def elegir_col_valor(df, candidates, col_periodo):
    # probar candidatos explícitos primero
    if candidates:
        for c in candidates:
            sample = df[c].astype(str).head(200).str.replace(r"[^\d\.,\- ]", "", regex=True)
            score = sample.str.match(r"^[\d\.\, \-]+$").sum()
            if score > 0:
                return c
    # fallback: columna con más valores numéricos tras limpieza
    best = None
    best_score = -1
    for c in df.columns:
        if c == col_periodo:
            continue
        cleaned = df[c].astype(str).head(200).apply(limpiar_numero_valor)
        score = cleaned.notna().sum()
        if score > best_score:
            best_score = score
            best = c
    return best

def extraer_codigo_nombre_safe(x):
    s = str(x).strip()
    m = re.match(r"^(\d{2,5})\s+(.+)$", s)
    if m:
        return m.group(1), m.group(2).strip()
    return (None, s)

def procesar():
    text = descargar_texto(URL)
    df = leer_dataframe(text)

    # guardar muestra cruda para depuración
    SAVE_DEBUG = False  # poner True para generar debug_raw_sample
    if SAVE_DEBUG:
        df.head(200).to_csv(os.path.join(OUTDIR, OUT_DEBUG), index=False, encoding="utf-8")

    # detectar columnas relevantes
    cols = [c for c in df.columns]
    col_provincias = next((c for c in cols if c.lower().strip() == "provincias"), None)
    col_municipios = next((c for c in cols if c.lower().strip() == "municipios"), None)
    col_cnae = next((c for c in cols if "cnae" in c.lower() or "grupo" in c.lower() or "actividad" in c.lower()), None)
    col_periodo = next((c for c in cols if re.search(r"period|año|ano|year", c, re.I)), None)

    # candidatos explícitos para valor
    candidates = [c for c in cols if re.search(r"valor|empresas|total|número|numero|unidades|totales", c, re.I)]

    # priorizar 'Total' si existe
    col_valor = "Total" if "Total" in df.columns else (candidates[0] if candidates else None)
    if col_valor is None:
        col_valor = elegir_col_valor(df, candidates, col_periodo)

    if col_cnae is None or col_periodo is None or col_valor is None:
        raise RuntimeError(f"Columnas no detectadas correctamente: provincias={col_provincias}, municipios={col_municipios}, cnae={col_cnae}, periodo={col_periodo}, valor={col_valor}. Revisa {OUT_DEBUG}")

    # construir columna geográfica combinada: preferir Municipios cuando exista, sino Provincias
    if col_municipios and col_provincias:
        df["geo_combined"] = df[col_municipios].astype(str).str.strip()
        mask_empty = df["geo_combined"].isna() | (df["geo_combined"].str.strip() == "") | (df["geo_combined"].str.lower() == "nan")
        df.loc[mask_empty, "geo_combined"] = df.loc[mask_empty, col_provincias].astype(str).str.strip()
        col_geo = "geo_combined"
    else:
        col_geo = col_municipios or col_provincias or next((c for c in cols if "municip" in c.lower() or "prov" in c.lower()), None)

    if col_geo is None:
        raise RuntimeError("No se pudo determinar columna geográfica. Revisa el fichero de depuración.")

    # filtrar por el grupo CNAE
    df = df[df[col_cnae].astype(str).str.contains(GRUPO_CNAE, case=False, na=False)].copy()
    if df.empty:
        raise RuntimeError("El filtro por Grupo CNAE devolvió 0 filas. Revisa el fichero de depuración.")

    # detectar si la tabla viene en formato ancho (años como columnas)
    year_cols = [c for c in df.columns if re.match(r"^\d{4}$", str(c))]
    if year_cols:
        # formato ancho: limpiar y usar directamente
        geo_s = df[col_geo].astype(str)
        mask_geo = geo_s.str.match(r"^\d{2}\s+.+$") | geo_s.str.match(r"^\d{5}\s+.+$")
        mask_geo = mask_geo.fillna(False)
        df = df[mask_geo].copy()
        df["codigo"], df["nombre"] = zip(*df[col_geo].map(extraer_codigo_nombre_safe))
        df["tipo"] = df["codigo"].astype(str).apply(lambda c: "provincia" if len(str(c)) == 2 else ("municipio" if len(str(c)) == 5 else "otro"))
        # limpiar cada columna de año
        for y in year_cols:
            df[y] = df[y].apply(limpiar_numero_valor)
        df[year_cols] = df[year_cols].fillna(0.0)
        # ordenar
        df["prov_code"] = df["codigo"].astype(str).str[:2]
        df["codigo_int"] = pd.to_numeric(df["codigo"], errors="coerce").fillna(0).astype(int)
        df = df.sort_values(["prov_code", "tipo", "codigo_int"]).drop(columns=["prov_code", "codigo_int"])
        cols_out = ["codigo", "nombre", "tipo"] + sorted(year_cols)
        out_path = os.path.join(OUTDIR, OUT_FINAL)
        df[cols_out].to_csv(out_path, index=False, encoding="utf-8")
        return out_path

    # formato largo: pivotar
    # quedarnos solo con filas que representen provincia (NN Nombre) o municipio (NNNNN Nombre)
    geo_s = df[col_geo].astype(str)
    mask_geo = geo_s.str.match(r"^\d{2}\s+.+$") | geo_s.str.match(r"^\d{5}\s+.+$")
    mask_geo = mask_geo.fillna(False)
    df = df[mask_geo].copy()

    df["codigo"], df["nombre"] = zip(*df[col_geo].map(extraer_codigo_nombre_safe))
    df["tipo"] = df["codigo"].astype(str).apply(lambda c: "provincia" if len(str(c)) == 2 else ("municipio" if len(str(c)) == 5 else "otro"))
    df["anio"] = pd.to_numeric(df[col_periodo], errors="coerce").astype("Int64")
    df["valor_clean"] = df[col_valor].apply(limpiar_numero_valor)

    # pivotar a ancho
    df_pivot = df.pivot_table(index=["codigo", "nombre", "tipo"], columns="anio", values="valor_clean", aggfunc="sum").reset_index()

    # normalizar columnas de año a enteros ordenados
    year_cols_pivot = [c for c in df_pivot.columns if isinstance(c, (int,)) or (isinstance(c, str) and re.match(r"^\d{4}$", str(c)))]
    year_cols_int = sorted([int(c) for c in year_cols_pivot])
    # asegurar columnas de año presentes
    for y in year_cols_int:
        if y not in df_pivot.columns:
            df_pivot[y] = 0.0
    cols_order = ["codigo", "nombre", "tipo"] + year_cols_int
    df_pivot = df_pivot[cols_order]
    df_pivot[year_cols_int] = df_pivot[year_cols_int].fillna(0.0)

    # ordenar filas por provincia y tipo
    df_pivot["prov_code"] = df_pivot["codigo"].astype(str).str[:2]
    df_pivot["codigo_int"] = pd.to_numeric(df_pivot["codigo"], errors="coerce").fillna(0).astype(int)
    df_pivot = df_pivot.sort_values(["prov_code", "tipo", "codigo_int"]).drop(columns=["prov_code", "codigo_int"])

    out_path = os.path.join(OUTDIR, OUT_FINAL)
    df_pivot.to_csv(out_path, index=False, encoding="utf-8")
    return out_path

if __name__ == "__main__":
    try:
        print("Iniciando descarga y procesamiento de t=4721 (API JAXI) — provincias y municipios, formato ancho...")
        out = procesar()
        print("CSV generado:", out)
    except Exception as e:
        print("ERROR:", type(e).__name__, e)
        print("He guardado una muestra cruda en", os.path.join(OUTDIR, OUT_DEBUG))
        sys.exit(1)
