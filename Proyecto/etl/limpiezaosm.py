#!/usr/bin/env python3
# add_province_column_and_clean.py
"""
Uso:
  python add_province_column_and_clean.py /ruta/input.csv /ruta/output.csv
Si no pasas argumentos, edita INPUT_CSV / OUTPUT_CSV en la cabecera.
"""

import os
import sys
import unicodedata
import pandas as pd
import numpy as np

# -----------------------------
# CONFIGURA AQUÍ (si no pasas argumentos)
# -----------------------------
INPUT_CSV  = r"data/transporte/muni_station_metrics_reduced.csv"
OUTPUT_CSV = r"data/transporte/muni_station_metrics_with_prov.csv"

# -----------------------------
# Mapa INE 2 dígitos → nombre oficial
# -----------------------------
INE_PROV_MAP = {
    "01": "Álava", "02": "Albacete", "03": "Alicante", "04": "Almería", "05": "Ávila",
    "06": "Badajoz", "07": "Islas Baleares", "08": "Barcelona", "09": "Burgos", "10": "Cáceres",
    "11": "Cádiz", "12": "Castellón", "13": "Ciudad Real", "14": "Córdoba", "15": "A Coruña",
    "16": "Cuenca", "17": "Girona", "18": "Granada", "19": "Guadalajara", "20": "Gipuzkoa",
    "21": "Huelva", "22": "Huesca", "23": "Jaén", "24": "León", "25": "Lleida",
    "26": "La Rioja", "27": "Lugo", "28": "Madrid", "29": "Málaga", "30": "Murcia",
    "31": "Navarra", "32": "Ourense", "33": "Asturias", "34": "Palencia", "35": "Las Palmas",
    "36": "Pontevedra", "37": "Salamanca", "38": "Santa Cruz de Tenerife", "39": "Cantabria",
    "40": "Segovia", "41": "Sevilla", "42": "Soria", "43": "Tarragona", "44": "Teruel",
    "45": "Toledo", "46": "Valencia", "47": "Valladolid", "48": "Bizkaia", "49": "Zamora",
    "50": "Zaragoza", "51": "Ceuta", "52": "Melilla"
}

# Columnas de conteo que queremos revisar
COUNT_COLS = [
    "stations_count", "stations_unique", "operator_count",
    "stations_within_1km_count", "stations_within_5km_count",
    "stations_in_muni_plus_1km_count", "stations_in_muni_plus_5km_count",
    "accessible_count"
]

# -----------------------------
# Utilidades
# -----------------------------
def read_csv_flexible(path):
    try:
        return pd.read_csv(path, low_memory=False, encoding="utf-8")
    except Exception:
        return pd.read_csv(path, low_memory=False, encoding="latin1")

def fix_mojibake(s):
    if not isinstance(s, str):
        return s
    try:
        repaired = s.encode("latin1").decode("utf-8")
        if repaired == s and "Ã" not in s and "Â" not in s:
            return s
        return repaired
    except Exception:
        return s

def normalize_unicode_str(s):
    if not isinstance(s, str):
        return s
    return unicodedata.normalize("NFC", s).strip()

def normalize_lau_name(val):
    if pd.isna(val):
        return val
    v = str(val).strip()
    v = fix_mojibake(v)
    v = normalize_unicode_str(v)
    # capitalización razonable
    v_title = v.title()
    for small in [" De ", " Del ", " La ", " Las ", " El ", " Y ", " A "]:
        v_title = v_title.replace(small, small.lower())
    return v_title

# -----------------------------
# Flujo principal
# -----------------------------
def main(input_path, output_path=None):
    if not os.path.exists(input_path):
        print("Error: archivo no encontrado:", input_path)
        sys.exit(1)

    print("Cargando:", input_path)
    df = read_csv_flexible(input_path)
    n_rows = len(df)
    print(f"Filas: {n_rows}, Columnas: {len(df.columns)}")

    # 1) Derivar PROV_NAME si no existe (desde LAU_ID)
    if "PROV_NAME" not in df.columns or df["PROV_NAME"].isna().all():
        if "LAU_ID" in df.columns:
            df["LAU_ID"] = df["LAU_ID"].astype(str).str.zfill(5)
            df["INE_PROV_CODE"] = df["LAU_ID"].str[:2]
            df["PROV_NAME"] = df["INE_PROV_CODE"].map(INE_PROV_MAP)
        else:
            df["PROV_NAME"] = None

    # 2) Normalizar LAU_NAME (reparar mojibake simple y normalizar)
    if "LAU_NAME" in df.columns:
        df["LAU_NAME"] = df["LAU_NAME"].apply(normalize_lau_name)

    # 3) Mover PROV_NAME al principio
    cols = df.columns.tolist()
    if "PROV_NAME" in cols:
        cols.remove("PROV_NAME")
        cols = ["PROV_NAME"] + cols
        df = df[cols]

    # 4) Detectar columnas numéricas y formatear a 3 decimales (no cambiar NaN)
    numeric_candidate = df.apply(lambda col: pd.to_numeric(col, errors="coerce"))
    numeric_cols = [c for c in df.columns if numeric_candidate[c].notna().sum() > 0]
    for c in numeric_cols:
        df[c] = pd.to_numeric(df[c], errors="coerce").round(3)

    # 5) Conteo de ceros y porcentaje por columna numérica
    zeros_summary = []
    for c in numeric_cols:
        ser = pd.to_numeric(df[c], errors="coerce")
        zeros = int((ser == 0).sum())
        pct = (zeros / n_rows * 100) if n_rows else 0.0
        zeros_summary.append((c, zeros, round(pct, 3)))

    # 6) Conteo de nulos y porcentaje para todas las columnas
    nulls_summary = []
    for c in df.columns:
        nulls = int(df[c].isna().sum())
        pct_null = (nulls / n_rows * 100) if n_rows else 0.0
        nulls_summary.append((c, nulls, round(pct_null, 3)))

    # 7) Imprimir resúmenes
    print("\n--- Ceros por columna numérica ---")
    print(f"{'columna':40s} {'ceros':>8s} {'% total':>10s}")
    for col, zeros, pct in zeros_summary:
        print(f"{col:40s} {zeros:8d} {pct:10.3f}")

    print("\n--- Nulos por columna ---")
    print(f"{'columna':40s} {'nulos':>8s} {'% total':>10s}")
    for col, nulls, pct in nulls_summary:
        print(f"{col:40s} {nulls:8d} {pct:10.3f}")

    # 8) Guardar CSV con PROV_NAME al principio y numéricos a 3 decimales
    if output_path:
        os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)
        df.to_csv(output_path, index=False, encoding="utf-8-sig", float_format="%.4f")
        print("\nCSV guardado en:", output_path)
    else:
        print("\nNo se guardó archivo. Pasa ruta de salida como segundo argumento para guardar.")

    # devolver objetos útiles
    zeros_df = pd.DataFrame(zeros_summary, columns=["column", "zeros", "pct_total"])
    nulls_df = pd.DataFrame(nulls_summary, columns=["column", "nulls", "pct_total"])
    return df, zeros_df, nulls_df

if __name__ == "__main__":
    inp = sys.argv[1] if len(sys.argv) > 1 else INPUT_CSV
    outp = sys.argv[2] if len(sys.argv) > 2 else OUTPUT_CSV
    main(inp, outp)
