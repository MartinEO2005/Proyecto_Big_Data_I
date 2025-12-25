#!/usr/bin/env python3
# migracion_interior.py
"""
Descarga y procesa:
"Inmigraciones intermunicipales por municipio de destino, año, sexo y nacionalidad"
INE table t=69743

Salida: data/migracion/migracion_interior_municipios.csv
Filtros aplicados:
 - Solo filas de MUNICIPIOS (sin distritos ni secciones)
 - Años: 2021, 2022, 2023, 2024
 - Sexo: Ambos sexos (valor que contenga 'ambos' o 'total' según la tabla)
 - Nacionalidad: Total (valor que contenga 'total' o 'ambas nacionalidades')
"""

import os
import re
import requests
import pandas as pd
from io import StringIO

T_ID = 69743
OUT_SUBDIR = "migracion"
OUT_FILENAME = "migracion_interior_municipios.csv"

MAPEO_PROVINCIAS = {
    "01": "Álava", "02": "Albacete", "03": "Alicante", "04": "Almería",
    "05": "Ávila", "06": "Badajoz", "07": "Illes Balears", "08": "Barcelona",
    "09": "Burgos", "10": "Cáceres", "11": "Cádiz", "12": "Castellón",
    "13": "Ciudad Real", "14": "Córdoba", "15": "A Coruña", "16": "Cuenca",
    "17": "Girona", "18": "Granada", "19": "Guadalajara", "20": "Gipuzkoa",
    "21": "Huelva", "22": "Huesca", "23": "Jaén", "24": "León",
    "25": "Lleida", "26": "La Rioja", "27": "Lugo", "28": "Madrid",
    "29": "Málaga", "30": "Murcia", "31": "Navarra", "32": "Ourense",
    "33": "Asturias", "34": "Palencia", "35": "Las Palmas", "36": "Pontevedra",
    "37": "Salamanca", "38": "Santa Cruz de Tenerife", "39": "Cantabria",
    "40": "Segovia", "41": "Sevilla", "42": "Soria", "43": "Tarragona",
    "44": "Teruel", "45": "Toledo", "46": "Valencia", "47": "Valladolid",
    "48": "Bizkaia", "49": "Zamora", "50": "Zaragoza", "51": "Ceuta",
    "52": "Melilla"
}

def descargar_csv_jaxi(t_id: int) -> pd.DataFrame:
    url = f"https://www.ine.es/jaxiT3/files/t/csv_bd/{t_id}.csv"
    r = requests.get(url)
    r.encoding = "utf-8"
    text = r.text

    # detectar separador (primera línea)
    first_line = text.splitlines()[0] if text else ""
    sep = "\t" if "\t" in first_line else ";"

    df = pd.read_csv(StringIO(text), sep=sep, low_memory=False)
    df.columns = df.columns.str.replace("\ufeff", "", regex=False).str.strip()
    return df

def limpiar_valor_columna(s: pd.Series) -> pd.Series:
    s = s.astype(str).str.strip()
    s = s.replace({"^\\.$": None, "^$": None, "^-$": None}, regex=True)
    mask_comma = s.str.contains(",", na=False)
    s_comma = s[mask_comma].str.replace(".", "", regex=False).str.replace(",", ".", regex=False)
    s_nocomma = s[~mask_comma]
    mask_miles = s_nocomma.str.match(r"^\d{1,3}\.\d{3}$", na=False)
    s_miles = s_nocomma[mask_miles].str.replace(".", "", regex=False)
    s_rest = s_nocomma[~mask_miles].str.replace(".", "", regex=False)
    out = pd.Series(index=s.index, dtype="object")
    out.loc[mask_comma] = s_comma
    out.loc[~mask_comma & mask_miles] = s_miles
    out.loc[~mask_comma & ~mask_miles] = s_rest
    return pd.to_numeric(out, errors="coerce")

def extraer_codigos_municipio(s_municipios: pd.Series) -> pd.DataFrame:
    """
    Extrae codigo_municipio (5 dígitos) y nombre_municipio desde la columna 'Municipios'
    """
    extra = s_municipios.astype(str).str.strip().str.extract(r"^(?P<codigo_municipio>\d{5})\s+(?P<nombre_municipio>.+)$")
    return extra

def normalizar_colname_busqueda(cols: pd.Index, keywords: list[str]) -> str | None:
    """
    Busca en cols un nombre que contenga todas las keywords (case-insensitive).
    Devuelve el nombre real de la columna o None.
    """
    cols_lower = [c.lower() for c in cols]
    for c, cl in zip(cols, cols_lower):
        if all(k.lower() in cl for k in keywords):
            return c
    return None

def procesar_migracion_municipios(anos=(2021,2022,2023,2024), indicador_sexo_keywords=("ambos","total"), indicador_nac_keywords=("total","ambas")):
    df = descargar_csv_jaxi(T_ID)

    # Filtrar solo MUNICIPIOS (sin distritos ni secciones) si existen esas columnas
    if "Distritos" in df.columns and "Secciones" in df.columns:
        df = df[df["Distritos"].isna() & df["Secciones"].isna()].copy()

    # Identificar columnas relevantes: Periodo, Municipios, Sexo, Nacionalidad, Total/Valor
    col_periodo = normalizar_colname_busqueda(df.columns, ["periodo"]) or normalizar_colname_busqueda(df.columns, ["año"]) or "Periodo"
    col_municipios = normalizar_colname_busqueda(df.columns, ["municipio"]) or "Municipios"
    col_sexo = normalizar_colname_busqueda(df.columns, ["sexo"])  # puede ser 'Sexo'
    col_nacionalidad = normalizar_colname_busqueda(df.columns, ["nacionalidad"]) or normalizar_colname_busqueda(df.columns, ["nacionalid"])  # 'Nacionalidad'
    # columna con el valor numérico (puede llamarse 'Total' o 'Valor' o 'Inmigraciones')
    col_valor = None
    for candidate in ["Total", "Valor", "Inmigraciones", "Número", "Nº"]:
        if candidate in df.columns:
            col_valor = candidate
            break
    if col_valor is None:
        # fallback: la última columna numérica
        numeric_cols = df.select_dtypes(include=["number"]).columns.tolist()
        if numeric_cols:
            col_valor = numeric_cols[-1]
        else:
            # intentar detectar columna que contenga 'total' en su nombre
            col_valor = normalizar_colname_busqueda(df.columns, ["total"]) or df.columns[-1]

    # Filtrar años
    if col_periodo not in df.columns:
        raise RuntimeError(f"No se encontró columna de periodo (buscada como 'Periodo' o 'Año'). Columnas disponibles: {list(df.columns)}")
    df[col_periodo] = pd.to_numeric(df[col_periodo], errors="coerce")
    df = df[df[col_periodo].isin(list(anos))].copy()

    # Filtrar sexo: buscar valores que contengan 'ambos' o 'total' (case-insensitive)
    if col_sexo and col_sexo in df.columns:
        mask_sexo = df[col_sexo].astype(str).str.lower().str.contains("|".join(indicador_sexo_keywords))
        df = df[mask_sexo].copy()
    else:
        # Si no hay columna sexo, asumimos que la tabla ya está desagregada por sexo y no hay filtro posible
        pass

    # Filtrar nacionalidad: buscar valores que contengan 'total' o 'ambas'
    if col_nacionalidad and col_nacionalidad in df.columns:
        mask_nac = df[col_nacionalidad].astype(str).str.lower().str.contains("|".join(indicador_nac_keywords))
        df = df[mask_nac].copy()
    else:
        # Si no hay columna nacionalidad, asumimos que la tabla ya está total
        pass

    # Extraer códigos y nombres de municipios
    if col_municipios not in df.columns:
        raise RuntimeError("No se encontró la columna de Municipios en la tabla.")
    extra = extraer_codigos_municipio(df[col_municipios])
    df["codigo_municipio"] = extra["codigo_municipio"]
    df["nombre_municipio"] = extra["nombre_municipio"]

    # Filtrar solo filas con codigo_municipio válido (municipios)
    df = df[df["codigo_municipio"].notna()].copy()

    # Codigo provincia y nombre provincia
    df["codigo_provincia"] = df["codigo_municipio"].astype(str).str.zfill(5).str[:2]
    df["provincia"] = df["codigo_provincia"].map(MAPEO_PROVINCIAS)

    # Normalizar columna de valor
    df["valor"] = limpiar_valor_columna(df[col_valor])

    # Normalizar columna periodo a 'anio'
    df = df.rename(columns={col_periodo: "anio"})
    df["anio"] = pd.to_numeric(df["anio"], errors="coerce").astype("Int64")

    # Añadir columnas de sexo y nacionalidad con nombres normalizados si existen
    if col_sexo and col_sexo in df.columns:
        df["sexo"] = df[col_sexo].astype(str).str.strip()
    else:
        df["sexo"] = "Ambos sexos"

    if col_nacionalidad and col_nacionalidad in df.columns:
        df["nacionalidad"] = df[col_nacionalidad].astype(str).str.strip()
    else:
        df["nacionalidad"] = "Total"

    # Seleccionar y ordenar columnas finales
    final_cols = ["codigo_municipio", "nombre_municipio", "codigo_provincia", "provincia", "anio", "sexo", "nacionalidad", "valor"]
    df_final = df[final_cols].copy()

    # Orden razonable
    df_final = df_final.sort_values(["codigo_provincia", "codigo_municipio", "anio"]).reset_index(drop=True)

    return df_final

def fetch_migracion_interior_and_save(base_outdir="data"):
    df = procesar_migracion_municipios()
    out_dir = os.path.join(base_outdir, OUT_SUBDIR)
    os.makedirs(out_dir, exist_ok=True)
    out_path = os.path.join(out_dir, OUT_FILENAME)
    df.to_csv(out_path, index=False, encoding="utf-8")
    return out_path

if __name__ == "__main__":
    try:
        path = fetch_migracion_interior_and_save(base_outdir="data")
        print("CSV guardado en:", path)
        df_preview = pd.read_csv(path, nrows=10)
        print(df_preview)
        print("Filas totales:", len(pd.read_csv(path)))
    except Exception as e:
        print("Error:", type(e), e)
