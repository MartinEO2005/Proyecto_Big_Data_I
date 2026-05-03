# renta_municipios.py
import requests
import pandas as pd
from io import StringIO
import os
T_ID = 30824  # Tabla nacional de municipios

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

def descargar_tabla():
    url = f"https://www.ine.es/jaxiT3/files/t/csv_bd/{T_ID}.csv"
    r = requests.get(url)
    r.raise_for_status()
    r.encoding = "utf-8"
    text = r.text
    sep = "\t" if "\t" in text.splitlines()[0] else ";"
    df = pd.read_csv(StringIO(text), sep=sep, low_memory=False)
    df.columns = df.columns.str.replace("\ufeff", "", regex=False).str.strip()
    return df

def limpiar_valor(s):
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

def procesar(indicador="Renta neta media por persona"):
    df = descargar_tabla()
    df = df[df["Distritos"].isna() & df["Secciones"].isna()].copy()
    col_ind = [c for c in df.columns if "renta" in c.lower()][0]
    df = df[df[col_ind] == indicador].copy()

    extra = df["Municipios"].str.extract(r"^(?P<codigo_municipio>\d{5})\s+(?P<nombre_municipio>.+)$")
    df["codigo_municipio"] = extra["codigo_municipio"]
    df["nombre_municipio"] = extra["nombre_municipio"]
    df["codigo_provincia"] = df["codigo_municipio"].str[:2]
    df["provincia"] = df["codigo_provincia"].map(MAPEO_PROVINCIAS)

    df["valor"] = limpiar_valor(df["Total"])
    df["anio"] = pd.to_numeric(df["Periodo"], errors="coerce").astype("Int64")

    return df[["codigo_municipio", "nombre_municipio", "provincia", "anio", "valor"]]

def fetch_renta_municipios_and_save(base_outdir="data"):
    df = procesar()
    out_path = f"{base_outdir}/renta/renta_municipios.csv"
    os.makedirs(f"{base_outdir}/renta", exist_ok=True)
    df.to_csv(out_path, index=False, encoding="utf-8")
    return out_path
