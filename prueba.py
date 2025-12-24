import requests
import pandas as pd
from io import StringIO
import re

# ============================================================
# 1. LISTA COMPLETA DE LOS 52 t= CORRECTOS
# ============================================================

def extraer_t_ids():
    return [
        # --- Provincias con t= estándar ---
        31001, 31002, 31003, 31004, 31005,
        # 31006 (Badajoz) sustituido por 30878
        # 31007 (Barcelona) sustituido por 30897
        # 31008 (Burgos) sustituido por 30926
        # 31009 (Cáceres) sustituido por 30936
        # 31010 (Cádiz) sustituido por 30945
        # 31011 (Castellón) sustituido por 30962
        # 31012 (Ciudad Real) sustituido por 30971
        # 31013 (Córdoba) sustituido por 30980
        # 31014 (A Coruña) sustituido por 30989
        # 31015 (Cuenca) sustituido por 30998
        # 31016 (Girona) sustituido por 31016 (correcto)
        31016,
        # 31017 (Granada) estándar
        31017, 31018, 31019, 31020, 31021, 31022, 31023, 31024,
        # 31025 (Lleida) estándar
        31025,
        # 31026 (La Rioja) sustituido por 31169
        # 31027 (Lugo) estándar
        31027,
        # 31028 (Madrid) estándar
        31028,
        # 31029 (Málaga) estándar
        31029,
        # 31030 (Murcia) estándar
        31030,
        # 31031 (Navarra) estándar
        31031,
        # 31032 (Ourense) estándar
        31032,
        # 31033 (Asturias) sustituido por 30860
        # 31034 (Palencia) estándar
        31034,
        # 31035 (Las Palmas) sustituido por 31151
        # 31036 (Pontevedra) estándar
        31036,
        # 31037 (Salamanca) estándar
        31037,
        # 31038 (Tenerife) estándar
        31038,
        # 31039 (Cantabria) estándar
        31039,
        # 31040 (Segovia) estándar
        31040,
        # 31041 (Sevilla) estándar
        31041,
        # 31042 (Soria) sustituido por 31214
        # 31043 (Tarragona) estándar
        31043,
        # 31044 (Teruel) sustituido por 31232
        # 31045 (Toledo) estándar
        31045,
        # 31046 (Valencia) estándar
        31046,
        # 31047 (Valladolid) estándar
        31047,
        # 31048 (Bizkaia) estándar
        31048,
        # 31049 (Zamora) estándar
        31049,
        # 31050 (Zaragoza) estándar
        31050,

        # --- Provincias con t= especial ---
        30878,  # Badajoz
        30897,  # Barcelona
        30926,  # Burgos
        30936,  # Cáceres
        30945,  # Cádiz
        30962,  # Castellón
        30971,  # Ciudad Real
        30980,  # Córdoba
        30989,  # A Coruña
        30998,  # Cuenca
        31070,  # León
        31169,  # La Rioja
        30860,  # Asturias
        31151,  # Las Palmas
        31214,  # Soria
        31232,  # Teruel

        # --- Ceuta y Melilla ---
        31294,  # Ceuta
        31295   # Melilla
    ]

# ============================================================
# 2. DESCARGA CSV JAXI
# ============================================================

def descargar_tabla_jaxi(t_id: int) -> pd.DataFrame:
    url = f"https://www.ine.es/jaxiT3/files/t/csv_bd/{t_id}.csv"
    r = requests.get(url)
    r.encoding = "utf-8"
    text = r.text

    sample = "\n".join(text.splitlines()[:5])
    sep = "\t" if "\t" in sample else ";"

    df = pd.read_csv(StringIO(text), sep=sep, low_memory=False)
    df.columns = df.columns.str.replace("\ufeff", "", regex=False).str.strip()
    return df

# ============================================================
# 3. LIMPIEZA ROBUSTA DE VALORES
# ============================================================

def limpiar_valor_total(s: pd.Series) -> pd.Series:
    s = s.astype(str).str.strip()
    s = s.replace({"^\\.$": None, "^$": None, "^-$": None}, regex=True)

    mask_comma = s.str.contains(",", na=False)
    s_comma = (
        s[mask_comma]
        .str.replace(".", "", regex=False)
        .str.replace(",", ".", regex=False)
    )

    s_nocomma = s[~mask_comma]
    mask_miles = s_nocomma.str.match(r"^\d{1,3}\.\d{3}$", na=False)
    s_miles = s_nocomma[mask_miles].str.replace(".", "", regex=False)
    s_rest = s_nocomma[~mask_miles].str.replace(".", "", regex=False)

    s_clean = pd.Series(index=s.index, dtype="object")
    s_clean.loc[mask_comma] = s_comma
    s_clean.loc[~mask_comma & mask_miles] = s_miles
    s_clean.loc[~mask_comma & ~mask_miles] = s_rest

    return pd.to_numeric(s_clean, errors="coerce")

# ============================================================
# 4. EXTRAER CÓDIGOS INE
# ============================================================

def extraer_codigos(df):
    s = df["Municipios"].astype(str).str.strip()
    extra = s.str.extract(r"^(?P<codigo_municipio>\d{5})\s+(?P<nombre_municipio>.+)$")
    df["codigo_municipio"] = extra["codigo_municipio"]
    df["nombre_municipio"] = extra["nombre_municipio"]
    df["codigo_provincia"] = df["codigo_municipio"].str[:2]
    return df

# ============================================================
# 5. PROCESAR UNA TABLA t=
# ============================================================

def procesar_t(t_id: int, indicador="Renta neta media por persona"):
    df = descargar_tabla_jaxi(t_id)

    df = df[df["Distritos"].isna() & df["Secciones"].isna()].copy()

    col_ind = [c for c in df.columns if "renta" in c.lower()][0]
    df = df[df[col_ind] == indicador].copy()

    df = extraer_codigos(df)
    df["valor"] = limpiar_valor_total(df["Total"])

    df = df.rename(columns={"Periodo": "anio"})
    df["anio"] = pd.to_numeric(df["anio"], errors="coerce").astype("Int64")

    return df[["codigo_municipio", "nombre_municipio", "codigo_provincia", "anio", "valor"]]

# ============================================================
# 6. UNIFICAR TODAS LAS PROVINCIAS
# ============================================================

def unificar_todas_provincias():
    t_ids = extraer_t_ids()
    print("Procesando t=", t_ids)

    dfs = []
    for t in t_ids:
        try:
            print(f"Procesando t={t} ...")
            df = procesar_t(t)
            dfs.append(df)
        except Exception as e:
            print(f"Error en t={t}: {e}")

    df_final = pd.concat(dfs, ignore_index=True)
    return df_final

# ============================================================
# EJECUCIÓN
# ============================================================

if __name__ == "__main__":
    df_unificado = unificar_todas_provincias()
    print(df_unificado.head())
    print("Filas totales:", len(df_unificado))

    df_unificado.to_csv("renta_neta_municipios.csv", index=False, encoding='utf-8-sig')
    print("Archivo 'renta_neta_municipios.csv' guardado.")