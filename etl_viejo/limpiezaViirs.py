import pandas as pd
import unicodedata
from tqdm import tqdm

# -----------------------------
# Configuración de entrada/salida
# -----------------------------
MUNI_CSV = "data/luz_nocturna/municipios/luz_nocturna/viirs_luz_nocturna.csv"      # LAU_ID, LAU_NAME, date, mean (municipal), ...
PROV_CSV = "data/luz_nocturna/provincias/viirs_provincias_2018_2022.csv"   # PROV_CODE, PROV_NAME, date, mean (provincial)
OUTPUT_CSV = "data/clean/viirsFinal_limpio.csv"   # archivo resultante

# -----------------------------
# Funciones de normalización
# -----------------------------
def fix_mojibake(s: str) -> str:
    """Repara mojibake típico (UTF-8 leído como Latin-1)."""
    if not isinstance(s, str):
        return s
    try:
        return s.encode("latin1").decode("utf-8")
    except (UnicodeEncodeError, UnicodeDecodeError):
        return s

def strip_accents(s: str) -> str:
    """Quita acentos para comparación (no para salida final)."""
    return "".join(c for c in unicodedata.normalize("NFD", s) if unicodedata.category(c) != "Mn")

# Alias bilingües/exónimos → forma canónica (ajusta si tu CSV provincial usa otras formas)
ALIAS_TO_INE = {
    "valencia": "Valencia", "castellon": "Castellón", "castello": "Castellón", "castelló": "Castellón",
    "alicante": "Alicante", "alacant": "Alicante", "alicante/alacant": "Alicante",
    "girona": "Girona", "gerona": "Girona",
    "lleida": "Lleida", "lerida": "Lleida", "lérida": "Lleida",
    "barcelona": "Barcelona", "tarragona": "Tarragona",
    "bizkaia": "Bizkaia", "vizcaya": "Bizkaia",
    "gipuzkoa": "Gipuzkoa", "guipuzcoa": "Gipuzkoa", "guipúzcoa": "Gipuzkoa",
    "alava": "Álava", "araba": "Álava",
    "a coruna": "A Coruña", "la coruna": "A Coruña", "la coruña": "A Coruña", "a coruña": "A Coruña",
    "pontevedra": "Pontevedra", "lugo": "Lugo", "ourense": "Ourense",
    "las palmas": "Las Palmas",
    "santa cruz de tenerife": "Santa Cruz de Tenerife", "sta. cruz de tenerife": "Santa Cruz de Tenerife",
    "cadiz": "Cádiz", "cordoba": "Córdoba", "malaga": "Málaga", "avila": "Ávila",
    "sevilla": "Sevilla", "jaen": "Jaén"
}

def normalize_prov_name(name: str) -> str:
    """Normaliza PROV_NAME del CSV provincial a forma canónica INE."""
    if pd.isna(name):
        return name
    repaired = fix_mojibake(name).strip()
    lowered = strip_accents(repaired.lower())

    if lowered in ALIAS_TO_INE:
        return ALIAS_TO_INE[lowered]

    if "/" in lowered:
        parts = [p.strip() for p in lowered.split("/")]
        for p in parts:
            if p in ALIAS_TO_INE:
                return ALIAS_TO_INE[p]
        return repaired.split("/")[0].strip()

    return repaired

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

# -----------------------------
# Flujo principal
# -----------------------------
def main():
    tqdm.pandas(desc="Normalizando PROV_NAME")

    # 1) Cargar municipal
    df_muni = pd.read_csv(MUNI_CSV)

    # 2) Cargar provincial con intento de codificación
    try:
        df_prov = pd.read_csv(PROV_CSV, encoding="utf-8")
    except UnicodeDecodeError:
        df_prov = pd.read_csv(PROV_CSV, encoding="latin1")

    # 3) Normalizar fechas a mensual (misma resolución)
    df_muni["date"] = pd.to_datetime(df_muni["date"]).dt.to_period("M").astype(str)
    df_prov["date"] = pd.to_datetime(df_prov["date"]).dt.to_period("M").astype(str)

    # 4) Derivar PROV_NAME desde LAU_ID en municipal
    df_muni["LAU_ID"] = df_muni["LAU_ID"].astype(str).str.zfill(5)
    df_muni["INE_PROV_CODE"] = df_muni["LAU_ID"].str[:2]
    df_muni["PROV_NAME"] = df_muni["INE_PROV_CODE"].map(INE_PROV_MAP)

    missing_prov_names = int(df_muni["PROV_NAME"].isna().sum())
    print(f"Provincias faltantes derivadas de LAU_ID: {missing_prov_names}")

    # 4b) Reparar LAU_NAME en municipal (evita "Villamalea" -> "Villamalea" mal mostrado)
    if "LAU_NAME" in df_muni.columns:
        df_muni["LAU_NAME"] = df_muni["LAU_NAME"].apply(fix_mojibake)

    # 5) Reparar mojibake si detectado en PROV_NAME del provincial
    if df_prov["PROV_NAME"].astype(str).str.contains("Ã").any():
        df_prov["PROV_NAME"] = df_prov["PRO_NAME"].apply(fix_mojibake) if "PRO_NAME" in df_prov.columns else df_prov["PROV_NAME"].apply(fix_mojibake)

    # 6) Normalizar nombres provinciales del CSV de provincias (barra de progreso)
    df_prov["PROV_NAME"] = df_prov["PROV_NAME"].progress_apply(normalize_prov_name)

    # 7) Conservar solo mean provincial y renombrar
    df_prov = df_prov[["PROV_NAME", "date", "mean"]].rename(columns={"mean": "mean_prov"})

    # 8) Merge por PROV_NAME + date (left para no perder municipios)
    df_final = df_muni.merge(df_prov, on=["PROV_NAME", "date"], how="left")

    # 9) Diagnóstico y Relleno de nulos (Imputación Temporal)
    missing_mean = int(df_final["mean_prov"].isna().sum())
    if missing_mean > 0:
        print(f"⚠️ Detectados {missing_mean} nulos en 'mean_prov'. Aplicando relleno temporal...")
        
        # Primero ordenamos por provincia y fecha para que el relleno sea coherente
        df_final = df_final.sort_values(by=["PROV_NAME", "date"])
        
        # 'ffill' (forward fill) propaga el último valor válido hacia adelante
        # 'bfill' (backward fill) por si el nulo es el primer dato de la serie
        df_final["mean_prov"] = df_final.groupby("PROV_NAME")["mean_prov"].ffill().bfill()
        
        new_missing = int(df_final["mean_prov"].isna().sum())
        print(f"✅ Nulos restantes tras imputación: {new_missing}")

    # 10) Reordenar columnas: primero provinciales (PROV_NAME, date, mean_prov)
    prov_cols = ["PROV_NAME", "date", "mean_prov"]
    other_cols = [c for c in df_final.columns if c not in prov_cols]
    df_final = df_final[prov_cols + other_cols]

    # 11) Ordenar por date, PROV_NAME y LAU_NAME (alfabético de provincia y municipio)
    if "LAU_NAME" not in df_final.columns:
        df_final["LAU_NAME"] = df_final.get("LAU_NAME", "")
    df_final["date_sort"] = pd.to_datetime(df_final["date"]).dt.to_period("M")
    df_final = df_final.sort_values(by=["date_sort", "PROV_NAME", "LAU_NAME"])
    df_final = df_final.drop(columns=["date_sort"])

    # Eliminar YEAR e INE_PROV_CODE (tal como pediste)
    df_final = df_final.drop(columns=["YEAR", "INE_PROV_CODE", "POP_2023"], errors="ignore")

    # 12) Exportar CSV con BOM para Excel (utf-8-sig)
    df_final.to_csv(OUTPUT_CSV, index=False, encoding="utf-8-sig", float_format="%.3f")
    print(f"✅ Exportado: {OUTPUT_CSV} (encoding utf-8-sig)")

    # Info adicional
    print(f"🔢 Filas: {len(df_final)}, Columnas: {len(df_final.columns)}")
    print("📦 Columnas (primeras 12):", df_final.columns.tolist()[:12])

if __name__ == "__main__":
    main()
