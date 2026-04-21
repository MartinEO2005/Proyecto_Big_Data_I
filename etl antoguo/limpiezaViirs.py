import pandas as pd
import unicodedata
from tqdm import tqdm
import os
import glob

# -----------------------------
# Configuración Dinámica (Ajustada a tu main)
# -----------------------------
BASE_PATH = "data/luz_nocturna"
MUNI_CSV = os.path.join(BASE_PATH, "viirs_luz_nocturna.csv")
# El patrón busca cualquier archivo de provincias (soluciona el tema de los años en el nombre)
PROV_PATTERN = os.path.join(BASE_PATH, "provincias/viirs_provincias_*.csv")

OUTPUT_DIR = "data/clean"
# Ajustado para que coincida con tu reporte final: "viirsFinal_limpio.csv"
OUTPUT_CSV = os.path.join(OUTPUT_DIR, "viirsFinal_limpio.csv")

# -----------------------------
# Funciones de Soporte
# -----------------------------
def fix_mojibake(s: str) -> str:
    if not isinstance(s, str): return s
    try: return s.encode("latin1").decode("utf-8")
    except: return s

def strip_accents(s: str) -> str:
    return "".join(c for c in unicodedata.normalize("NFD", s) if unicodedata.category(c) != "Mn")

# Diccionario de normalización para unión perfecta
ALIAS_TO_INE = {
    "valencia": "Valencia", "castellon": "Castellón", "castello": "Castellón",
    "alicante": "Alicante", "alacant": "Alicante", "girona": "Girona", "gerona": "Girona",
    "lleida": "Lleida", "lerida": "Lleida", "bizkaia": "Bizkaia", "vizcaya": "Bizkaia",
    "gipuzkoa": "Gipuzkoa", "guipuzcoa": "Gipuzkoa", "araba": "Álava", "alava": "Álava",
    "a coruna": "A Coruña", "la coruna": "A Coruña", "ourense": "Ourense", "orense": "Ourense",
    "balears": "Islas Baleares", "baleares": "Islas Baleares"
}

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

def normalize_prov_name(name: str) -> str:
    if pd.isna(name): return name
    repaired = fix_mojibake(name).strip()
    lowered = strip_accents(repaired.lower())
    if lowered in ALIAS_TO_INE: return ALIAS_TO_INE[lowered]
    if "/" in lowered:
        for part in lowered.split("/"):
            if part.strip() in ALIAS_TO_INE: return ALIAS_TO_INE[part.strip()]
    return repaired

# -----------------------------
# Función Principal (Llamada por tu main)
# -----------------------------
def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    
    # 1. Localizar archivo provincial dinámicamente
    prov_files = glob.glob(PROV_PATTERN)
    if not prov_files:
        print("❌ Error: No se encontró archivo VIIRS provincial en data/luz_nocturna/provincias/")
        return
    prov_csv_path = sorted(prov_files)[-1] 

    # 2. Cargar datos
    df_muni = pd.read_csv(MUNI_CSV)
    df_prov = pd.read_csv(prov_csv_path)

    # 3. Limpieza y Normalización
# Reemplaza las líneas de pd.to_datetime por estas:
    df_muni["date"] = pd.to_datetime(df_muni["date"], format='%Y-%m').dt.to_period("M").astype(str)
    df_prov["date"] = pd.to_datetime(df_prov["date"], format='%Y-%m').dt.to_period("M").astype(str)

    # Derivación infalible de provincia por código INE
    df_muni["LAU_ID"] = df_muni["LAU_ID"].astype(str).str.zfill(5)
    df_muni["PROV_NAME"] = df_muni["LAU_ID"].str[:2].map(INE_PROV_MAP)

    # Normalizar nombres del provincial para el merge
    df_prov["PROV_NAME"] = df_prov["PROV_NAME"].apply(normalize_prov_name)
    df_prov = df_prov[["PROV_NAME", "date", "mean"]].rename(columns={"mean": "mean_prov"})

    # 4. Merge y relleno de nulos
    df_final = df_muni.merge(df_prov, on=["PROV_NAME", "date"], how="left")
    df_final = df_final.sort_values(by=["LAU_ID", "date"])
    df_final["mean_prov"] = df_final.groupby("PROV_NAME")["mean_prov"].ffill().bfill()

    # 5. Limpieza de columnas sobrantes
    cols_drop = ["YEAR", "INE_PROV_CODE", "POP_2023", "system:index", ".geo"]
    df_final = df_final.drop(columns=[c for c in cols_drop if c in df_final.columns], errors="ignore")

    # 6. Exportación
    df_final.to_csv(OUTPUT_CSV, index=False, encoding="utf-8-sig", float_format="%.4f")
    print(f"✅ [VIIRS] Limpieza terminada. Archivo en: {OUTPUT_CSV}")

if __name__ == "__main__":
    main()