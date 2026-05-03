# storage.py
import os
import pandas as pd

THEME_DIRS = {
    "satelital":            "satelital",
    "transporte":           "transporte",
    "luz_nocturna":         "luz_nocturna",
    "demografia":           "demografia",
    "movilidad":            "movilidad",
    "renta":                "renta",
    "migracion":            "migracion",
    "energia":              "energia",
    "empresas_transporte":  "empresas_transporte",
}

def ensure_theme_dirs(base_outdir):
    base = os.path.abspath(base_outdir)
    os.makedirs(base, exist_ok=True)
    mapping = {}
    for key, sub in THEME_DIRS.items():
        path = os.path.join(base, sub)
        os.makedirs(path, exist_ok=True)
        mapping[key] = path
    return mapping

def save_df_to_theme(df, theme, filename, base_outdir="data/raw"):
    mapping = ensure_theme_dirs(base_outdir)
    if theme not in mapping:
        raise ValueError(f"Tema desconocido: {theme}. Temas válidos: {list(mapping.keys())}")
    outpath = os.path.join(mapping[theme], filename)
    df.to_csv(outpath, index=False)
    return outpath
