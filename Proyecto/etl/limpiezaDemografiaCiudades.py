#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import pandas as pd
import re, json, unicodedata, difflib, os
import numpy as np

# --------------------------
# Configuración de Rutas
# --------------------------
MUNI_RAW = "data/demografia/demografia_poblacion_municipios.csv"
GEOJSON = "municipios_es.geojson"
PROV_CSV = "data/demografia/demografia_poblacion_provincias.csv"
MIGRACIONES_CSV = "data/migracion/migracion_interior_municipios.csv" 
OUTPUT = "demografia_municipios_con_provincia.csv"

# --------------------------
# Helpers de Limpieza
# --------------------------
def normalize(s):
    if pd.isna(s): return ""
    t = str(s).lower().strip()
    t = unicodedata.normalize('NFKD', t)
    t = "".join(c for c in t if not unicodedata.combining(c))
    t = t.replace("ñ", "n")
    t = re.sub(r"[^a-z0-9\s]", " ", t)
    t = re.sub(r"\s{2,}", " ", t).strip()
    return t

def clean_municipio_string(text):
    if pd.isna(text): return pd.Series([pd.NA, 'Total'])
    t = str(text).strip()
    cat = 'Total'
    if re.search(r'\b(Hombres?)\b', t, re.IGNORECASE): cat = 'Hombres'
    elif re.search(r'\b(Mujeres?)\b', t, re.IGNORECASE): cat = 'Mujeres'
    
    patterns = [r'\.?\s*Total\.\s*Total habitantes\.\s*Personas\.?$', r'\.?\s*Total habitantes\.\s*Personas\.?$', r'\.?\s*Personas\.?$', r'\.?\s*Total\s*$']
    clean_name = t
    for pat in patterns:
        clean_name = re.sub(pat, '', clean_name, flags=re.IGNORECASE)
    clean_name = re.sub(r'\.?\s*(Hombres?|Mujeres?|Total)\s*$', '', clean_name, flags=re.IGNORECASE)
    return pd.Series([clean_name.strip(' .'), cat])

# --------------------------
# 1) Cargar y Limpiar Municipios
# --------------------------
print("1) Cargando municipios...")
df = pd.read_csv(MUNI_RAW, dtype=str)
df['population'] = pd.to_numeric(df['population'], errors='coerce')
df['year'] = pd.to_numeric(df['year'], errors='coerce').astype('Int64')
df = df.dropna(subset=['population'])

df[['municipio_clean', 'categoria']] = df['municipio'].apply(clean_municipio_string)
df['municipio_norm'] = df['municipio_clean'].apply(normalize)

df_pivot = df.pivot_table(
    index=['municipio_clean', 'municipio_norm', 'year'],
    columns='categoria', values='population', aggfunc='sum'
).reset_index().fillna(0)

# --------------------------
# 2) Construir Diccionarios de Referencia (Mapping)
# --------------------------
print("2) Construyendo diccionarios de referencia...")

# A) Desde Migraciones (Prioridad 1)
master_mapping = {}
if os.path.exists(MIGRACIONES_CSV):
    df_migra = pd.read_csv(MIGRACIONES_CSV, dtype=str)
    df_migra['nom_norm'] = df_migra['nombre_municipio'].apply(normalize)
    master_mapping = df_migra.drop_duplicates('nom_norm').set_index('nom_norm')['codigo_provincia'].to_dict()
    print(f"   - Referencia Migraciones: {len(master_mapping)} municipios.")

# B) Desde GeoJSON (Prioridad 2)
with open(GEOJSON, encoding="utf-8") as f:
    gj = json.load(f)
geo_props = []
for feat in gj.get("features", []):
    p = feat.get("properties", {})
    lid = str(p.get("LAU_ID", ""))
    if len(lid) >= 2:
        geo_props.append({'n': normalize(p.get("LAU_NAME", "")), 'c': lid[:2]})
mapping_geojson = pd.DataFrame(geo_props).drop_duplicates('n').set_index('n')['c'].to_dict()

# ... (mantén tus funciones de normalize y clean_municipio_string arriba) ...

# --------------------------
# NUEVO: DICCIONARIO DE CORRECCIONES MANUALES
# --------------------------
CORRECCIONES_MANUALES = {
    "oza dos rios": "15", # A Coruña
    "cesuras": "15",      # A Coruña
    "cotobade": "36",     # Pontevedra (se fusionó en Cerdedo-Cotobade)
    "atez atetz": "31",   # Navarra
    "novetle novele": "46" # Valencia
}

# ... (Secciones 1 y 2 igual que antes) ...

# --------------------------
# 3) Asignar Provincias (Triple Fallback + Parche Manual)
# --------------------------
print("3) Asignando provincias...")

# Paso 1: Migraciones
df_pivot['region_code'] = df_pivot['municipio_norm'].map(master_mapping)

# Paso 2: GeoJSON
mask = df_pivot['region_code'].isna()
df_pivot.loc[mask, 'region_code'] = df_pivot.loc[mask, 'municipio_norm'].map(mapping_geojson)

# Paso 3: PARCHE MANUAL (Para Oza, Cesuras, etc.)
mask = df_pivot['region_code'].isna()
df_pivot.loc[mask, 'region_code'] = df_pivot.loc[mask, 'municipio_norm'].map(CORRECCIONES_MANUALES)

# Paso 4: Fuzzy Match (Para el resto)
mask = df_pivot['region_code'].isna()
missing_names = df_pivot.loc[mask, 'municipio_norm'].unique()
if len(missing_names) > 0:
    all_refs = {**mapping_geojson, **master_mapping}
    choices = list(all_refs.keys())
    fuzzy_map = {}
    for name in missing_names:
        if not name: continue
        m = difflib.get_close_matches(name, choices, n=1, cutoff=0.75) # Bajamos un poco el cutoff
        if m: fuzzy_map[name] = all_refs[m[0]]
    df_pivot.loc[mask, 'region_code'] = df_pivot.loc[mask, 'municipio_norm'].map(fuzzy_map)

# Limpiar códigos (Asegurar 2 dígitos)
df_pivot['region_code'] = df_pivot['region_code'].astype(str).str.replace(r'\.0$', '', regex=True).str.zfill(2)
df_pivot.loc[df_pivot['region_code'].isin(['nan', 'None', '']), 'region_code'] = pd.NA

# --------------------------
# 4) Merge Provincial y Relleno Temporal (Igual que el anterior)
# --------------------------
# ... (Aquí va la lógica de df_final.groupby('region_code')['population'].ffill().bfill() que pusimos antes) ...

# --------------------------
# 4) Merge Provincial con Reutilización de Datos (Lógica de Relleno)
# --------------------------
print("4) Uniendo con población provincial (Lógica de relleno temporal)...")

# A) Cargar provincias y asegurar formato
df_p = pd.read_csv(PROV_CSV, dtype=str)
df_p['region_code'] = df_p['region_code'].str.zfill(2)
df_p['year'] = pd.to_numeric(df_p['year'], errors='coerce')
df_p['population'] = pd.to_numeric(df_p['population'], errors='coerce')
df_p = df_p.dropna(subset=['region_code', 'year'])

# B) Crear una "Matriz Maestra" de Provincias
# Esto asegura que tengamos el nombre de la provincia siempre disponible por código
prov_names = df_p.drop_duplicates('region_code').set_index('region_code')['region_name'].to_dict()

# C) Unir por Año y Provincia (Merge inicial)
# Primero intentamos el cruce normal
df_final = pd.merge(
    df_pivot, 
    df_p[['region_code', 'year', 'population']], 
    on=['region_code', 'year'], 
    how='left'
)

# D) Rellenar Nombres de Provincia faltantes
df_final['region_name'] = df_final['region_code'].map(prov_names)

# E) RELLENO TEMPORAL (La clave del éxito)
# Ordenamos para que el relleno tenga sentido cronológico
df_final = df_final.sort_values(['region_code', 'year'])

print("   - Rellenando huecos de población provincial...")
# Agrupamos por provincia y rellenamos la población hacia arriba y hacia abajo
# Esto hace que si tenemos datos de 2021, se copien a 1996 y a 2024
df_final['population'] = df_final.groupby('region_code')['population'].ffill().bfill()

# F) Limpieza final
df_final = df_final.rename(columns={
    'municipio_clean': 'municipio', 
    'population': 'provincia_population'
})

# --------------------------
# 5) Exportar
# --------------------------
print("5) Guardando resultado final...")
cols_finales = [
    'region_code', 'region_name', 'year', 'provincia_population', 
    'municipio', 'Total', 'Hombres', 'Mujeres'
]

# Solo las columnas que existen
df_export = df_final[[c for c in cols_finales if c in df_final.columns]]
df_export.to_csv(OUTPUT, index=False, encoding='utf-8-sig')

print(f"✅ ¡Hecho! Población provincial recuperada para todos los años.")