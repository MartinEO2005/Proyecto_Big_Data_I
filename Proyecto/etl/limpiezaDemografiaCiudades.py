import pandas as pd
import re

# Función para reparar mojibake típico (UTF-8 leído como Latin-1)
def fix_mojibake(s: str) -> str:
    if not isinstance(s, str):
        return s
    try:
        return s.encode("latin1").decode("utf-8")
    except (UnicodeEncodeError, UnicodeDecodeError):
        return s

# -----------------------------
# 1 Cargar CSV
# -----------------------------
df = pd.read_csv("demografia_poblacion_municipios.csv", dtype=str)

# -----------------------------
# 0. Tipos y limpieza básica
# -----------------------------
# Reparar mojibake en la columna municipio antes de cualquier otra operación
if 'municipio' in df.columns:
    df['municipio'] = df['municipio'].astype(str).apply(fix_mojibake).str.strip()
else:
    raise KeyError("No se encontró la columna 'municipio' en el CSV.")

df['population'] = pd.to_numeric(df['population'], errors='coerce')
df['year'] = pd.to_numeric(df['year'], errors='coerce').astype('Int64')

# 1. Rellenar codigos vacíos para que no se pierdan filas al agrupar
df['cod_prov'] = df.get('cod_prov', pd.Series([''] * len(df))).fillna('').astype(str)
df['cod_muni'] = df.get('cod_muni', pd.Series([''] * len(df))).fillna('').astype(str)

# 2. Quitar sufijos fijos redundantes
def remove_fixed_suffixes(text):
    if pd.isna(text):
        return text
    s = text
    # eliminar las frases repetidas al final (case-insensitive)
    s = re.sub(r'(?i)\s*\.?\s*Total habitantes\s*\.?\s*', ' ', s)
    s = re.sub(r'(?i)\s*\.?\s*Personas\s*\.?\s*', ' ', s)
    # limpiar puntos finales sobrantes y espacios
    s = re.sub(r'\s*\.\s*$', '', s).strip()
    s = re.sub(r'\s{2,}', ' ', s)
    return s.strip()

df['municipio_tmp'] = df['municipio'].apply(remove_fixed_suffixes)

# 3. Extraer nombre y categoría (buscando la primera aparición de Hombres/Mujeres/Total)
def extract_name_and_category(text):
    if pd.isna(text) or text.strip() == '':
        return pd.Series([pd.NA, pd.NA])
    t = text.strip()
    m = re.search(r'\b(Hombres|Hombre|Mujeres|Mujer|Total)\b', t, flags=re.I)
    if m:
        cat_raw = m.group(0)
        name = t[:m.start()].rstrip(' .').strip()
        c = cat_raw.lower()
        if 'hombre' in c:
            cat = 'Hombres'
        elif 'mujer' in c:
            cat = 'Mujeres'
        else:
            cat = 'Total'
    else:
        # fallback: separar por primer punto si existe
        if '.' in t:
            left, right = t.split('.', 1)
            name = left.strip()
            cat = right.strip().capitalize()
            if 'hombre' in cat.lower(): cat = 'Hombres'
            if 'mujer' in cat.lower(): cat = 'Mujeres'
            if 'total' in cat.lower(): cat = 'Total'
        else:
            name = t
            cat = pd.NA
    return pd.Series([name, cat])

df[['municipio_clean', 'categoria']] = df['municipio_tmp'].apply(extract_name_and_category)

# 4. Si municipio_clean quedó vacío, usar municipio sin sufijos
df['municipio_clean'] = df['municipio_clean'].fillna(df['municipio_tmp'])

# 5. Filtrar filas con población válida
df = df[df['population'].notna()]

# 6. Agrupar y pivotar (ahora cod_prov/cod_muni no son NaN)
df_group = (df
            .groupby(['cod_prov', 'cod_muni', 'municipio_clean', 'year', 'categoria'], dropna=False)['population']
            .sum()
            .reset_index())

df_wide = df_group.pivot_table(index=['cod_prov', 'cod_muni', 'municipio_clean', 'year'],
                               columns='categoria',
                               values='population',
                               aggfunc='first').reset_index()

# 7. Asegurar columnas esperadas y tipos
for col in ['Total', 'Hombres', 'Mujeres']:
    if col not in df_wide.columns:
        df_wide[col] = pd.NA

df_wide[['Total','Hombres','Mujeres']] = df_wide[['Total','Hombres','Mujeres']].fillna(0).astype('Int64')

# 8. Orden final
df_wide = df_wide[['cod_prov', 'cod_muni', 'municipio_clean', 'year', 'Total', 'Hombres', 'Mujeres']]
df_wide = df_wide.sort_values(['municipio_clean','year']).reset_index(drop=True)

# 9) Eliminar cod_prov / cod_muni si están completamente vacías
for c in ['cod_prov', 'cod_muni']:
    if c in df_wide.columns:
        if df_wide[c].replace('', pd.NA).isna().all():
            df_wide = df_wide.drop(columns=c)

# 10) Renombrar municipio_clean a municipio (mantener el nombre solicitado)
if 'municipio_clean' in df_wide.columns:
    df_wide = df_wide.rename(columns={'municipio_clean': 'municipio'})

# 11) Agrupar por municipio y después por year (suma por si hubiera duplicados) y ordenar
group_cols = ['municipio', 'year']
value_cols = ['Total', 'Hombres', 'Mujeres']

df_final = (df_wide
            .groupby(group_cols, as_index=False)[value_cols]
            .sum()
            .sort_values(['municipio', 'year'])
            .reset_index(drop=True)
           )

print("Filas resultantes:", len(df_final))
print(df_final.head(10).to_string())

#----------------------------------------------------------------------------------------
# Conteo de municipios con exactamente 5 años y con menos de 5 años
num_igual_5 = df_final.groupby('municipio')['year'].nunique().eq(5).sum()
num_menor_5 = df_final.groupby('municipio')['year'].nunique().lt(5).sum()
print("Municipios con exactamente 5 años:", num_igual_5)
print("Municipios con menos de 5 años:", num_menor_5)

# 12) Exportar resultado final con codificación segura para Excel
df_final.to_csv("demografia_municipios_limpio.csv", index=False, encoding="utf-8-sig")
print("✅ Exportado: demografia_municipios_limpio.csv (utf-8-sig)")
