import pandas as pd

# 1) Leer CSV completo
df = pd.read_csv(r"C:\Users\Martin Otero\Desktop\luminosidad_municipios.csv")

# 2) Normalizar nombres de columnas
df = df.rename(columns={
    'LAU_NAME': 'municipio'
})
if 'date' not in df.columns:
    # detecta columna de fecha si tiene otro nombre
    for c in df.columns:
        if 'DATE' in c.upper():
            df = df.rename(columns={c: 'date'})
            break

# 3) Deduplicar: una fila por municipio+mes
key_cols = ['date']
num_cols = df.select_dtypes(include=['number']).columns.tolist()
agg_dict = {c: 'mean' for c in num_cols if c not in key_cols}

df_dedup = df.groupby(key_cols, as_index=False).agg(agg_dict)

# 4) Recuperar nombre de municipio (first)
df_names = df.groupby(key_cols, as_index=False)[['municipio']].first()
df_dedup = pd.merge(df_dedup, df_names, on=key_cols, how='left')

# 5) Guardar limpio
df_dedup.to_csv("viirs_municipios_clean.csv", index=False)
print("✅ CSV limpio guardado: viirs_municipios_clean.csv")
print("Filas:", len(df_dedup))
