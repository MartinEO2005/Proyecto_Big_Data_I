import pandas as pd
import numpy as np
import os

# 1) CONSUMO ENERGÍA
try:
    df_raw = pd.read_csv(r"data/energia/consumo_electrico.csv", sep=';', encoding='utf-8', on_bad_lines='skip')
    df_raw['Consumo eléctrico'] = df_raw['Consumo eléctrico'].str.strip()
    df_raw['Codigo'] = df_raw['Codigo'].astype(str)

    df_pivot = df_raw.pivot_table(index=['Codigo', 'Nombre'], columns='Consumo eléctrico', values='Total', aggfunc='first').reset_index()
    df_prov = df_pivot[df_pivot['Codigo'].str.len() <= 2].copy()
    df_mun = df_pivot[df_pivot['Codigo'].str.len() > 2].copy()

    columnas_consumo = [c for c in df_prov.columns if c not in ['Codigo', 'Nombre']]
    rename_dict = {col: f"prov_{col.replace(' ', '_').lower()}" for col in columnas_consumo}
    rename_dict['Nombre'] = 'nombre_provincia'
    df_prov = df_prov.rename(columns=rename_dict)

    df_mun['id_prov_join'] = df_mun['Codigo'].apply(lambda x: x[:-3] if len(x) > 3 else x)
    df1_consumo = pd.merge(df_mun, df_prov, left_on='id_prov_join', right_on='Codigo', suffixes=('', '_prov_eliminar'))
    df1_consumo = df1_consumo.drop(columns=['id_prov_join', 'Codigo_prov_eliminar'])
    df1_consumo = df1_consumo.sort_values(by=['nombre_provincia', 'Nombre'])

    # 🌟 ESTANDARIZACIÓN DEL ID OFICIAL
    df1_consumo['muni_id_join'] = df1_consumo['Codigo'].astype(str).str.zfill(5)
    
    cols_geo = ['nombre_provincia', 'Nombre', 'muni_id_join', 'Codigo']
    otras_cols = [c for c in df1_consumo.columns if c not in cols_geo]
    df1_consumo = df1_consumo[cols_geo + otras_cols]
    print("✅ Energía lista.")
except Exception as e:
    print(f"❌ Error en reestructuración de energía: {e}")
    df1_consumo = pd.DataFrame()


# 2) MIGRACIÓN MUNICIPIOS
df2_migracion = pd.read_csv(r"data/migracion/migracion_interior_municipios.csv")
df2_migracion = df2_migracion.drop(columns=['codigo_provincia', 'sexo'])
df2_migracion = df2_migracion.rename(columns={'valor': 'cantidad (personas)'})

# 🌟 ESTANDARIZACIÓN DEL ID OFICIAL
df2_migracion['muni_id_join'] = df2_migracion['codigo_municipio'].astype(str).str.replace(r'\.0$', '', regex=True).str.zfill(5)

columnas_ordenadas = ['provincia', 'nombre_municipio', 'muni_id_join', 'anio', 'nacionalidad', 'cantidad (personas)']
df2_migracion = df2_migracion[columnas_ordenadas].sort_values(by=['provincia', 'nombre_municipio', 'anio'])
df2_migracion = df2_migracion.dropna(subset=['cantidad (personas)'])
print(f"✅ Migración lista.")


# 3) PIB / RENTA MUNICIPAL
df3_renta_municipios = pd.read_csv(r"data/renta/renta_municipios.csv")
df3_renta_municipios = df3_renta_municipios.rename(columns={'valor': 'pib'})

# 🌟 ESTANDARIZACIÓN DEL ID OFICIAL
df3_renta_municipios['muni_id_join'] = df3_renta_municipios['codigo_municipio'].astype(str).str.replace(r'\.0$', '', regex=True).str.zfill(5)

columnas_renta = ['provincia', 'nombre_municipio', 'muni_id_join', 'anio', 'pib']
df3_renta_municipios = df3_renta_municipios[columnas_renta].sort_values(by=['provincia', 'nombre_municipio', 'anio'])
df3_renta_municipios = df3_renta_municipios.dropna(subset=['pib'])
print(f"✅ Renta lista.")


# 4) EMPRESAS DE TRANSPORTE
df4_empresasTrans = pd.read_csv(r"data/empresas_transporte/empresas_transporte_prov_mun_anchos.csv")
# Si tiene código de municipio, lo estandarizamos también
col_id = next((c for c in df4_empresasTrans.columns if 'codigo' in c.lower()), None)
if col_id:
    df4_empresasTrans['muni_id_join'] = df4_empresasTrans[col_id].astype(str).str.replace(r'\.0$', '', regex=True).str.zfill(5)
print(f"✅ Empresas de Transporte listas.")


# 5) EXPORTACIÓN
output_folder = "data/clean"
os.makedirs(output_folder, exist_ok=True)

dfs_to_export = {
    "consumo_electrico_final": df1_consumo,
    "migracion_municipios_final": df2_migracion,
    "rentamedia_municipios_final": df3_renta_municipios,
    "empresas_transporte_final": df4_empresasTrans
}

for name, df in dfs_to_export.items():
    file_path = os.path.join(output_folder, f"{name}_limpio.csv")
    df.to_csv(file_path, index=False, encoding="utf-8-sig")
    print(f"✅ Exportado: {file_path} ({df.shape[0]} filas)")

print("\n✨ Proceso de limpieza y exportación finalizado.")