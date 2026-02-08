import pandas as pd
import numpy as np

# 1) CONSUMO ENERGÍA

import pandas as pd
import numpy as np
import os

# 1) CONSUMO ENERGÍA - [REESTRUCTURADO Y ORDENADO POR JERARQUÍA]
try:
    df_raw = pd.read_csv(
        r"data/energia/consumo_electrico.csv", 
        sep=';', 
        encoding='utf-8', 
        on_bad_lines='skip'
    )
    
    df_raw['Consumo eléctrico'] = df_raw['Consumo eléctrico'].str.strip()
    df_raw['Codigo'] = df_raw['Codigo'].astype(str)

    # A) PIVOTAR: Pasamos las métricas a columnas
    df_pivot = df_raw.pivot_table(
        index=['Codigo', 'Nombre'], 
        columns='Consumo eléctrico', 
        values='Total', 
        aggfunc='first'
    ).reset_index()

    # B) SEPARAR CAPAS
    df_prov = df_pivot[df_pivot['Codigo'].str.len() <= 2].copy()
    df_mun = df_pivot[df_pivot['Codigo'].str.len() > 2].copy()

    # C) PREPARAR PROVINCIAS
    columnas_consumo = [c for c in df_prov.columns if c not in ['Codigo', 'Nombre']]
    rename_dict = {col: f"prov_{col.replace(' ', '_').lower()}" for col in columnas_consumo}
    rename_dict['Nombre'] = 'nombre_provincia'
    df_prov = df_prov.rename(columns=rename_dict)

    # D) VINCULACIÓN
    df_mun['id_prov_join'] = df_mun['Codigo'].apply(lambda x: x[:-3])

    # E) MERGE FINAL
    df1_consumo = pd.merge(
        df_mun, 
        df_prov, 
        left_on='id_prov_join', 
        right_on='Codigo', 
        suffixes=('', '_prov_eliminar')
    )

    # F) LIMPIEZA Y ORDENAMIENTO CRUCIAL
    df1_consumo = df1_consumo.drop(columns=['id_prov_join', 'Codigo_prov_eliminar'])
    
    # Aquí es donde fuerzo el orden que me pides: Provincia -> Municipio
    df1_consumo = df1_consumo.sort_values(by=['nombre_provincia', 'Nombre'])

    # Reordenar columnas para que la lectura sea lógica
    cols_geo = ['nombre_provincia', 'Nombre', 'Codigo']
    otras_cols = [c for c in df1_consumo.columns if c not in cols_geo]
    df1_consumo = df1_consumo[cols_geo + otras_cols]

    print(f"✅ Energía lista y ordenada por provincia/municipio.")
    print(df1_consumo[['nombre_provincia', 'Nombre']].head(10))

except Exception as e:
    print(f"❌ Error en reestructuración de energía: {e}")
    df1_consumo = pd.DataFrame()


# 2) MIGRACIÓN MUNICIPIOS

df2_migracion = pd.read_csv(r"data/migracion/migracion_interior_municipios.csv")

df2_migracion = df2_migracion.drop(columns=['codigo_provincia', 'sexo'])
df2_migracion = df2_migracion.rename(columns={'valor': 'cantidad (personas)'})

columnas_ordenadas = [
    'provincia', 'nombre_municipio', 'codigo_municipio',
    'anio', 'nacionalidad', 'cantidad (personas)'
]

df2_migracion = df2_migracion[columnas_ordenadas]
df2_migracion = df2_migracion.sort_values(by=['provincia', 'nombre_municipio', 'anio'])

print("\n✅ DataFrame reorganizado y simplificado:")
print(df2_migracion.head())
print(df2_migracion.shape)

print("\n🔎 Conteo de valores nulos por columna:")
print(df2_migracion.isna().sum())

n_nulos = df2_migracion['cantidad (personas)'].isna().sum()
n_ceros = (df2_migracion['cantidad (personas)'] == 0).sum()

print(f"\n📌 En 'cantidad (personas)':")
print(f"   - Valores Nulos (NaN): {n_nulos}")
print(f"   - Valores Cero (0):   {n_ceros}")

# 🧹 ELIMINAR SOLO NULOS EN CANTIDAD
df2_migracion = df2_migracion.dropna(subset=['cantidad (personas)'])

print(f"\n🧽 Filas eliminadas por NaN en migración: {n_nulos}")
print(f"📌 Nuevo tamaño df2_migracion: {df2_migracion.shape}")


# 3) PIB / RENTA MUNICIPAL

df3_renta_municipios = pd.read_csv(r"data/renta/renta_municipios.csv")

df3_renta_municipios = df3_renta_municipios.rename(columns={'valor': 'pib'})

columnas_renta = [
    'provincia', 'nombre_municipio',
    'codigo_municipio', 'anio', 'pib'
]

df3_renta_municipios = df3_renta_municipios[columnas_renta]
df3_renta_municipios = df3_renta_municipios.sort_values(
    by=['provincia', 'nombre_municipio', 'anio']
)

print("\n✅ df3_renta_municipios reorganizado:")
print(df3_renta_municipios.head())

nulos_pib = df3_renta_municipios['pib'].isna().sum()
ceros_pib = (df3_renta_municipios['pib'] == 0).sum()

print(f"\n🔍 Chequeo de integridad en 'pib':")
print(f"   - Valores Nulos (NaN): {nulos_pib}")
print(f"   - Valores Cero (0.0):  {ceros_pib}")

# 🧹 ELIMINAR FILAS CON PIB = NaN
df3_renta_municipios = df3_renta_municipios.dropna(subset=['pib'])

print(f"\n🧽 Filas eliminadas por NaN en 'pib': {nulos_pib}")
print(f"📌 Nuevo tamaño df3_renta_municipios: {df3_renta_municipios.shape}")

# 4) Empresas de transporte

df4_empresasTrans = pd.read_csv(r"data/empresas_transporte/empresas_transporte_prov_mun_anchos.csv")

cols_años = [c for c in df4_empresasTrans.columns if c.isdigit()]

nulos_por_columna = df4_empresasTrans.isnull().sum()
print("\n1. Nulos por columna (Años):")
print(nulos_por_columna[nulos_por_columna > 0])

filas_con_nulos = df4_empresasTrans[df4_empresasTrans.isnull().any(axis=1)]
print(f"\n2. Total de municipios con algún dato faltante: {len(filas_con_nulos)}")

totalmente_vacias = df4_empresasTrans[df4_empresasTrans[cols_años].isnull().all(axis=1)]
print(f"3. Municipios con 0 datos en toda la serie (2012-2021): {len(totalmente_vacias)}")

ceros_por_año = (df4_empresasTrans[cols_años] == 0).sum()
print("\n4. Presencia de valores '0.0' por año:")
print(ceros_por_año)

import os

output_folder = "data/clean"
os.makedirs(output_folder, exist_ok=True)

dfs_to_export = {
    "consumo_electrico_final": df1_consumo,
    "migracion_municipios_final": df2_migracion,
    "rentamedia_municipios_final": df3_renta_municipios,
    "empresas_transporte_final": df4_empresasTrans
}

for name, df in dfs_to_export.items():
    df_final = df
        # Construir ruta: data/clean/nombre_limpio.csv
    file_path = os.path.join(output_folder, f"{name}_limpio.csv")
    # Exportar
    df_final.to_csv(file_path, index=False, encoding="utf-8-sig")
    print(f"✅ Exportado: {file_path} ({df_final.shape[0]} filas)")

print("\n✨ Proceso de limpieza y exportación finalizado.")