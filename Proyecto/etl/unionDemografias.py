import pandas as pd
import geopandas as gpd

# 1. Cargar los datasets
print("Cargando datos...")
df_muni = pd.read_csv("demografia_municipios_limpio.csv")
df_prov = pd.read_csv("demografia_provincial_clean.csv")
gdf_geo = gpd.read_file("municipios_es.geojson")

# 2. Crear un mapeo Municipio -> Código de Provincia usando el GeoJSON
# Extraemos los 2 primeros dígitos del LAU_ID (código de provincia)
gdf_geo['cod_prov_extraido'] = gdf_geo['LAU_ID'].str[:2].astype(int)
mapa_muni_prov = gdf_geo.set_index('LAU_NAME')['cod_prov_extraido'].to_dict()

# 3. Asignar el código de provincia al dataframe de municipios
# Esto nos permite saber a qué provincia pertenece cada "Ababuj" o "Madrid"
df_muni['cod_prov'] = df_muni['municipio'].map(mapa_muni_prov)

# 4. Asegurar que los tipos de datos coincidan para el merge
df_muni['cod_prov'] = pd.to_numeric(df_muni['cod_prov'], errors='coerce')
df_prov['cod_prov'] = pd.to_numeric(df_prov['cod_prov'], errors='coerce')
df_muni['year'] = df_muni['year'].astype(int)
df_prov['year'] = df_prov['year'].astype(int)

# 5. UNIÓN FINAL (Merge)
# Unimos por código de provincia Y por año para que la población provincial sea la correcta de ese año
print("Realizando unión histórica...")
df_final = pd.merge(
    df_muni,
    df_prov,
    on=['cod_prov', 'year'],
    how='inner'
)

# 6. Reordenar y seleccionar columnas finales
columnas_ordenadas = [
    'year',
    'NombreProvincia',
    'PoblacionProvincial',
    'municipio',
    'Total',
    'Hombres',
    'Mujeres'
]

df_resultado = df_final[columnas_ordenadas].sort_values(['year', 'NombreProvincia', 'municipio'])

# 7. Guardar el CSV
output_file = "datasetfinal_demografia_espana.csv"
df_resultado.to_csv(output_file, index=False, encoding='utf-8-sig')

print(f"✅ ¡Hecho! Archivo guardado como: {output_file}")
print(f"📊 Total de registros: {len(df_resultado)}")
print(df_resultado.head())