import pandas as pd
import numpy as np
import re
from pathlib import Path

# --- Funciones de Utilidad ---

# Función para reparar mojibake típico (UTF-8 leído como Latin-1)
def fix_mojibake(s):
    if not isinstance(s, str):
        return s
    try:
        return s.encode("latin1").decode("utf-8")
    except (UnicodeEncodeError, UnicodeDecodeError):
        return s

# --- Mapeo de Corrección y Datos Oficiales ---

# Listado de las 52 provincias oficiales (50 + Ceuta y Melilla)
NOMBRES_OFICIALES = {
    "Álava", "Albacete", "Alicante", "Almería", "Ávila", "Badajoz", "Islas Baleares", 
    "Barcelona", "Burgos", "Cáceres", "Cádiz", "Castellón", "Ciudad Real", "Córdoba", 
    "A Coruña", "Cuenca", "Girona", "Granada", "Guadalajara", "Gipuzkoa", "Huelva", 
    "Huesca", "Jaén", "León", "Lleida", "La Rioja", "Lugo", "Madrid", "Málaga", "Murcia", 
    "Navarra", "Ourense", "Asturias", "Palencia", "Las Palmas", "Pontevedra", "Salamanca", 
    "Santa Cruz de Tenerife", "Cantabria", "Segovia", "Sevilla", "Soria", "Tarragona", 
    "Teruel", "Toledo", "Valencia", "Valladolid", "Bizkaia", "Zamora", "Zaragoza", 
    "Ceuta", "Melilla"
}

# 1. Definir los nombres de las SUB-REGIONES (ISLAS) que deben ser SUMADAS.
# Estas entradas son las que, si aparecen junto al Total Provincial, causan duplicación.
MAPA_SUBREGIONES = {
    "Mallorca": "Islas Baleares", "Menorca": "Islas Baleares", "Eivissa y Formentera": "Islas Baleares", 
    "Ibiza": "Islas Baleares", "Formentera": "Islas Baleares",
    "Gran Canaria": "Las Palmas", "Lanzarote": "Las Palmas", "Fuerteventura": "Las Palmas",
    "Tenerife": "Santa Cruz de Tenerife", "La Gomera": "Santa Cruz de Tenerife", "La Palma": "Santa Cruz de Tenerife", 
    "El Hierro": "Santa Cruz de Tenerife"
}
NOMBRES_SUBREGIONES = set(MAPA_SUBREGIONES.keys())
PROVINCIAS_CON_ISLAS = set(MAPA_SUBREGIONES.values())

# 2. Definir los nombres ALTERNATIVOS (bilingües, CCAA, etc.) que se reemplazan.
MAPA_ALTERNATIVO = {
    "Araba/Álava": "Álava", "Alicante/Alacant": "Alicante", "Castellón/Castelló": "Castellón", 
    "Valencia/València": "Valencia", "Coruña, A": "A Coruña", 
    "Comunidad Foral de Navarra": "Navarra", "Comunidad de Madrid": "Madrid", 
    "Region de Murcia": "Murcia", "Illes Balears": "Islas Baleares"
}

# Unir ambos mapas para la corrección final de nombres
MAPA_CORRECCION = {**MAPA_SUBREGIONES, **MAPA_ALTERNATIVO}

# Mapeo de nombre de provincia a código INE para el output final
CODIGOS_PROVINCIAS = {
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
NAME_TO_INE_CODE = {v: k for k, v in CODIGOS_PROVINCIAS.items()}


# ----------------------------
# 1. Cargar y Limpiar Tipos
# ----------------------------
# Asegúrate de usar el nombre de archivo que resulta de la descarga.
# Si el nombre es 'demografia_poblacion.csv', cámbialo aquí.
FILE_INPUT = "demografia_poblacion_provincias.csv" 

df = pd.read_csv(FILE_INPUT, dtype=str)

df['region_name'] = df['region_name'].astype(str).apply(fix_mojibake).str.strip()
df['population'] = pd.to_numeric(df['population'], errors='coerce')
df['year'] = pd.to_numeric(df['year'], errors='coerce').astype('Int64')
df = df.dropna(subset=['population', 'year'])


# ----------------------------
# 2. Lógica de Exclusión (Anti-Duplicación)
# ----------------------------

# Creamos una columna con el nombre oficial de destino (aplicando todos los mapeos)
df['region_name_mapped'] = df['region_name'].map(lambda x: MAPA_CORRECCION.get(x, x))

# La condición para MANTENER una fila es:
# a) Es una sub-región/isla (se sumará en el siguiente paso), O
# b) No es una de las provincias insulares (es decir, península, Ceuta, Melilla, que asumimos son totales correctos).

cond_subregion = df['region_name'].isin(NOMBRES_SUBREGIONES)
cond_peninsula = ~df['region_name_mapped'].isin(PROVINCIAS_CON_ISLAS)

# Filtramos: Mantenemos todas las filas peninsulares/ciudades autónomas.
# Para las insulares, solo mantenemos las filas a nivel isla, excluyendo la fila del TOTAL provincial.
df_clean = df[cond_peninsula | cond_subregion].copy()

# ----------------------------
# 3. Agrupación y Suma (Corrección de Población Total)
# ----------------------------

df_grouped = (df_clean
              .groupby(['region_name_mapped', 'year'], as_index=False)['population']
              .sum() # SUMA solo las islas que quedaron, el resto se mantiene como está
              .rename(columns={'region_name_mapped': 'region_name'})
              .sort_values(['year', 'region_name'])
             )

# Filtrar para quedarnos SOLO con los 52 nombres oficiales.
df_grouped = df_grouped[df_grouped['region_name'].isin(NOMBRES_OFICIALES)].reset_index(drop=True)

# --- VERIFICACIÓN TRAS SUMA ---
poblacion_maxima = df_grouped.groupby('year')['population'].sum().max()
num_provincias = df_grouped['region_name'].nunique()
print(f"✅ Número de provincias únicas tras limpieza: {num_provincias} (Objetivo: 52)")
print(f"✅ Población Nacional Máxima (Verificación de Duplicados): {poblacion_maxima:,.0f} (Objetivo: ~48M)")
# --------------------------------------------------------------------------------------------------


# ----------------------------
# 4. Rellenar Valores Faltantes (Interpolación Lineal)
# ----------------------------
# Asegurar que todas las provincias tengan datos para todos los 35 años (1990-2024, si esa es la ventana)

min_year = df_grouped['year'].min()
max_year = df_grouped['year'].max()
years = range(min_year, max_year + 1)
provinces = list(NOMBRES_OFICIALES) 

# Crear un índice completo (Producto cartesiano)
full_index = pd.MultiIndex.from_product([provinces, years], names=['region_name', 'year'])
df_grouped = df_grouped.set_index(['region_name', 'year']).reindex(full_index).reset_index()

# Aplicar interpolación lineal (media entre anterior y siguiente)
df_grouped['population'] = df_grouped.groupby('region_name')['population'].apply(
    # 'linear' = media entre los puntos vecinos. 'limit_direction=both' rellena los extremos.
    lambda x: x.interpolate(method='linear', limit_direction='both')
).reset_index(drop=True)


# --- VERIFICACIÓN FINAL ---
df_count_after = df_grouped.groupby('region_name')['year'].count().reset_index()
years_expected = df_grouped['year'].nunique()
print(f"\n✅ Total de años en el dataset: {years_expected}. (Objetivo: 35)")
print(f"✅ ¿Todas las provincias tienen {years_expected} años? {(df_count_after['year'] == years_expected).all()}")
print(df_grouped.head(10).to_string(index=False))


# ----------------------------
# 5. Salida Final
# ----------------------------

# Añadir código INE para el paso de unión
df_grouped['cod_prov'] = df_grouped['region_name'].map(NAME_TO_INE_CODE)
df_grouped.rename(columns={'region_name': 'NombreProvincia', 'population': 'PoblacionProvincial'}, inplace=True)

# Seleccionar y reordenar columnas finales
df_final_output = df_grouped[['cod_prov', 'NombreProvincia', 'year', 'PoblacionProvincial']]


# Guardar el resultado limpio
FILE_OUTPUT = Path("demografia_provincial_limpio.csv")
df_final_output.to_csv(FILE_OUTPUT, index=False, encoding='utf-8-sig')

# mostrar poblacion total por año para verificacion
total_por_anyo = df_final_output.groupby('year')['PoblacionProvincial'].sum().reset_index()
print("\n✅ Población total por año tras limpieza e interpolación:")
print(total_por_anyo.to_string(index=False))

print(f"\n💾 Datos provinciales limpios guardados en: {FILE_OUTPUT.resolve()}")