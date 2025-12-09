import pandas as pd
import numpy as np

# 1. Cargar datos
df = pd.read_csv("demografia_poblacion_provincias.csv", dtype=str)

# 2. Reparar codificación (Mojibake)
def fix_mojibake(s):
    if not isinstance(s, str):
        return s
    try:
        return s.encode("latin1").decode("utf-8")
    except (UnicodeEncodeError, UnicodeDecodeError):
        return s

df['region_name'] = df['region_name'].astype(str).apply(fix_mojibake).str.strip()

# 3. Convertir columnas numéricas
df['population'] = pd.to_numeric(df['population'], errors='coerce')
df['year'] = pd.to_numeric(df['year'], errors='coerce').astype('Int64')

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

# Creamos un set con los nombres válidos para filtrar rápido
NOMBRES_OFICIALES = set(CODIGOS_PROVINCIAS.values())

# Diccionario de corrección: Mapea lo que sale en el CSV -> Al nombre oficial del diccionario
MAPA_CORRECCION = {
    # --- ISLAS CANARIAS (Las Palmas) ---
    "Gran Canaria": "Las Palmas",
    "Lanzarote": "Las Palmas",
    "Fuerteventura": "Las Palmas",
    # --- ISLAS CANARIAS (Santa Cruz de Tenerife) ---
    "Tenerife": "Santa Cruz de Tenerife",
    "La Palma": "Santa Cruz de Tenerife",
    "La Gomera": "Santa Cruz de Tenerife",
    "El Hierro": "Santa Cruz de Tenerife",
    # --- ISLAS BALEARES ---
    "Mallorca": "Islas Baleares",
    "Menorca": "Islas Baleares",
    "Eivissa y Formentera": "Islas Baleares",
    "Ibiza": "Islas Baleares",
    "Formentera": "Islas Baleares",
    # --- NOMBRES BILINGÜES/COMPUESTOS -> NOMBRE SIMPLE ---
    "Araba/Álava": "Álava",
    "Alicante/Alacant": "Alicante",
    "Castellón/Castelló": "Castellón",
    "Valencia/València": "Valencia",
    "Coruña, A": "A Coruña",
    "Bizkaia": "Bizkaia", # Ya está bien en tu mapa
    "Gipuzkoa": "Gipuzkoa", # Ya está bien
    "Comunidad Foral de Navarra": "Navarra",
    "Comunitat Valenciana": "Valencia", # Ojo, a veces sale la CCAA
    "Comunidad de Madrid": "Madrid",
    "Region de Murcia": "Murcia",
    "Illes Balears": "Islas Baleares"
}

# 1. Aplicar correcciones de nombres
# Si el nombre está en el mapa de corrección, lo cambia. Si no, lo deja como está.
df['region_name'] = df['region_name'].map(lambda x: MAPA_CORRECCION.get(x, x))

# 2. Filtrar para quedarse SOLO con las provincias oficiales (elimina CCAA o datos extraños)
df = df[df['region_name'].isin(NOMBRES_OFICIALES)]

# 3. Agrupar por provincia y año (CRUCIAL: Esto suma las islas en una sola provincia)
df_grouped = (df
              .groupby(['region_name', 'year'], as_index=False)['population']
              .sum() # Aquí se suman Mallorca + Menorca = Baleares
              .sort_values(['year', 'region_name'])
             )

# --- VERIFICACIÓN ---
num_provincias = df_grouped['region_name'].nunique()
print(f"✅ Número de provincias únicas tras limpieza: {num_provincias}")
if num_provincias == 52:
    print("   ¡Correcto! (50 Provincias + Ceuta + Melilla)")
else:
    print(f"   ⚠️ Aún no es exacto. Provincias encontradas: {df_grouped['region_name'].unique()}")


 # Contar valores de años por provincia
years_per_province = df_grouped.groupby('region_name')['year'].count().reset_index()
years_per_province = years_per_province.rename(columns={'year': 'num_years'})
print("\n📊 Número de años por provincia:")
print(years_per_province.to_string(index=False))

# 4. Rellenar valores faltantes (Interpolación)
# Primero aseguramos que existan todas las combinaciones de Año-Provincia
years = range(df_grouped['year'].min(), df_grouped['year'].max() + 1)
provinces = list(NOMBRES_OFICIALES) # Usamos tu lista oficial

# Crear un índice completo (Producto cartesiano)
full_index = pd.MultiIndex.from_product([provinces, years], names=['region_name', 'year'])
df_grouped = df_grouped.set_index(['region_name', 'year']).reindex(full_index).reset_index()

# Interpolar
df_grouped['population'] = df_grouped.groupby('region_name')['population'].apply(
    lambda x: x.interpolate(method='linear', limit_direction='both')
).reset_index(drop=True)

years_per_province = df_grouped.groupby('region_name')['year'].count().reset_index()
years_per_province = years_per_province.rename(columns={'year': 'num_years'})
print("\n📊 Número de años por provincia:")
print(years_per_province.to_string(index=False))

# 5. Salidas finales
print("\n📍 Población total por provincia y año (Primeras filas):")
print(df_grouped.head(10).to_string(index=False))

# Población nacional
df_total_por_año = df_grouped.groupby('year')['population'].sum().reset_index()
df_total_por_año = df_total_por_año.rename(columns={'population': 'total_population'})

print("\n🇪🇸 Población total nacional por año:")
print(df_total_por_año.tail().to_string(index=False))


print(df_grouped["region_name"].nunique())
print(df_grouped["year"].nunique())


# Guardar el resultado limpio (opcional) con la codificación correcta
df_grouped.to_csv("demografia_provincial_clean.csv", index=False, encoding='utf-8-sig')

