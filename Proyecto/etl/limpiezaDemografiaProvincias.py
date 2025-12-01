import pandas as pd

# 1. Cargar el CSV
df = pd.read_csv("demografia_poblacion.csv", dtype=str)

# 2. Reparar codificación en nombres de provincia (mojibake típico)
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

# 4. Agrupar por provincia y año
df_grouped = (df
              .groupby(['region_name', 'year'], as_index=False)['population']
              .sum()
              .sort_values(['year', 'region_name'])
             )

print("📍 Población total por provincia y año:")
print(df_grouped.head(20).to_string())

# 5. Población total nacional por año
df_total_por_año = df_grouped.groupby('year')['population'].sum().reset_index()
df_total_por_año = df_total_por_año.rename(columns={'population': 'total_population'})

print("\n🇪🇸 Población total nacional por año:")
print(df_total_por_año.to_string(index=False))
