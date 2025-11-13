import pandas as pd

df = pd.read_csv(r"C:\Users\Iker\OneDrive\Escritorio\Universidad\Año3-Sem1\Proyecto de big data I\Proyecto_Open_Data_I\neo_lumina_output\demografia_poblacion_municipios.csv")

df.head()

df["sexo"] = df["municipio"].str.extract(r"\b(Total|Hombres|Mujeres)\b")

# Limpiar el nombre del municipio (quedarnos solo con el nombre antes del primer punto)
df["municipio"] = df["municipio"].str.split(".").str[0].str.strip()

df.head(6)

df_pivot = df.pivot_table(
    index=["municipio", "year"],
    columns="sexo",
    values="population"
).reset_index()

df_pivot.columns.name = None
df_pivot = df_pivot.rename(columns={
    "Total": "total",
    "Hombres": "hombres",
    "Mujeres": "mujeres"
})

print(df_pivot.head())

df_pivot.head(50)

# Limpiar el texto del municipio
df_pivot["municipio"] = df_pivot["municipio"].str.split(".").str[0].str.strip()

# Mostrar número total de municipios únicos
print("Número de municipios únicos:", df_pivot["municipio"].nunique())

# Mostrar todos los nombres únicos
municipios_unicos = sorted(df_pivot["municipio"].unique())
print(municipios_unicos)

# Normalizar nombres
def normalizar(texto):
    return (
        str(texto)
        .strip()
        .lower()
        .replace("’", "'")
        .replace("á", "a")
        .replace("é", "e")
        .replace("í", "i")
        .replace("ó", "o")
        .replace("ú", "u")
        .replace("ü", "u")
        .replace("ñ", "n")
    )

df_pivot["municipio"] = df_pivot["municipio"].apply(normalizar)


df_pivot.head(50)

# Limpiar el texto del municipio
df_pivot["municipio"] = df_pivot["municipio"].str.split(".").str[0].str.strip()

# Mostrar número total de municipios únicos
print("Número de municipios únicos:", df_pivot["municipio"].nunique())

# Mostrar todos los nombres únicos
municipios_unicos = sorted(df_pivot["municipio"].unique())
print(municipios_unicos)

# Ejemplo simple
municipio_a_provincia = {
    "ababuj": "Teruel",
    "abades": "Segovia",
    "abanilla": "Murcia",
    "abárzuza": "Navarra"
}

df_pivot["provincia"] = df_pivot["municipio"].map(municipio_a_provincia)

df_pivot["provincia"] = df_pivot["municipio"].map(municipio_a_provincia)

df_pivot[df_pivot["provincia"].isna()]

df_pivot["provincia"] = df_pivot["municipio"].map(municipio_a_provincia).fillna("Desconocida")
df_pivot.head(50)