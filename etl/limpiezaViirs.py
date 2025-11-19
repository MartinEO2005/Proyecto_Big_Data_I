import pandas as pd

# Cargar dataset original
df = pd.read_csv("viirs_provincias_2018_2022.csv")

# Seleccionar columnas y hacer copia segura
df_limpio = df[["PROV_CODE", "PROV_NAME", "date", "mean"]].copy()

# Corrección de provincias con caracteres corruptos
correcciones = {
    "AlmerÃ­a": "Almeria",
    "CÃ¡diz": "Cadiz",
    "CÃ³rdoba": "Cordoba",
    "JaÃ©n": "Jaen",
    "MÃ¡laga": "Malaga",
    "Ãvila": "Avila",
    "LeÃ³n": "Leon",
    "CastellÃ³n/CastellÃ³": "Castellon/Castello",
    "CÃ¡ceres": "Caceres",
    "A CoruÃ±a": "A Coruna",
    "Araba/Ãlava": "Araba/Alava",
    "Valencia/ValÃ¨ncia": "Valencia/Valencia"
}

df_limpio["PROV_NAME"] = df_limpio["PROV_NAME"].replace(correcciones)

df_limpio["mean"] = df_limpio["mean"].round(4)
df_limpio.to_csv("viirs_provincias_2018_2022.csv", index=False, encoding="utf-8-sig")

print("CSV reemplazado correctamente.")
print(df_limpio.head())