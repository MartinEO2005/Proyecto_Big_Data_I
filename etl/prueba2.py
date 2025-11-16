import pandas as pd

# 1) Leer CSV completo
df = pd.read_csv("Desktop/salida_viirs/viirs_municipios_final.csv")

unique_count = df['date'].nunique()

print("Número de valores únicos en LAU_NAME:", unique_count)
