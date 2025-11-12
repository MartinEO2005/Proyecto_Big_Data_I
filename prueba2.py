import pandas as pd

# 1) Leer CSV completo
df = pd.read_csv("salida_viirs/viirs_bloque_0.csv")

unique_count = df['date'].nunique()

print("Número de valores únicos en LAU_NAME:", unique_count)
