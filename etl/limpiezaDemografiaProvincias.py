import pandas as pd

df = pd.read_csv(r"C:\Users\Iker\OneDrive\Escritorio\Universidad\Año3-Sem1\Proyecto de big data I\Proyecto_Open_Data_I\outputs\data\demografia\demografia_poblacion_provincias.csv")

df.head()

df = df.sort_values(by=["region_code", "year"]).reset_index(drop=True)


df.head(60)

df = df.drop(columns="code_num").reset_index(drop=True)

df.head(60)