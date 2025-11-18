import pandas as pd

df = pd.read_csv(r"C:\Users\Iker\OneDrive\Escritorio\Universidad\Año3-Sem1\Proyecto de big data I\Proyecto_Open_Data_I\outputs\data\demografia\demografia_poblacion_provincias.csv")

df.head()

df_sorted = df.sort_values(by="year")

df.head()