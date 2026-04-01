import pandas as pd
import os
import glob

# Ruta a la carpeta con los CSVs
csv_folder = os.path.dirname(os.path.abspath(__file__))

# Obtener todos los archivos CSV en la carpeta
csv_files = glob.glob(os.path.join(csv_folder, "*.csv"))

# Bucle para leer y mostrar el head de cada CSV
for csv_file in csv_files:
    csv_name = os.path.basename(csv_file)
    print(f"\n{'='*80}")
    print(f"CSV: {csv_name}")
    print(f"{'='*80}\n")
    
    df = pd.read_csv(csv_file)
    pd.set_option('display.max_columns', None)
    pd.set_option('display.width', None)
    print(df.head())
    print(f"\nForma: {df.shape[0]} filas, {df.shape[1]} columnas")
    
    # Find date columns and display date range
    date_cols = df.select_dtypes(include=['datetime64']).columns
    if len(date_cols) == 0:
        # Try to infer date columns by name
        date_cols = [col for col in df.columns if any(keyword in col.lower() for keyword in ['date', 'fecha', 'time', 'hora', "anio", "year", "Anio"])]
    
    if date_cols:
        for date_col in date_cols:
            if date_col not in df.select_dtypes(include=['datetime64']).columns:
                df[date_col] = pd.to_datetime(df[date_col], errors='coerce')
            min_date = df[date_col].min()
            max_date = df[date_col].max()
            print(f"Rango de fechas ({date_col}): {min_date} to {max_date}")
    else:
        print("No date columns found")
