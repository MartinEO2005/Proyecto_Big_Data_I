import pandas as pd
import os

def main():
    print("   -> Vinculando Movilidad con nombres oficiales de Demografía...")
    
    input_csv = "data/transporte/conectividad_municipal_2010_2025.csv"
    # Usamos tu archivo ya limpio como base de nombres
    ref_clean_csv = "data/clean/demografia_municipios_final.csv"
    output_file = "data/clean/conectividad_final_limpio.csv"

    if not os.path.exists(input_csv):
        print(f"   ❌ Error: No existe {input_csv}")
        return

    # 1. Cargar movilidad
    df = pd.read_csv(input_csv)
    df['LAU_ID'] = df['LAU_ID'].astype(str).str.zfill(5)
    df['region_code'] = df['LAU_ID'].str[:2]

    # 2. Mapear nombres de provincia desde tu archivo limpio
    if os.path.exists(ref_clean_csv):
        # Cargamos solo lo necesario para el mapeo
        df_ref = pd.read_csv(ref_clean_csv, dtype={'region_code': str})
        map_nombres = df_ref.drop_duplicates('region_code').set_index('region_code')['region_name'].to_dict()
        df['PROV_NAME'] = df['region_code'].map(map_nombres)
        print(f"   ℹ️ Mapeadas {len(map_nombres)} provincias desde el catálogo limpio.")
    else:
        print(f"   ⚠️ ATENCIÓN: No se encontró {ref_clean_csv}. Ejecuta primero la limpieza de demografía.")
        df['PROV_NAME'] = df['region_code']

    # 3. Totales provinciales por año (Suma corregida)
    # Agrupamos por el nombre de provincia obtenido y el año
    df_prov_anual = df.groupby(['PROV_NAME', 'Anio'])['Vehiculos_Oficial'].sum().reset_index()
    df_prov_anual.rename(columns={'Vehiculos_Oficial': 'Vehiculos_Prov_Total'}, inplace=True)

    # 4. Merge y Cálculo de porcentajes
    df_final = pd.merge(df, df_prov_anual, on=['PROV_NAME', 'Anio'], how='left')
    
    # Calculamos el peso del municipio sobre su provincia
    df_final['Pct_Vehiculos_Muni_vs_Prov'] = (
        (df_final['Vehiculos_Oficial'] / df_final['Vehiculos_Prov_Total']) * 100
    ).round(4)

    # 5. Exportar columnas finales
    cols = [
        'LAU_ID', 'LAU_NAME', 'Anio', 'Vehiculos_Oficial', 
        'Indice_Conectividad', 'Poblacion_Est', 'PROV_NAME', 
        'Vehiculos_Prov_Total', 'Pct_Vehiculos_Muni_vs_Prov'
    ]
    df_final[cols].to_csv(output_file, index=False, encoding='utf-8-sig')
    print(f"   ✅ [LISTO] {len(df_final)} registros procesados con nombres oficiales.")

if __name__ == "__main__":
    main()