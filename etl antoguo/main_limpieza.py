import os
import sys
import pandas as pd

# 1. Importación de módulos
try:
    import limpieza_energia_migracion_renta  # Genera 4 CSVs
    import limpiezaDemografia                # Genera 1 CSV (Referencia para otros)
    import limpieza_conectividad             # Genera 1 CSV (Depende de Demografía)
    import limpiezaosm                       # Genera 1 CSV
    import limpiezaViirs                     # Genera 1 CSV
except ImportError as e:
    print(f"❌ Error: No se pudo importar el módulo: {e.name}")
    print("Asegúrate de que los archivos .py estén en la misma carpeta que este script.")
    sys.exit(1)

def verificar_salud_csv(ruta):
    """Chequea si el archivo existe y si tiene nulos (crítico para Ridge/LightGBM)."""
    if os.path.exists(ruta):
        df = pd.read_csv(ruta)
        nulos = df.isna().sum().sum()
        if nulos == 0:
            print(f"✅ [OK]      {ruta.split('/')[-1]} - {len(df)} filas.")
        else:
            # Mostramos dónde están los nulos para facilitar la depuración
            col_con_nulos = df.columns[df.isnull().any()].tolist()
            print(f"⚠️ [NULOS]   {ruta.split('/')[-1]} - ¡Atención! {nulos} nulos en: {col_con_nulos}")
    else:
        print(f"❌ [MISSING] {ruta.split('/')[-1]} - El archivo no se generó.")

def main():
    print("="*60)
    print("🚀 PROCESO INTEGRADO DE LIMPIEZA - PROYECTO BIG DATA 2026")
    print("="*60)

    # PASO 1: Socioeconómico
    print("\n[1/5] Ejecutando Limpieza Socioeconómica...")
    if hasattr(limpieza_energia_migracion_renta, 'main'):
        limpieza_energia_migracion_renta.main()

    # PASO 2: Demografía (CRÍTICO: Genera el maestro de provincias)
    print("\n[2/5] Ejecutando Limpieza de Demografía...")
    if hasattr(limpiezaDemografia, 'main'):
        limpiezaDemografia.main()

    # PASO 3: Movilidad (Depende del Paso 2 para mapear nombres)
    print("\n[3/5] Ejecutando Limpieza de Movilidad (Conectividad)...")
    if hasattr(limpieza_conectividad, 'main'):
        limpieza_conectividad.main()

    # PASO 4: Transporte (OSM)
    print("\n[4/5] Ejecutando Limpieza de Transporte (OSM)...")
    limpiezaosm.main(
        "data/transporte/muni_station_metrics.csv",
        "data/clean/muni_station_osm_limpio.csv"
    )

    # PASO 5: Luz Nocturna (VIIRS)
    print("\n[5/5] Ejecutando Limpieza de Luz Nocturna (VIIRS)...")
    if hasattr(limpiezaViirs, 'main'):
        limpiezaViirs.main()

    # --- REPORTE FINAL ---
    print("\n" + "="*60)
    print("📊 REPORTE DE SALUD DE DATOS (data/clean/)")
    print("="*60)

    archivos_finales = [
        "data/clean/consumo_electrico_final_limpio.csv",
        "data/clean/migracion_municipios_final_limpio.csv",
        "data/clean/rentamedia_municipios_final_limpio.csv",
        "data/clean/empresas_transporte_final_limpio.csv",
        "data/clean/demografia_municipios_final.csv",
        "data/clean/conectividad_final_limpio.csv",  
        "data/clean/muni_station_osm_limpio.csv",
        "data/clean/viirsFinal_limpio.csv"
    ]

    for ruta in archivos_finales:
        verificar_salud_csv(ruta)
    
    print("\n" + "="*60)
    print("✨ Proceso finalizado. Los datos están listos para el modelo.")
    print("="*60)

if __name__ == "__main__":
    main()