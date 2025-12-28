import os
import sys
import pandas as pd

try:
    import limpieza_energia_migracion_renta  # Genera 4 CSVs
    import limpiezaDemografia               # Genera 1 CSV
    import limpiezaosm                      # Genera 1 CSV
    import limpiezaViirs                    # Genera 1 CSV
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
            print(f"✅ [OK]    {ruta.split('/')[-1]} - {len(df)} filas.")
        else:
            print(f"⚠️ [NULOS] {ruta.split('/')[-1]} - ¡Atención! Tiene {nulos} valores faltantes.")
    else:
        print(f"❌ [MISSING] {ruta.split('/')[-1]} - El archivo no se generó.")

def main():
    print("="*60)
    print("🚀 PROCESO INTEGRADO DE LIMPIEZA - DATASET 2025")
    print("="*60)

    print("\n[1/4] Ejecutando Limpieza Socioeconómica (4 archivos)...")

    print("\n[2/4] Ejecutando Limpieza de Demografía...")
    if hasattr(limpiezaDemografia, 'main'):
        limpiezaDemografia.main()

    print("\n[3/4] Ejecutando Limpieza de Transporte (OSM)...")
    limpiezaosm.main(
        "data/transporte/muni_station_metrics_reduced.csv",
        "data/clean/muni_station_osm_limpio.csv"
    )

    print("\n[4/4] Ejecutando Limpieza de Luz Nocturna (VIIRS)...")
    if hasattr(limpiezaViirs, 'main'):
        limpiezaViirs.main()

    print("\n" + "="*60)
    print("📊 REPORTE DE ARCHIVOS GENERADOS EN data/clean/")
    print("="*60)

    archivos_finales = [
        "data/clean/consumo_electricoProv_final_limpio.csv",
        "data/clean/migracion_municipios_final_limpio.csv",
        "data/clean/rentamedia_municipios_final_limpio.csv",
        "data/clean/empresas_transporte_final_limpio.csv",
        "data/clean/demografia_municipios_final.csv",
        "data/clean/muni_station_osm_limpio.csv",
        "data/clean/viirsFinal_limpio.csv"
    ]

    for ruta in archivos_finales:
        verificar_salud_csv(ruta)

if __name__ == "__main__":
    main()