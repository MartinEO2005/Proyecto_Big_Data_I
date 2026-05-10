import pandas as pd
import numpy as np
import geopandas as gpd
import matplotlib.pyplot as plt
import seaborn as sns
import os
import joblib
import time
from sqlalchemy import create_engine
from sklearn.model_selection import train_test_split, RandomizedSearchCV
from sklearn.ensemble import GradientBoostingClassifier
from sklearn.metrics import classification_report

# ---------------------------------------------------------
# 1. CONFIGURACIÓN DE RUTAS BLINDADAS
# ---------------------------------------------------------
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
MODEL_DIR = os.path.abspath(os.path.join(BASE_DIR, "..", "models", "modelos_exportados"))
FIGURES_DIR = os.path.abspath(os.path.join(BASE_DIR, "..", "models", "figuras"))

# El GeoJSON está asumiendo que está 2 niveles arriba (raíz del proyecto)
GEOJSON_PATH = os.path.abspath(os.path.join(BASE_DIR, "..", "..", "municipios_es.geojson"))

os.makedirs(MODEL_DIR, exist_ok=True)
os.makedirs(FIGURES_DIR, exist_ok=True)

def ejecutar_pipeline_clasificacion(df_master):
    # ---------------------------------------------------------
    # 2. ANÁLISIS ESPACIAL: Cálculo del 'Efecto Contagio'
    # ---------------------------------------------------------
    print("\n🌍 INICIANDO ANÁLISIS ESPACIAL: Cálculo del 'Efecto Contagio' (Spatial Lags)...")
    print(f"   -> Cargando mapa de España desde {GEOJSON_PATH}...")
    
    # Cargar fronteras
    try:
        gdf_espana = gpd.read_file(GEOJSON_PATH)
    except Exception as e:
        print(f"❌ Error al cargar el GeoJSON. Verifica la ruta: {GEOJSON_PATH}")
        return
    
    # --- NUEVO: Detector Automático de la Columna ID ---
    posibles_nombres = ['NATCODE', 'CUMUN', 'LAU_ID', 'muni_key', 'codigo_ine', 'CODIGOINE', 'ID', 'id_ine']
    columna_id_mapa = None
    
    for col in posibles_nombres:
        if col in gdf_espana.columns:
            columna_id_mapa = col
            break
            
    if columna_id_mapa is None:
        print(f"❌ ERROR CRÍTICO: No encuentro la columna del código de municipio en el GeoJSON.")
        print(f"   Columnas que tiene tu mapa: {list(gdf_espana.columns)}")
        print("   Por favor, edita el script e incluye el nombre correcto en la lista 'posibles_nombres'.")
        return
        
    print(f"   -> Detectada la columna '{columna_id_mapa}' como identificador en el mapa.")
    
    # Sincronizar IDs (asegurar que ambos sean strings de 5 dígitos con ceros a la izquierda)
    gdf_espana['LAU_ID'] = gdf_espana[columna_id_mapa].astype(str).str[-5:].str.zfill(5)
    df_master['LAU_ID'] = df_master['muni_id_join'].astype(str).str.zfill(5)
    
    # Unir geometrías con los datos de clústeres
    gdf_master = gdf_espana[['LAU_ID', 'geometry']].merge(df_master, on='LAU_ID', how='inner')
    print(f"   -> Geometrías unidas con éxito: {len(gdf_master)} municipios mapeados.")
    
    # Calcular matriz de pesos espaciales (Spatial Join)
    print("   -> Calculando matriz de vecindad (¿Qué municipio toca a cuál?)...")
    vecinos = gpd.sjoin(gdf_master[['LAU_ID', 'geometry']], gdf_master[['LAU_ID', 'geometry']], how='left', predicate='touches')
    
    print("   -> Calculando las medias vecinales...")
    
    # (El resto del código sigue exactamente igual a partir de aquí...)
    variables_espaciales = [
        'pob_absoluta_actual', 
        'delta_pob_pct', 
        'luz_absoluta_actual', 
        'delta_luz_pct', 
        'pib_act',              
        'emp_act',              
        'Indice_Conectividad', 
        'tasa_migratoria_pct'
    ]
    
    for var in variables_espaciales:
        vecinos[f'{var}_vecino'] = vecinos['LAU_ID_right'].map(gdf_master.set_index('LAU_ID')[var])
    
    medias_vecinales = vecinos.groupby('LAU_ID_left')[[f'{var}_vecino' for var in variables_espaciales]].mean().reset_index()
    
    gdf_master = gdf_master.merge(medias_vecinales, left_on='LAU_ID', right_on='LAU_ID_left', how='left').drop(columns=['LAU_ID_left'])
    
    for var in variables_espaciales:
        gdf_master[f'{var}_vecino'] = gdf_master[f'{var}_vecino'].fillna(gdf_master[var])
        
    print("✅ Efecto contagio calculado. Spatial Lags añadidos al dataset.")

    # ---------------------------------------------------------
    # 3. PREPARACIÓN DEL MODELO (Gradient Boosting)
    # ---------------------------------------------------------
    print("\n🧠 INICIANDO ENTRENAMIENTO DEL CLASIFICADOR (Gradient Boosting)...")
    
    # Transformar la variable objetivo a numérica
    mapping = {perfil: i for i, perfil in enumerate(sorted(gdf_master['Perfil_Final'].unique()))}
    gdf_master['Perfil_Final_Numerico'] = gdf_master['Perfil_Final'].map(mapping)
    
    # 🛑 ACTUALIZACIÓN: Las 16 variables exactas que salen del Clustering
    features_base = [
        'pob_absoluta_actual', 'delta_pob_pct', 'ratio_masculinidad',
        'luz_absoluta_actual', 'delta_luz_pct', 'luz_volatilidad_std',
        'pib_act', 'pib_std', 
        'emp_act', 'delta_emp_pct', 
        'Indice_Conectividad', 'Vehiculos_Oficial',
        'stations_density_km2', 'mean_distance_km_to_station',
        'tasa_migratoria_pct'
    ]
    
    features_espaciales = [f"{var}_vecino" for var in variables_espaciales]
    X = gdf_master[features_base + features_espaciales].fillna(0)
    y = gdf_master['Perfil_Final_Numerico']
    
    # Separación
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42, stratify=y)
    
    # ---------------------------------------------------------
    # 4. ENTRENAMIENTO
    # ---------------------------------------------------------
    gb = GradientBoostingClassifier(random_state=42)
    param_grid = {
        'n_estimators': [100, 200],
        'learning_rate': [0.05, 0.1],
        'max_depth': [3, 5]
    }
    
    print("   -> Buscando los mejores hiperparámetros (RandomizedSearchCV)...")
    gb_search = RandomizedSearchCV(gb, param_distributions=param_grid, n_iter=3, cv=3, random_state=42, n_jobs=-1)
    gb_search.fit(X_train, y_train)
    
    mejor_modelo = gb_search.best_estimator_
    
    # Evaluación
    preds = mejor_modelo.predict(X_test)
    print("\n📊 REPORTE DE CLASIFICACIÓN FINAL:")
    print(classification_report(y_test, preds, target_names=list(mapping.keys()), zero_division=0))

    # ---------------------------------------------------------
    # 5. FEATURE IMPORTANCE (Visualización)
    # ---------------------------------------------------------
    plt.figure(figsize=(12, 10))
    feat_importances = pd.Series(mejor_modelo.feature_importances_, index=X.columns)
    feat_importances.nlargest(20).plot(kind='barh', color='#313695')
    plt.title('Feature Importance - Gradient Boosting (Top 20 Variables con Spatial Lags)')
    plt.xlabel('Peso Predictivo')
    plt.tight_layout()
    
    ruta_grafica = os.path.join(FIGURES_DIR, "feature_importance_clasificador.png")
    plt.savefig(ruta_grafica)
    plt.close()

    # ---------------------------------------------------------
    # 6. EXPORTACIÓN BINARIA
    # ---------------------------------------------------------
    joblib.dump(mejor_modelo, os.path.join(MODEL_DIR, "gradient_boosting_perfiles.pkl"))
    joblib.dump(mapping, os.path.join(MODEL_DIR, "mapping_perfiles.pkl"))
    
    print(f"\n📸 Gráfica de Importancia exportada en: {ruta_grafica}")
    print(f"📦 Modelo Campeón (Gradient Boosting) exportado.")
    print("🏆 PIPELINE DE CLASIFICACIÓN FINALIZADO CON ÉXITO.")

if __name__ == "__main__":
    # Importamos las funciones de tu pipeline de clustering para generar la data fresca
    try:
        from pipeline_clustering import trozo_1_matriz_maestra, trozo_2_machine_learning
    except ImportError:
        print("❌ Error: Asegúrate de ejecutar esto en la misma carpeta que 'pipeline_clustering.py'")
        exit()
        
    print("🚀 INICIANDO ORQUESTACIÓN: CLUSTERING -> CLASIFICACIÓN...")
    
    # 1. Obtenemos la matriz limpia desde la DB
    df_base = trozo_1_matriz_maestra()
    
    # 2. Le pasamos el K-Means para que le asigne el 'Perfil_Final' a cada municipio
    df_etiquetado = trozo_2_machine_learning(df_base)
    
    # 3. Lanzamos la Clasificación Espacial con esos perfiles
    ejecutar_pipeline_clasificacion(df_etiquetado)