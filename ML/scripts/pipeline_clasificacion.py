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
from sklearn.ensemble import RandomForestClassifier, GradientBoostingClassifier
from sklearn.metrics import classification_report

# ---------------------------------------------------------
# 1. CONFIGURACIÓN DE RUTAS BLINDADAS
# ---------------------------------------------------------
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
MODEL_DIR = os.path.join(BASE_DIR, "..", "models", "modelos_exportados", "")
FIGURES_DIR = os.path.join(BASE_DIR, "..", "models", "figuras", "")

# El GeoJSON está en la raíz del proyecto
GEOJSON_PATH = os.path.abspath(os.path.join(BASE_DIR, "..", "..", "municipios_es.geojson"))

os.makedirs(MODEL_DIR, exist_ok=True)
os.makedirs(FIGURES_DIR, exist_ok=True)

def ejecutar_pipeline_clasificacion(df_master):
    # ---------------------------------------------------------
    # 2. ANÁLISIS ESPACIAL: Cálculo del 'Efecto Contagio'
    # ---------------------------------------------------------
    print("\n🌍 INICIANDO ANÁLISIS ESPACIAL: Cálculo del 'Efecto Contagio' (Spatial Lags)...")
    print(f"   -> Cargando mapa de España desde {GEOJSON_PATH}...")
    
    try:
        gdf = gpd.read_file(GEOJSON_PATH)
        gdf['LAU_ID'] = gdf['LAU_ID'].astype(str).str.zfill(5)
    except Exception as e:
        print(f"❌ Error cargando GeoJSON: {e}")
        return

    # Cruzamos con nuestro dataset maestro
    gdf_master = gdf.merge(df_master, left_on='LAU_ID', right_on='muni_id_join', how='inner')

    print("   -> Calculando matriz de vecindad (¿Qué municipio toca a cuál?)...")
    vecinos = gpd.sjoin(gdf_master[['LAU_ID', 'geometry']], gdf_master[['LAU_ID', 'geometry']], 
                        how='left', predicate='intersects')
    vecinos = vecinos[vecinos['LAU_ID_left'] != vecinos['LAU_ID_right']]

    print("   -> Calculando las medias vecinales...")
    vars_contagio = ['delta_pob_pct', 'delta_luz_pct', 'pib_media_historica']
    df_lags = pd.DataFrame({'LAU_ID': gdf_master['LAU_ID'].unique()})

    for var in vars_contagio:
        vecinos[f'{var}_vecino'] = vecinos['LAU_ID_right'].map(gdf_master.set_index('LAU_ID')[var])
        media_vecinos = vecinos.groupby('LAU_ID_left')[f'{var}_vecino'].mean().reset_index()
        nombre_columna_lag = f'{var}_LAG_VECINOS'
        media_vecinos.rename(columns={'LAU_ID_left': 'LAU_ID', f'{var}_vecino': nombre_columna_lag}, inplace=True)
        df_lags = df_lags.merge(media_vecinos, on='LAU_ID', how='left')

    df_lags.fillna(0, inplace=True)
    columnas_lag_existentes = [f'{var}_LAG_VECINOS' for var in vars_contagio]
    df_master = df_master.drop(columns=columnas_lag_existentes, errors='ignore')
    df_master = df_master.merge(df_lags, left_on='muni_id_join', right_on='LAU_ID', how='left').drop(columns=['LAU_ID'])

    print("✅ ¡Nuevas variables espaciales creadas con éxito!")

    # ---------------------------------------------------------
    # 3. PREPARACIÓN DE DATOS (Fase Predictiva)
    # ---------------------------------------------------------
    print("\n🚀 FASE PREDICTIVA: Preparando datos para modelos supervisados...")
    Y = df_master['Perfil_Final']
    perfiles_unicos = sorted(Y.unique())
    mapping = {perfil: i for i, perfil in enumerate(perfiles_unicos)}
    y_encoded = Y.map(mapping)

    columnas_a_borrar = [
        'muni_id_join', 'muni_display', 'prov_id_join', 
        'Cluster_GeoLumina', 'Perfil_Territorial', 'Perfil_Final', 'cluster_raw'
    ]
    X = df_master.drop(columns=columnas_a_borrar, errors='ignore').fillna(0)

    print(f"   -> Tenemos {X.shape[1]} variables predictoras (incluyendo entorno espacial).")
    X_train, X_test, y_train, y_test = train_test_split(X, y_encoded, test_size=0.20, random_state=42, stratify=y_encoded)
    print(f"✅ Datos divididos: {len(X_train)} municipios para entrenar, {len(X_test)} para examinar al modelo.")

    # ---------------------------------------------------------
    # 4. COMPETICIÓN DE ALGORITMOS (Búsqueda de Hiperparámetros)
    # ---------------------------------------------------------
    print("\n⚔️ INICIANDO COMPETICIÓN DE ALGORITMOS (Búsqueda de Hiperparámetros)...")
    
    # --- RANDOM FOREST ---
    print("🌲 Entrenando Random Forest...")
    t0_rf = time.time()
    rf_grid = {
        'n_estimators': [100, 200],
        'min_samples_split': [2, 5],
        'max_depth': [10, 20, None],
        'class_weight': ['balanced', None]
    }
    rf_search = RandomizedSearchCV(RandomForestClassifier(random_state=42), rf_grid, n_iter=5, cv=3, random_state=42, n_jobs=-1)
    rf_search.fit(X_train, y_train)
    tiempo_rf = round(time.time() - t0_rf, 1)
    print(f"✅ Random Forest listo en {tiempo_rf} segundos. Mejores hiperparámetros: {rf_search.best_params_}")

    # --- GRADIENT BOOSTING (XGBoost style) ---
    print("🚀 Entrenando Gradient Boosting (Estilo XGBoost)...")
    t0_gb = time.time()
    gb_grid = {
        'n_estimators': [100, 200],
        'learning_rate': [0.05, 0.1],
        'max_depth': [3, 5],
        'subsample': [0.8, 1.0]
    }
    gb_search = RandomizedSearchCV(GradientBoostingClassifier(random_state=42), gb_grid, n_iter=5, cv=3, random_state=42, n_jobs=-1)
    gb_search.fit(X_train, y_train)
    tiempo_gb = round(time.time() - t0_gb, 1)
    print(f"✅ Gradient Boosting listo en {tiempo_gb} segundos. Mejores hiperparámetros: {gb_search.best_params_}")

    # ---------------------------------------------------------
    # 5. EVALUACIÓN Y SELECCIÓN DEL CAMPEÓN
    # ---------------------------------------------------------
    print("\n🏆 EVALUANDO LOS MODELOS CON EL 20% DE DATOS OCULTOS (TEST):")
    print("-" * 60)
    print("REPORTE DE RANDOM FOREST:")
    y_pred_rf = rf_search.predict(X_test)
    
    # FIX APLICADO: labels y zero_division para que no colapse si falta alguna clase rara
    print(classification_report(
        y_test, 
        y_pred_rf, 
        target_names=perfiles_unicos, 
        labels=list(mapping.values()), 
        zero_division=0
    ))

    print("-" * 60)
    print("REPORTE DE GRADIENT BOOSTING:")
    y_pred_gb = gb_search.predict(X_test)
    
    print(classification_report(
        y_test, 
        y_pred_gb, 
        target_names=perfiles_unicos, 
        labels=list(mapping.values()), 
        zero_division=0
    ))

    # ---------------------------------------------------------
    # 6. EXPORTACIÓN DE GRÁFICAS Y MODELOS (.png y .pkl)
    # ---------------------------------------------------------
    # Seleccionamos el campeón (Gradient Boosting en tu reporte original)
    mejor_modelo = gb_search.best_estimator_

    # 📊 FEATURE IMPORTANCE (Guardado como .png para el cuadro de mando)
    plt.figure(figsize=(12, 10))
    feat_importances = pd.Series(mejor_modelo.feature_importances_, index=X.columns)
    feat_importances.nlargest(20).plot(kind='barh', color='#313695')
    plt.title('Feature Importance - Gradient Boosting (Top 20 Variables con Spatial Lags)')
    plt.xlabel('Peso Predictivo')
    plt.tight_layout()
    plt.savefig(f"{FIGURES_DIR}feature_importance_clasificador.png")
    plt.close()

    # 📦 EXPORTACIÓN BINARIA
    joblib.dump(mejor_modelo, f"{MODEL_DIR}gradient_boosting_perfiles.pkl")
    joblib.dump(mapping, f"{MODEL_DIR}mapping_perfiles.pkl")
    
    print(f"\n📊 Gráfica de Feature Importance exportada en: {FIGURES_DIR}feature_importance_clasificador.png")
    print(f"📦 Modelo Campeón (Gradient Boosting) exportado en: {MODEL_DIR}gradient_boosting_perfiles.pkl")
    print("🏆 PIPELINE DE CLASIFICACIÓN FINALIZADO.")

import geopandas as gpd

def generar_mapa_final_automatico(df_final):
    print("\n🗺️  Generando visualización cartográfica de GeoLúmica...")
    
    # 1. Tu escala divergente exacta
    colores_divergentes = {
        '1 - Despoblación Grave (Pueblos en riesgo crítico)': '#d73027',
        '2 - Pérdida Moderada (Pueblos que se vacían lentamente)': '#fc8d59',
        '3 - Población Estable (Pueblos medianos sin grandes cambios)': '#fee090',
        '4 - Crecimiento Leve (Pueblos que atraen nuevos vecinos)': '#e0f3f8',
        '5 - Fuerte Crecimiento (Zonas residenciales y turísticas)': '#91bfdb',
        '6 - Grandes Ciudades (Capitales y grandes núcleos)': '#4575b4',
        '7 - Enormes Centros Logísticos (Zonas de gran industria y transporte)': '#313695'
    }

    # 2. Cargar el GeoJSON (Ruta absoluta desde la raíz del proyecto)
    # Como el script está en ML/scripts/, subimos dos niveles
    ruta_geojson = os.path.abspath(os.path.join(BASE_DIR, "..", "..", "municipios_es.geojson"))
    
    try:
        print(f"📂 Cargando cartografía desde: {ruta_geojson}")
        gdf = gpd.read_file(ruta_geojson)
        gdf['LAU_ID'] = gdf['LAU_ID'].astype(str).str.zfill(5)

        # 3. Cruzar con los resultados del ML
        # Aseguramos que lau_id sea string de 5 dígitos
        df_final['muni_id_join'] = df_final['muni_id_join'].astype(str).str.split('.').str[0].str.zfill(5)
        
        gdf_mapa = gdf.merge(df_final, left_on='LAU_ID', right_on='muni_id_join', how='left')

        # 4. Pintar el mapa
        fig, ax = plt.subplots(1, 1, figsize=(16, 12))
        
        # Asignamos los colores según tu diccionario
        gdf_mapa.plot(
            ax=ax,
            color=[colores_divergentes.get(p, '#e0e0e0') for p in gdf_mapa['Perfil_Final']],
            edgecolor='black',
            linewidth=0.03
        )

        ax.set_title("GeoLúmica: Segmentación Territorial Final", fontsize=18, fontweight='bold')
        ax.axis('off')

        # Guardar en la carpeta de figuras que ya tienes creada
        output_path = os.path.join(FIGURES_DIR, "MAPA_PROYECTO_FINAL.png")
        plt.savefig(output_path, dpi=300, bbox_inches='tight')
        print(f"✅ ¡Mapa generado con éxito en: {output_path}")
        
    except Exception as e:
        print(f"❌ Error al generar el mapa: {e}")

# --- DENTRO DE TU FUNCIÓN PRINCIPAL DE ML ---
# Al final del proceso, antes del return df_final:
# generar_mapa_final_automatico(df_final)

if __name__ == "__main__":
    from pipeline_clustering import trozo_1_matriz_maestra, trozo_2_machine_learning
    
    # 1. Extracción y Feature Engineering
    df_raw = trozo_1_matriz_maestra()
    
    # 2. Asignación temporal de etiquetas vía Clustering en memoria
    df_etiquetado = trozo_2_machine_learning(df_raw)
    
    # 3. Lags, Competición GridSearchCV, Evaluación y Exportación
    ejecutar_pipeline_clasificacion(df_etiquetado)

    generar_mapa_final_automatico(df_final)