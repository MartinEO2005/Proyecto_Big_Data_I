import pandas as pd
import numpy as np
from sqlalchemy import create_engine
import warnings
warnings.filterwarnings('ignore')

import matplotlib.pyplot as plt
from sklearn.preprocessing import StandardScaler
from sklearn.decomposition import PCA
from sklearn.cluster import KMeans
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import silhouette_score
import joblib
import os
import seaborn as sns

BASE_DIR = os.path.dirname(os.path.abspath(__file__)) 

# Subimos un nivel para llegar a 'ML' y ahí apuntamos a 'models'
MODEL_DIR = os.path.join(BASE_DIR, "..", "models", "modelos_exportados", "")
FIGURES_DIR = os.path.join(BASE_DIR, "..", "models", "figuras", "")

# Creamos las carpetas si no existen
os.makedirs(MODEL_DIR, exist_ok=True)
os.makedirs(FIGURES_DIR, exist_ok=True)

print(f"📁 Las carpetas se gestionarán en: {os.path.abspath(os.path.join(BASE_DIR, '..', 'models'))}")
# =================================================================

def trozo_1_matriz_maestra():
    # ---------------------------------------------------------
    # 1. EXTRACCIÓN DESDE CAPA SILVER (Tablas Históricas)
    # ---------------------------------------------------------
    print("🔌 Conexión establecida con GeoLúmica DB: proyecto_big_data")
    DB_URL = "mysql+pymysql://bd_rvm_gelumica:Rio45Abc@10.151.30.2:3306/bd_rvm_gelumica"
    engine = create_engine(DB_URL)

    # Extraemos las tablas completas
    df_geo = pd.read_sql("SELECT lau_id, nombre_municipio, provincia_id FROM dim_geografia", engine)
    df_demografia = pd.read_sql("SELECT lau_id, year, poblacion_muni, Hombres, Mujeres FROM fact_demografia", engine)
    df_viirs = pd.read_sql("SELECT lau_id, year, intensidad_luz FROM fact_viirs", engine)
    df_pib = pd.read_sql("SELECT lau_id, year, renta_media FROM fact_renta", engine)
    df_empresas = pd.read_sql("SELECT lau_id, year, total_empresas FROM fact_empresas", engine)
    df_conectividad = pd.read_sql("SELECT lau_id, year, indice_conectividad, Vehiculos_Oficial FROM fact_conectividad", engine)
    df_migraciones = pd.read_sql("SELECT lau_id, year, migrantes, nacionalidad FROM fact_migracion", engine)
    df_osm = pd.read_sql("SELECT * FROM fact_osm", engine)

    # 🛑 CRUCIAL: Estandarizar todos los lau_id a String de 5 dígitos (con ceros a la izquierda)
    # Esto evita que el merge falle y la población se vuelva 0
    tablas = [df_geo, df_demografia, df_viirs, df_pib, df_empresas, df_conectividad, df_migraciones, df_osm]
    for df in tablas:
        # Buscamos la columna de ID (puede ser lau_id o sog_id en OSM)
        col_id = 'lau_id' if 'lau_id' in df.columns else ('sog_id' if 'sog_id' in df.columns else None)
        if col_id:
            df[col_id] = df[col_id].astype(str).str.zfill(5)

    # 🛑 FIX PARA OSM: Traemos TODO, estandarizamos y preparamos para el Merge
    df_osm = pd.read_sql("SELECT * FROM fact_osm", engine)
    df_osm.columns = [c.lower() for c in df_osm.columns] # Todo a minúsculas
    if 'sog_id' in df_osm.columns and 'lau_id' not in df_osm.columns:
        df_osm = df_osm.rename(columns={'sog_id': 'lau_id'})
    df_osm = df_osm[['lau_id', 'mean_distance_km_to_station', 'stations_density_km2']]

    # ---------------------------------------------------------
    # ⚙️ FEATURE ENGINEERING (Tu Lógica Exacta Adaptada)
    # ---------------------------------------------------------
    print("⚙️ [1/5] Creando Esqueleto y calculando Deltas Demográficos...")
    
    print("⚙️ [1/5] Creando Esqueleto y calculando Deltas Demográficos...")
    
    # 1. Esqueleto Base (Asegurando IDs perfectos)
    df_geo = df_geo.rename(columns={'lau_id': 'muni_key', 'nombre_municipio': 'muni_display'})
    df_master = df_geo[['muni_key', 'muni_display', 'provincia_id']].copy()
    df_master = df_master.rename(columns={'muni_key': 'muni_id_join', 'provincia_id': 'prov_id_join'})
    
    # 🛑 LIJADO DE ID 1: Convertir a texto, quitar decimales fantasmas (.0) y poner 5 dígitos
    df_master['muni_id_join'] = df_master['muni_id_join'].astype(str).str.split('.').str[0].str.zfill(5)

    # 2. Demografía
    df_demografia = df_demografia.rename(columns={'lau_id': 'muni_id_join', 'poblacion_muni': 'Total'})
    
    # 🛑 LIJADO DE ID 2: Hacemos exactamente lo mismo en la otra tabla
    df_demografia['muni_id_join'] = df_demografia['muni_id_join'].astype(str).str.split('.').str[0].str.zfill(5)

    # Forzar tipos numéricos
    df_demografia['year'] = pd.to_numeric(df_demografia['year'], errors='coerce')
    df_demografia['Total'] = pd.to_numeric(df_demografia['Total'], errors='coerce')
    df_demografia['Hombres'] = pd.to_numeric(df_demografia['Hombres'], errors='coerce')
    df_demografia['Mujeres'] = pd.to_numeric(df_demografia['Mujeres'], errors='coerce')

    # 🛑 FIJAMOS EL AÑO REAL (Evitamos años "trampa" como 2025 o 2026 vacíos)
    max_yr_demo = 2023
    min_yr_demo = 2000 # O el año base que uses, ej. 2010 o 2015

    demo_actual = df_demografia[df_demografia['year'] == max_yr_demo][['muni_id_join', 'Total', 'Hombres', 'Mujeres']].copy()
    demo_actual = demo_actual.rename(columns={'Total': 'pob_absoluta_actual'})
    demo_actual['ratio_masculinidad'] = demo_actual['Hombres'] / (demo_actual['Mujeres'] + 0.1) 
    
    demo_hist = df_demografia[df_demografia['year'] == min_yr_demo][['muni_id_join', 'Total']].copy()
    demo_hist = demo_hist.rename(columns={'Total': 'pob_historica'})

    demo_trend = pd.merge(demo_actual, demo_hist, on='muni_id_join', how='left')
    demo_trend['delta_pob_pct'] = ((demo_trend['pob_absoluta_actual'] - demo_trend['pob_historica']) / (demo_trend['pob_historica'] + 1)) * 100

    df_master = pd.merge(df_master, demo_trend[['muni_id_join', 'pob_absoluta_actual', 'delta_pob_pct', 'ratio_masculinidad']], on='muni_id_join', how='left')

    # 🔍 DEBUG: Comprobemos a Madrid de nuevo
    madrid_check = df_master[df_master['muni_id_join'] == '28079']['pob_absoluta_actual']
    if not madrid_check.empty:
        print(f"🔎 DEBUG: Población detectada en Madrid: {madrid_check.values[0]}")
    else:
        print("❌ DEBUG: Madrid NO ENCONTRADA en el cruce.")

    print("⚙️ [2/5] Procesando Satélites VIIRS (Consumo y Volatilidad)...")
    df_viirs = df_viirs.rename(columns={'lau_id': 'muni_id_join', 'intensidad_luz': 'mean'})
    viirs_anual = df_viirs.groupby(['muni_id_join', 'year'])['mean'].mean().reset_index()
    max_yr_viirs, min_yr_viirs = viirs_anual['year'].max(), viirs_anual['year'].min()

    viirs_actual = viirs_anual[viirs_anual['year'] == max_yr_viirs][['muni_id_join', 'mean']].rename(columns={'mean': 'luz_absoluta_actual'})
    viirs_hist = viirs_anual[viirs_anual['year'] == min_yr_viirs][['muni_id_join', 'mean']].rename(columns={'mean': 'luz_historica'})

    viirs_trend = pd.merge(viirs_actual, viirs_hist, on='muni_id_join', how='left')
    viirs_trend['delta_luz_pct'] = ((viirs_trend['luz_absoluta_actual'] - viirs_trend['luz_historica']) / (viirs_trend['luz_historica'] + 0.01)) * 100

    viirs_std = df_viirs.groupby('muni_id_join')['mean'].std().reset_index().rename(columns={'mean': 'luz_volatilidad_std'})

    df_master = pd.merge(df_master, viirs_trend[['muni_id_join', 'luz_absoluta_actual', 'delta_luz_pct']], on='muni_id_join', how='left')
    df_master = pd.merge(df_master, viirs_std, on='muni_id_join', how='left')
    print("  ✅ VIIRS procesado e integrado con éxito.")

    print("⚙️ [3/5] Integrando Riqueza e Imputando Secreto Estadístico...")
    df_pib = df_pib.rename(columns={'lau_id': 'muni_id_join', 'renta_media': 'pib'})
    max_yr_pib = df_pib['year'].max()

    pib_actual = df_pib[df_pib['year'] == max_yr_pib][['muni_id_join', 'pib']].rename(columns={'pib': 'pib_absoluto_actual'})
    pib_stats = df_pib.groupby('muni_id_join')['pib'].agg(pib_media_historica='mean', pib_volatilidad_std='std').reset_index()

    df_master = pd.merge(df_master, pib_actual, on='muni_id_join', how='left')
    df_master = pd.merge(df_master, pib_stats, on='muni_id_join', how='left')

    # Imputación Inteligente
    for col in ['pib_absoluto_actual', 'pib_media_historica', 'pib_volatilidad_std']:
        df_master[col] = df_master.groupby('prov_id_join')[col].transform(lambda x: x.fillna(x.median()))

    print("⚙️ [4/5] Fusionando Empresas, Transporte (OSM) y Magnetismo Migratorio...")
    df_empresas = df_empresas.rename(columns={'lau_id': 'muni_id_join'})
    max_yr_emp, min_yr_emp = df_empresas['year'].max(), df_empresas['year'].min()
    emp_actual = df_empresas[df_empresas['year'] == max_yr_emp][['muni_id_join', 'total_empresas']].rename(columns={'total_empresas': 'emp_act'})
    emp_hist = df_empresas[df_empresas['year'] == min_yr_emp][['muni_id_join', 'total_empresas']].rename(columns={'total_empresas': 'emp_hist'})
    
    emp_trend = pd.merge(emp_actual, emp_hist, on='muni_id_join', how='left')
    emp_trend['delta_empresas_transporte_pct'] = ((emp_trend['emp_act'] - emp_trend['emp_hist']) / (emp_trend['emp_hist'] + 1)) * 100
    df_master = pd.merge(df_master, emp_trend[['muni_id_join', 'emp_act', 'delta_empresas_transporte_pct']].rename(columns={'emp_act': 'empresas_transporte_actual'}), on='muni_id_join', how='left')

    df_conectividad = df_conectividad.rename(columns={'lau_id': 'muni_id_join', 'indice_conectividad': 'Indice_Conectividad', 'Vehiculos_Oficial': 'Pct_Vehiculos_Muni_vs_Prov'})
    max_con = df_conectividad['year'].max()
    con_actual = df_conectividad[df_conectividad['year'] == max_con][['muni_id_join', 'Indice_Conectividad', 'Pct_Vehiculos_Muni_vs_Prov']]
    df_master = pd.merge(df_master, con_actual, on='muni_id_join', how='left')

    # ---> AQUÍ ESTÁ EL MERGE DE OSM RECUPERADO <---
    df_osm = df_osm.rename(columns={'lau_id': 'muni_id_join'})
    df_master = pd.merge(df_master, df_osm[['muni_id_join', 'stations_density_km2', 'mean_distance_km_to_station']], on='muni_id_join', how='left')

    df_migraciones = df_migraciones.rename(columns={'lau_id': 'muni_id_join'})
    max_mig = df_migraciones['year'].max()
    mig_actual = df_migraciones[(df_migraciones['year'] == max_mig) & (df_migraciones['nacionalidad'].str.lower() == 'total')]
    df_master = pd.merge(df_master, mig_actual[['muni_id_join', 'migrantes']].rename(columns={'migrantes': 'mig_act'}), on='muni_id_join', how='left')

    df_master['tasa_migratoria_pct'] = (df_master['mig_act'] / (df_master['pob_absoluta_actual'] + 1)) * 100
    df_master = df_master.drop(columns=['mig_act'])

    print("⚙️ [5/5] Limpieza de Nulos Residuales (Penalizaciones Matemáticas)...")
    df_master['mean_distance_km_to_station'] = df_master['mean_distance_km_to_station'].fillna(100.0)
    df_master = df_master.fillna(0)

    print("\n" + "="*80)
    print(f"✅ ¡MATRIZ MAESTRA LISTA PARA PCA! Dimensiones: {df_master.shape[0]} filas x {df_master.shape[1]} columnas.")
    print("="*80)
    
    return df_master

# Aseguramos que las carpetas existan


def trozo_2_machine_learning(df_master):
    print("\n🧠 INICIANDO PIPELINE DE MACHINE LEARNING (Modelo Híbrido de Dos Fases)...")
    print("   -> Separando las Grandes Urbes del mundo Rural...")
    
    UMBRAL_POBLACION = 50000 
    
    df_outliers = df_master[df_master['pob_absoluta_actual'] > UMBRAL_POBLACION].copy()
    df_rural = df_master[df_master['pob_absoluta_actual'] <= UMBRAL_POBLACION].copy()
    
    print(f"   🏙️ Gigantes aislados: {len(df_outliers)} municipios.")
    print(f"   🌾 Municipios rurales a analizar: {len(df_rural)} municipios.")

    # ---------------------------------------------------------
    # 1. ESTANDARIZACIÓN Y PCA
    # ---------------------------------------------------------
    cols_a_excluir = ['muni_id_join', 'muni_display', 'prov_id_join']
    features = [c for c in df_rural.columns if c not in cols_a_excluir]
    
    X_rural = df_rural[features]
    
    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X_rural)
    print(f"✅ Datos rurales estandarizados. Forma de la matriz: {X_scaled.shape}")

    print("⚙️ [1/2] Aplicando PCA Definitivo a la España Rural (10 Componentes)...")
    pca = PCA(n_components=10, random_state=42)
    X_pca = pca.fit_transform(X_scaled)
    print(f"✅ Matriz reducida lista. Nueva forma: {X_pca.shape}")

    # 📊 GRÁFICA 1: Varianza Explicada (PCA)
    plt.figure(figsize=(10, 5))
    plt.plot(range(1, 11), pca.explained_variance_ratio_.cumsum(), marker='o', linestyle='--', color='#0078d4')
    plt.title('Varianza Explicada Acumulada - PCA (10 Componentes)')
    plt.xlabel('Número de Componentes')
    plt.ylabel('Varianza Acumulada')
    plt.grid(True)
    plt.savefig(f"{FIGURES_DIR}pca_variance.png", bbox_inches='tight')
    plt.close()

    # ---------------------------------------------------------
    # 2. VALIDACIÓN DEL K OPTIMO (Codo y Silhouette)
    # ---------------------------------------------------------
    print("⚙️ Evaluando métricas de Codo y Silhouette para K=2 hasta K=10...")
    inertias = []
    silhouettes = []
    K_range = range(2, 11)
    
    for k in K_range:
        km_temp = KMeans(n_clusters=k, random_state=42, n_init=10)
        labels_temp = km_temp.fit_predict(X_pca)
        inertias.append(km_temp.inertia_)
        silhouettes.append(silhouette_score(X_pca, labels_temp))

    # 📊 GRÁFICA 2: Método del Codo y Silhouette (Combinada)
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(15, 5))
    
    ax1.plot(K_range, inertias, marker='o', color='#e74c3c')
    ax1.set_title('Método del Codo (Elbow)')
    ax1.set_xlabel('Número de Clusters (K)')
    ax1.set_ylabel('Inercia')
    ax1.grid(True)

    ax2.plot(K_range, silhouettes, marker='s', color='#2ecc71')
    ax2.set_title('Coeficiente de Silhouette')
    ax2.set_xlabel('Número de Clusters (K)')
    ax2.set_ylabel('Score Silhouette')
    ax2.grid(True)

    plt.tight_layout()
    plt.savefig(f"{FIGURES_DIR}codo_silhouette.png", bbox_inches='tight')
    plt.close()
    print(f"📊 Gráficas de validación guardadas en: {FIGURES_DIR}")

    # ---------------------------------------------------------
    # 3. K-MEANS CLUSTERING (K=6)
    # ---------------------------------------------------------
    print("🚀 LISTOS PARA K-MEANS: Entrenando modelo final con K=6...")
    kmeans = KMeans(n_clusters=6, random_state=42, n_init=10)
    df_rural['cluster_raw'] = kmeans.fit_predict(X_pca)
    df_rural['Perfil_Territorial'] = 'Clúster Rural ' + df_rural['cluster_raw'].astype(str)

    # ---------------------------------------------------------
    # 4. MAPEO AL DICCIONARIO DIVERGENTE Y COLORES
    # ---------------------------------------------------------
    diccionario_divergente = {
        'Clúster Rural 5': '1 - Despoblación Grave (Pueblos en riesgo crítico)',
        'Clúster Rural 1': '2 - Pérdida Moderada (Pueblos que se vacían lentamente)',
        'Clúster Rural 3': '3 - Población Estable (Pueblos medianos sin grandes cambios)',
        'Clúster Rural 0': '4 - Crecimiento Leve (Pueblos que atraen nuevos vecinos)',
        'Clúster Rural 2': '5 - Fuerte Crecimiento (Zonas residenciales y turísticas)',
        '0 - Grandes Urbes y Motores Regionales': '6 - Grandes Ciudades (Capitales y grandes núcleos)',
        'Clúster Rural 4': '7 - Enormes Centros Logísticos (Zonas de gran industria y transporte)'
    }

    colores_divergentes = {
        '1 - Despoblación Grave (Pueblos en riesgo crítico)': '#d73027',        # Rojo Alerta
        '2 - Pérdida Moderada (Pueblos que se vacían lentamente)': '#fc8d59',   # Naranja
        '3 - Población Estable (Pueblos medianos sin grandes cambios)': '#fee090', # Amarillo
        '4 - Crecimiento Leve (Pueblos que atraen nuevos vecinos)': '#e0f3f8',   # Azul muy clarito
        '5 - Fuerte Crecimiento (Zonas residenciales y turísticas)': '#91bfdb', # Azul claro
        '6 - Grandes Ciudades (Capitales y grandes núcleos)': '#4575b4',        # Azul oscuro
        '7 - Enormes Centros Logísticos (Zonas de gran industria y transporte)': '#313695' # Azul marino profundo
    }

    df_rural['Perfil_Final'] = df_rural['Perfil_Territorial'].map(diccionario_divergente)
    
    # A los gigantes les asignamos automáticamente la categoría 6
    df_outliers['Perfil_Final'] = diccionario_divergente['0 - Grandes Urbes y Motores Regionales']
    
    # Reconstruimos España entera (8131 municipios clasificados)
    df_final = pd.concat([df_rural, df_outliers], ignore_index=True)

    # 📊 GRÁFICA 3: Scatter de PCA en 2D con los colores de tu paleta
    plt.figure(figsize=(12, 8))
    sns.scatterplot(
        x=X_pca[:, 0], 
        y=X_pca[:, 1], 
        hue=df_rural['Perfil_Final'], 
        palette=colores_divergentes, 
        s=60, alpha=0.8, edgecolor=None
    )
    plt.title('Distribución Espacial de Clústeres (PCA 1 vs PCA 2)')
    plt.xlabel('Componente Principal 1')
    plt.ylabel('Componente Principal 2')
    plt.legend(bbox_to_anchor=(1.05, 1), loc='upper left', title="Perfiles Estratégicos")
    plt.tight_layout()
    plt.savefig(f"{FIGURES_DIR}pca_scatter_clusters.png")
    plt.close()

    # ---------------------------------------------------------
    # 5. RANDOM FOREST CLASSIFIER (Feature Importance)
    # ---------------------------------------------------------
    print("🧠 Entrenando Clasificador Supervisado para decodificar K-Means...")
    rf_classifier = RandomForestClassifier(n_estimators=100, random_state=42)
    rf_classifier.fit(X_rural, df_rural['cluster_raw'])

    # 📊 GRÁFICA 4: Feature Importance
    importances = rf_classifier.feature_importances_
    indices = np.argsort(importances)[::-1]

    plt.figure(figsize=(12, 6))
    sns.barplot(x=importances[indices], y=np.array(features)[indices], palette="viridis")
    plt.title('Feature Importance del Clasificador K-Means (España Rural)')
    plt.xlabel('Peso Predictivo (0.0 a 1.0)')
    plt.tight_layout()
    plt.savefig(f"{FIGURES_DIR}feature_importance_clustering.png")
    plt.close()

    # ---------------------------------------------------------
    # 6. SERIALIZACIÓN (Modelos "Congelados")
    # ---------------------------------------------------------
    joblib.dump(scaler, f"{MODEL_DIR}scaler_rural.pkl")
    joblib.dump(pca, f"{MODEL_DIR}pca_10comp_rural.pkl")
    joblib.dump(kmeans, f"{MODEL_DIR}kmeans_rural.pkl")
    joblib.dump(rf_classifier, f"{MODEL_DIR}clasificador_perfiles.pkl")

    print(f"📦 Modelos y figuras exportados con éxito en la carpeta /models/")
    print("🏆 PIPELINE DE MACHINE LEARNING COMPLETADO EXITOSAMENTE.")
    
    return df_final


# ====================================================================
# EL DIRECTOR DE ORQUESTA (Único bloque __main__ al final del archivo)
# ====================================================================
if __name__ == "__main__":
    # 1. Ejecutamos el Trozo 1 (Extracción y Feature Engineering)
    # Asegúrate de que tienes la función trozo_1_matriz_maestra() justo arriba de este código
    matriz_maestra = trozo_1_matriz_maestra()
    
    # 2. Ejecutamos el Trozo 2 pasándole la matriz (Machine Learning)
    dataset_clasificado = trozo_2_machine_learning(matriz_maestra)
    
    # 3. Opcional: Verificamos los resultados
    print("\n🔍 Muestra de resultados finales:")
    print(dataset_clasificado[['muni_display', 'Perfil_Final']].sample(5))