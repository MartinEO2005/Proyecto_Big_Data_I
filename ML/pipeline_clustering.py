import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import os
import joblib
import geopandas as gpd
from sqlalchemy import create_engine, text
from sklearn.preprocessing import StandardScaler
from sklearn.decomposition import PCA
from sklearn.cluster import KMeans
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import silhouette_score
from matplotlib.lines import Line2D
import json                     # <-- NUEVO
from datetime import datetime

# --- CONFIGURACIÓN GLOBAL ---
COLORES_GEOLUMICA = {
    '1 - Despoblación Grave (Riesgo Crítico)': '#d73027',
    '2 - Pérdida Moderada (Rural en Retroceso)': '#f46d43',
    '3 - Estancamiento Rural (Declive Suave)': '#fdae61',
    '4 - Población Estable (Núcleos Tradicionales)': '#fee090',
    '5 - Fuerte Crecimiento (Zonas de Expansión)': '#abd9e9',
    '6 - Grandes Ciudades (Municipios Aislados)': '#74add1',
    '7 - Enormes Centros (Motores Regionales)': '#4575b4'
}

# ---------------------------------------------------------
# 1. CONFIGURACIÓN DE RUTAS BLINDADAS (Todo dentro de ML)
# ---------------------------------------------------------
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
MODEL_DIR = os.path.join(BASE_DIR, "models", "modelos_exportados")
FIGURES_DIR = os.path.join(BASE_DIR, "models", "figuras")
GEOJSON_PATH = os.path.join(BASE_DIR, "municipios_es.geojson")

os.makedirs(MODEL_DIR, exist_ok=True)
os.makedirs(FIGURES_DIR, exist_ok=True)

# ---------------------------------------------------------
# 2. FUNCIÓN UNIFICADA DE SYSTEM HEALTH
# ---------------------------------------------------------
def actualizar_estado_modelo(nombre_modelo, precision_o_error, tipo_metrica):
    ruta_json = os.path.join(MODEL_DIR, "system_health.json")
    
    datos_estado = {
        "servicios": {
            "VIIRS (Luz Nocturna)": {"estado": "ok", "fecha": "Hace 12 días"},
            "OpenStreetMap": {"estado": "ok", "fecha": "Hace 12 horas"},
            "INE Demografía": {"estado": "warning", "fecha": "Diciembre 2023 (Anual)"}
        },
        "modelos": {}
    }

    if os.path.exists(ruta_json):
        try:
            with open(ruta_json, "r", encoding="utf-8") as f:
                datos_estado = json.load(f)
        except Exception: pass

    datos_estado["modelos"][nombre_modelo] = {
        "fecha_entrenamiento": datetime.now().strftime("%Y-%m-%d %H:%M"),
        "metrica": f"{tipo_metrica}: {precision_o_error}"
    }

    os.makedirs(os.path.dirname(ruta_json), exist_ok=True)
    with open(ruta_json, "w", encoding="utf-8") as f:
        json.dump(datos_estado, f, indent=4)

# =================================================================

def trozo_1_matriz_maestra():
    print("🔌 Conexión establecida con GeoLúmica DB: proyecto_big_data")
    DB_URL = "mysql+pymysql://bd_rvm_gelumica:Rio45Abc@10.151.30.2:3306/bd_rvm_gelumica"
    engine = create_engine(DB_URL)

    # 1. Carga con nombres estandarizados desde el origen
    df_geo = pd.read_sql("SELECT muni_key AS muni_id_join, muni_display AS nombre_municipio FROM dim_geografia", engine)
    df_dem = pd.read_sql("SELECT muni_id_join, Anio AS year, Total, Hombres, Mujeres FROM fact_demografia", engine)
    df_v   = pd.read_sql("SELECT muni_id_join, Anio AS year, mean AS intensidad_luz FROM fact_viirs", engine)
    df_pib = pd.read_sql("SELECT muni_id_join, Anio AS year, pib FROM fact_renta", engine)
    df_emp = pd.read_sql("SELECT muni_id_join, Anio AS year, num_empresas_transporte FROM fact_empresas", engine)
    df_con = pd.read_sql("SELECT muni_id_join, Anio AS year, Indice_Conectividad, Vehiculos_Oficial FROM fact_conectividad", engine)
    df_mig = pd.read_sql("SELECT muni_id_join, Anio AS year, migracion_total FROM fact_migracion", engine)
    df_osm = pd.read_sql("SELECT muni_id_join, stations_density_km2, mean_distance_km_to_station FROM fact_osm", engine)

    # Limpieza de IDs (Zfill 5)
    for df in [df_geo, df_dem, df_v, df_pib, df_emp, df_con, df_mig, df_osm]:
        df['muni_id_join'] = df['muni_id_join'].astype(str).str.split('.').str[0].str.zfill(5)

    # --- ANCLA GEOGRÁFICA (Asegura los 8.131) ---
    df_master = df_geo.copy()

    # 2. Feature Engineering
    # Demografía (Deltas)
    max_y, min_y = df_dem['year'].max(), df_dem['year'].min()
    d_act = df_dem[df_dem['year'] == max_y][['muni_id_join', 'Total', 'Hombres', 'Mujeres']].drop_duplicates('muni_id_join')
    d_his = df_dem[df_dem['year'] == min_y][['muni_id_join', 'Total']].drop_duplicates('muni_id_join').rename(columns={'Total': 'pob_his'})
    df_master = pd.merge(df_master, d_act, on='muni_id_join', how='left')
    df_master = pd.merge(df_master, d_his, on='muni_id_join', how='left')
    df_master['pob_absoluta_actual'] = df_master['Total']
    df_master['ratio_masculinidad'] = df_master['Hombres'] / (df_master['Mujeres'] + 0.1)
    df_master['delta_pob_pct'] = ((df_master['Total'] - df_master['pob_his']) / (df_master['pob_his'] + 1)) * 100

    # VIIRS (Luz y Volatilidad)
    v_stats = df_v.groupby('muni_id_join')['intensidad_luz'].agg(luz_absoluta_actual='last', luz_his='first', luz_volatilidad_std='std').reset_index()
    v_stats['delta_luz_pct'] = ((v_stats['luz_absoluta_actual'] - v_stats['luz_his']) / (v_stats['luz_his'] + 0.01)) * 100
    df_master = pd.merge(df_master, v_stats, on='muni_id_join', how='left')

    # Renta y Empresas
    df_master = pd.merge(df_master, df_pib.groupby('muni_id_join')['pib'].agg(pib_act='last', pib_his='mean', pib_std='std').reset_index(), on='muni_id_join', how='left')
    
    emp_s = df_emp.groupby('muni_id_join')['num_empresas_transporte'].agg(emp_act='last', emp_base='first').reset_index()
    emp_s['delta_emp_pct'] = ((emp_s['emp_act'] - emp_s['emp_base']) / (emp_s['emp_base'] + 1)) * 100
    df_master = pd.merge(df_master, emp_s[['muni_id_join', 'emp_act', 'delta_emp_pct']], on='muni_id_join', how='left')

    # Conectividad y Migración
    df_master = pd.merge(df_master, df_con.groupby('muni_id_join')[['Indice_Conectividad', 'Vehiculos_Oficial']].last().reset_index(), on='muni_id_join', how='left')
    df_master = pd.merge(df_master, df_osm.drop_duplicates('muni_id_join'), on='muni_id_join', how='left')
    df_master = pd.merge(df_master, df_mig.groupby('muni_id_join')['migracion_total'].last().reset_index(), on='muni_id_join', how='left')
    df_master['tasa_migratoria_pct'] = (df_master['migracion_total'] / (df_master['Total'] + 1)) * 100

    df_master['mean_distance_km_to_station'] = df_master['mean_distance_km_to_station'].fillna(50.0)

    # 🛑 FILTRO DE IDENTIDAD (Sincronizado con el Notebook)
    # He quitado las variables que "ensucian" (Total, pob_his, etc.) 
    # para dejar solo las 16 que hacían que el Notebook funcionara.
    cols_identidad = [
        'muni_id_join', 'nombre_municipio', 
        'pob_absoluta_actual', 'delta_pob_pct', 'ratio_masculinidad',
        'luz_absoluta_actual', 'delta_luz_pct', 'luz_volatilidad_std',
        'pib_act', 'pib_std', 
        'emp_act', 'delta_emp_pct', 
        'Indice_Conectividad', 'Vehiculos_Oficial',
        'stations_density_km2', 'mean_distance_km_to_station', # La variable conflictiva
        'tasa_migratoria_pct'
    ]
    
    df_master = df_master[cols_identidad].fillna(0)
    print(f"✅ Matriz Maestra Final: {len(df_master)} filas x {len(df_master.columns)} columnas.")
    return df_master

def trozo_2_machine_learning(df_master):
    print("\n🧠 INICIANDO PIPELINE DE MACHINE LEARNING (Modelo Híbrido)...")
    UMBRAL_POBLACION = 48000
    
    # 1. Separación de Mundos
    df_outliers = df_master[df_master['pob_absoluta_actual'] > UMBRAL_POBLACION].copy()
    df_rural = df_master[df_master['pob_absoluta_actual'] <= UMBRAL_POBLACION].copy()
    
    # 2. Selección de Features y Escalado
    X_rural = df_rural.select_dtypes(include=[np.number])
    cols_drop = ['year', 'lau_id', 'muni_id_join', 'cluster_raw']
    X_rural = X_rural.drop(columns=[c for c in cols_drop if c in X_rural.columns], errors='ignore')
    features = X_rural.columns.tolist()

    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X_rural)
    pca = PCA(n_components=10, random_state=42)
    X_pca = pca.fit_transform(X_scaled)

    # 3. K-Means (K=6)
    
    # 3. K-Means (K=6)
    kmeans = KMeans(n_clusters=6, random_state=42, n_init=10)
    df_rural['cluster_raw'] = kmeans.fit_predict(X_pca)

    # --- AÑADIDO: Calcular Silhouette y Guardar en JSON ---
    score_sil = silhouette_score(X_pca, df_rural['cluster_raw'])
    print(f"   📊 Silhouette Score del Clustering: {score_sil:.3f}")
    actualizar_estado_modelo("Segmentación Territorial (K-Means)", f"{score_sil:.2f}", "Silhouette Score")
# ---------------------------------------------------------
    # 4. 🔄 MAPEADO ESTRATÉGICO (Identificación por ADN)
    # ---------------------------------------------------------
    # Calculamos estadísticas para identificar al "monstruo" logístico
    stats_clusters = df_rural.groupby('cluster_raw').agg({
        'pob_absoluta_actual': 'mean',
        'delta_emp_pct': 'mean'
    })

    # A. Identificamos el clúster "Amazon" (el que tenga el crecimiento de empresas más bestia)
    cluster_amazon = stats_clusters['delta_emp_pct'].idxmax()
    
    # B. Los otros 5 clústeres los ordenamos por población para la escala de colores
    otros_clusters = stats_clusters.drop(cluster_amazon).sort_values('pob_absoluta_actual').index.tolist()
    
    # C. Creamos el mapeo final asegurando la jerarquía visual
    mapeo_final = {}
    
    # Los 4 primeros por población (Del rojo al amarillo)
    mapeo_final[otros_clusters[0]] = '1 - Despoblación Grave (Riesgo Crítico)'
    mapeo_final[otros_clusters[1]] = '2 - Pérdida Moderada (Rural en Retroceso)'
    mapeo_final[otros_clusters[2]] = '3 - Estancamiento Rural (Declive Suave)'
    mapeo_final[otros_clusters[3]] = '4 - Población Estable (Núcleos Tradicionales)'
    
    # El más poblado de los rurales (Azul claro)
    mapeo_final[otros_clusters[4]] = '5 - Fuerte Crecimiento (Zonas de Expansión)'
    
    # 👇 CAMBIAR AQUÍ EL 7 👇
    mapeo_final[cluster_amazon] = '7 - Enormes Centros (Motores Regionales)'

    df_rural['Perfil_Final'] = df_rural['cluster_raw'].map(mapeo_final)
    
    # 5. REUNIFICACIÓN CON GIGANTES
    # 👇 CAMBIAR AQUÍ EL 6 👇
    df_outliers['Perfil_Final'] = '6 - Grandes Ciudades (Municipios Aislados)'
    
    df_final = pd.concat([df_rural, df_outliers], ignore_index=True)
    # ---------------------------------------------------------
    # 📊 RADIOGRAFÍA TOTAL (Los 8.131 Municipios)
    # ---------------------------------------------------------
    print("\n" + "="*80)
    print("📊 RADIOGRAFÍA FINAL DE LAS 7 ESPAÑAS (Informe GeoLúmica)")
    print("="*80)
    
    # Agrupamos por Perfil_Final para ver las medias de TODO el país
    informe = df_final.groupby('Perfil_Final').agg({
        'pob_absoluta_actual': 'mean',
        'delta_pob_pct': 'mean',
        'ratio_masculinidad': 'mean',
        'pib_act': 'mean',
        'delta_emp_pct': 'mean',
        'muni_id_join': 'count' # Esto nos da el n_municipios
    }).rename(columns={'muni_id_join': 'n_municipios'}).round(2)
    
    print(informe)
    print("="*80)

    # 6. Guardar auditoría completa
    informe.to_csv(os.path.join(FIGURES_DIR, "auditoria_final_perfiles.csv"))
    
    return df_final

import geopandas as gpd
from matplotlib.lines import Line2D

def generar_mapa_final_automatico(df, colores):
    print("\n🗺️ Generando visualización cartográfica FINAL...")
    try:
        gdf = gpd.read_file(GEOJSON_PATH)
        gdf['LAU_ID'] = gdf['LAU_ID'].astype(str).str.zfill(5)
        
        # Merge de datos con los 8.131 municipios
        gdf_mapa = gdf.merge(df, left_on='LAU_ID', right_on='muni_id_join', how='left')

        fig, ax = plt.subplots(1, 1, figsize=(20, 15), facecolor='white')
        
        # Fondo gris para municipios sin datos
        gdf.plot(ax=ax, color='#f5f5f5', edgecolor='#d0d0d0', linewidth=0.1)

        # Capa GeoLúmica con los colores RdYlBu
        gdf_mapa.plot(
            ax=ax, 
            color=[colores.get(p, '#f5f5f5') for p in gdf_mapa['Perfil_Final']],
            edgecolor='black', 
            linewidth=0.04
        )

        # Leyenda
        legend_elements = [Line2D([0], [0], marker='s', color='w', label=k,
                          markerfacecolor=v, markersize=15) for k, v in colores.items()]
        ax.legend(handles=legend_elements, loc='lower right', title="Estratigrafía Territorial", 
                  fontsize=10, frameon=True, shadow=True)

        ax.set_title("GeoLúmica 2026: Diagnóstico de la Identidad Municipal", fontsize=22, fontweight='bold', pad=20)
        ax.axis('off')

        output = os.path.join(FIGURES_DIR, "MAPA_PROYECTO_FINAL_V2.png")
        plt.savefig(output, dpi=300, bbox_inches='tight')
        print(f"✅ ¡LOGRADO! Mapa profesional guardado en: {output}")
        
    except Exception as e:
        print(f"❌ Error crítico en la cartografía: {e}")

# --- BLOQUE DE EJECUCIÓN (MAIN) ---
if __name__ == "__main__":
    matriz_maestra = trozo_1_matriz_maestra()
    dataset_clasificado = trozo_2_machine_learning(matriz_maestra)
    
    # 🛑 ESTO ES LO QUE FALLABA: Ahora pasamos los dos argumentos
    generar_mapa_final_automatico(dataset_clasificado, COLORES_GEOLUMICA)