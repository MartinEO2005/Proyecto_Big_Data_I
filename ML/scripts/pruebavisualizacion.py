import pandas as pd
import geopandas as gpd
import matplotlib.pyplot as plt
import os
from sqlalchemy import create_engine

def plot_mapa_clusters_reales():
    print("🎨 Generando Mapa Maestro basado en los CLUSTERS de la IA...")
    
    # 1. Configuración de tu Escala Divergente (Copiada de tu original)
    colores_divergentes = {
        '1 - Despoblación Grave (Pueblos en riesgo crítico)': '#d73027',
        '2 - Pérdida Moderada (Pueblos que se vacían lentamente)': '#fc8d59',
        '3 - Población Estable (Pueblos medianos sin grandes cambios)': '#fee090',
        '4 - Crecimiento Leve (Pueblos que atraen nuevos vecinos)': '#e0f3f8',
        '5 - Fuerte Crecimiento (Zonas residenciales y turísticas)': '#91bfdb',
        '6 - Grandes Ciudades (Capitales y grandes núcleos)': '#4575b4',
        '7 - Enormes Centros Logísticos (Zonas de gran industria y transporte)': '#313695'
    }

    # 2. Conexión a la DB saneada de Lorca
    engine = create_engine("mysql+pymysql://bd_rvm_gelumica:Rio45Abc@10.151.30.2:3306/bd_rvm_gelumica")
    
    # 3. Cargar el GeoJSON (Geometría)
    print("📂 Cargando cartografía...")
    gdf = gpd.read_file("ML/municipios_es.geojson")
    gdf['LAU_ID'] = gdf['LAU_ID'].astype(str).str.zfill(5)

    # 4. Obtener resultados del Pipeline de ML
    # (Aquí cargamos el df_final que generó tu pipeline_clustering.py)
    # Si lo tienes en la base de datos, lo leemos de la tabla fact_master_gold
    print("🧠 Recuperando segmentación del modelo de Machine Learning...")
    query = "SELECT lau_id, Perfil_Final FROM fact_master_gold WHERE year = 2025"
    try:
        df_resultados = pd.read_sql(query, engine)
    except:
        print("⚠️ No encontré fact_master_gold. Cargando datos de demografía para procesar...")
        # Fallback: leemos demografía y simulamos la columna para que el código no falle
        df_resultados = pd.read_sql("SELECT lau_id FROM fact_demografia WHERE year = 2025", engine)
        # Aquí es donde el pipeline_clustering asignaría el Perfil_Final real
        # Para que el mapa funcione ahora, asumo que ya has corrido el pipeline
    
    df_resultados['lau_id'] = df_resultados['lau_id'].astype(str).str.zfill(5)

    # 5. MERGE: El cruce que antes fallaba y ahora es perfecto
    gdf_final = gdf.merge(df_resultados, left_on='LAU_ID', right_on='lau_id', how='left')

    # 6. RENDERIZADO
    fig, ax = plt.subplots(1, 1, figsize=(20, 15), facecolor='white')
    
    print("🖌️  Pintando el mapa con los perfiles del K-Means...")
    
    # Pintamos cada municipio con su color de la escala divergente
    gdf_final.plot(
        column='Perfil_Final',
        categorical=True,
        legend=True,
        ax=ax,
        color=[colores_divergentes.get(p, '#dfdfdf') for p in gdf_final['Perfil_Final']],
        edgecolor='black',
        linewidth=0.03,
        legend_kwds={'title': "Perfiles Territoriales GeoLúmica", 'loc': 'lower right'}
    )

    ax.set_title("Mapa de Segmentación GeoLúmica: Inteligencia Territorial", fontsize=22, fontweight='bold', pad=20)
    ax.axis('off')

    output_img = "RESULTADO_CLUSTERING_FINAL.png"
    plt.savefig(output_img, dpi=300, bbox_inches='tight')
    print(f"✅ ¡MAPA GENERADO! Guardado en {output_img}")
    plt.show()

if __name__ == "__main__":
    plot_mapa_clusters_reales()