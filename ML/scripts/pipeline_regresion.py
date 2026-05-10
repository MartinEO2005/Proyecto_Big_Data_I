import pandas as pd
import numpy as np
import os
from sqlalchemy import create_engine

# --- CONFIGURACIÓN DE RUTAS BLINDADA ---
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
# Si tu script está en ML/scripts/, subimos un nivel a ML/ y buscamos /models/
MODEL_DIR = os.path.abspath(os.path.join(BASE_DIR, "..", "models", "modelos_exportados"))
FIGURES_DIR = os.path.abspath(os.path.join(BASE_DIR, "..", "models", "figuras"))

os.makedirs(MODEL_DIR, exist_ok=True)
os.makedirs(FIGURES_DIR, exist_ok=True)

def extraer_gold_regresion_pro():
    print("🔌 Extrayendo Matriz Gold con Ingeniería PRO (Lags & Aceleración)...")
    DB_URL = "mysql+pymysql://bd_rvm_gelumica:Rio45Abc@10.151.30.2:3306/bd_rvm_gelumica"
    engine = create_engine(DB_URL)
    
    df_geo = pd.read_sql("SELECT muni_key AS muni_id_join FROM dim_geografia", engine)
    df_dem = pd.read_sql("SELECT muni_id_join, Anio, Total FROM fact_demografia", engine)
    df_v   = pd.read_sql("SELECT muni_id_join, Anio, mean AS luz FROM fact_viirs", engine)
    df_pib = pd.read_sql("SELECT muni_id_join, Anio, pib FROM fact_renta", engine)
    df_emp = pd.read_sql("SELECT muni_id_join, Anio, num_empresas_transporte FROM fact_empresas", engine)
    df_con = pd.read_sql("SELECT muni_id_join, Anio, Indice_Conectividad FROM fact_conectividad", engine)
    df_mig = pd.read_sql("SELECT muni_id_join, Anio, migracion_total FROM fact_migracion", engine)
    df_osm = pd.read_sql("SELECT muni_id_join, stations_density_km2, mean_distance_km_to_station FROM fact_osm", engine)

    for df in [df_geo, df_dem, df_v, df_pib, df_emp, df_con, df_mig, df_osm]:
        df['muni_id_join'] = df['muni_id_join'].astype(str).str.split('.').str[0].str.zfill(5)

    df_master = df_geo.copy()

    # --- POBLACIÓN Y LAGS ---
    y_max, y_min = df_dem['Anio'].max(), df_dem['Anio'].min()
    y_mid = (y_max + y_min) // 2

    pob_actual = df_dem[df_dem['Anio'] == y_max].drop_duplicates('muni_id_join').rename(columns={'Total': 'pob_2023'})
    pob_mid = df_dem[df_dem['Anio'] == y_mid].drop_duplicates('muni_id_join')[['muni_id_join', 'Total']].rename(columns={'Total': 'pob_2018'})
    pob_his = df_dem[df_dem['Anio'] == y_min].drop_duplicates('muni_id_join')[['muni_id_join', 'Total']].rename(columns={'Total': 'pob_2013'})

    df_master = df_master.merge(pob_actual[['muni_id_join', 'pob_2023']], on='muni_id_join', how='left')
    df_master = df_master.merge(pob_mid, on='muni_id_join', how='left')
    df_master = df_master.merge(pob_his, on='muni_id_join', how='left')

    df_master['delta_reciente'] = (df_master['pob_2023'] - df_master['pob_2018']) / (df_master['pob_2018'] + 1)
    df_master['delta_antiguo'] = (df_master['pob_2018'] - df_master['pob_2013']) / (df_master['pob_2013'] + 1)
    df_master['aceleracion_pob'] = df_master['delta_reciente'] - df_master['delta_antiguo']

    # --- LUZ (FIX: Guardamos luz_his) ---
    v_stats = df_v.groupby('muni_id_join')['luz'].agg(luz_act='last', luz_his='first').reset_index()
    df_master = df_master.merge(v_stats, on='muni_id_join', how='left')

    # --- RESTO DE VARIABLES ---
    df_master = df_master.merge(df_pib.groupby('muni_id_join')['pib'].last().reset_index().rename(columns={'pib': 'pib_act'}), on='muni_id_join', how='left')
    df_master = df_master.merge(df_emp.groupby('muni_id_join')['num_empresas_transporte'].last(), on='muni_id_join', how='left')
    df_master = df_master.merge(df_con.groupby('muni_id_join')['Indice_Conectividad'].last(), on='muni_id_join', how='left')
    df_master = df_master.merge(df_mig.groupby('muni_id_join')['migracion_total'].last(), on='muni_id_join', how='left')
    df_master = df_master.merge(df_osm.drop_duplicates('muni_id_join'), on='muni_id_join', how='left')

    df_master['eficiencia_luz_pib'] = df_master['pib_act'] / (df_master['luz_act'] + 0.1)
    df_master['emp_por_hab'] = df_master['num_empresas_transporte'] / (df_master['pob_2023'] + 1)
    df_master['mean_distance_km_to_station'] = df_master['mean_distance_km_to_station'].fillna(100.0)
    
    print(f"✅ Matriz Gold construida con Lags y Luz histórica.")
    return df_master.fillna(0)
import joblib
import matplotlib.pyplot as plt
import seaborn as sns
from sklearn.ensemble import RandomForestRegressor
from sklearn.model_selection import RandomizedSearchCV, train_test_split
from sklearn.metrics import mean_absolute_error, r2_score, mean_squared_error

def entrenar_motores_geolumica(df):
    print("\n🧠 INICIANDO APRENDIZAJE REFINADO (Log-Scale & Ratios)...")
    
    # 1. Feature Engineering Proporcional (Relativizar para pueblos pequeños)
    df['emp_por_hab'] = df['emp_act'] / (df['pob_absoluta_actual'] + 1)
    df['luz_por_hab'] = df['luz_absoluta_actual'] / (df['pob_absoluta_actual'] + 1)
    
    # Lista de variables definitivas
    features = [
        'pob_absoluta_actual', 'ratio_masculinidad', 
        'luz_por_hab', 'luz_volatilidad_std',
        'pib_act', 'emp_por_hab', 'Indice_Conectividad', 
        'stations_density_km2', 'mean_distance_km_to_station',
        'tasa_migratoria_pct'
    ]
    
    # 2. Aplicamos Log-Transform a la Población (Entrada y Target)
    # np.log1p es log(1+x) para evitar errores con ceros
    X = df[features].copy()
    X['pob_absoluta_actual'] = np.log1p(X['pob_absoluta_actual'])
    
    y_pob_log = np.log1p(df['pob_absoluta_actual'] * (1 + (df['delta_pob_pct'] / 100)))
    y_luz_log = np.log1p(df['luz_absoluta_actual'] * (1 + (df['delta_luz_pct'] / 100)))

def entrenar_regresion_segmentada_pro(df):
    print("\n🚀 ENTRENANDO SISTEMA PREDICTIVO DUAL (Población + Economía)...")
    
    # 1. DEFINIMOS LAS VARIABLES AQUÍ (Ámbito global de la función)
    features = [
        'pob_2023', 'pob_2018', 'aceleracion_pob', 'eficiencia_luz_pib',
        'emp_por_hab', 'Indice_Conectividad', 'mean_distance_km_to_station',
        'migracion_total'
    ]

    # 2. Segmentación de datos
    df_rural = df[df['pob_2023'] <= 50000].copy()
    df_urbano = df[df['pob_2023'] > 50000].copy()

    def fit_especialista(data, target_col, nombre_segmento, nombre_target):
        print(f"\n--- 🧠 Entrenando {nombre_target} en {nombre_segmento} ---")
        
        X = data[features].copy()
        
        # Log-Transform para estabilizar la varianza
        X['pob_2023'] = np.log1p(X['pob_2023'])
        X['pob_2018'] = np.log1p(X['pob_2018'])
        
        # Definición del Target (Tendencia 2030)
        if nombre_target == "Población":
            y_real = data['pob_2023'] * (1 + data['delta_reciente'])
        else:
            delta_luz = (data['luz_act'] - data['luz_his']) / (data['luz_his'] + 0.1)
            y_real = data['luz_act'] * (1 + delta_luz)

        y_log = np.log1p(y_real)

        X_train, X_test, y_train, y_test = train_test_split(X, y_log, test_size=0.2, random_state=42)
        
        model = RandomForestRegressor(n_estimators=300, max_depth=15, random_state=42)
        model.fit(X_train, y_train)
        
        # Validación: Volvemos de escala Logarítmica a Real
        preds_log = model.predict(X_test)
        preds_real = np.expm1(preds_log)
        y_test_real = np.expm1(y_test)
        
        mae = mean_absolute_error(y_test_real, preds_real)
        mape = np.mean(np.abs((y_test_real - preds_real) / (y_test_real + 1))) * 100
        r2 = r2_score(y_test, preds_log)

        print(f"    📊 Métricas {nombre_target} ({nombre_segmento}):")
        print(f"      - R² Score: {r2:.4f}")
        print(f"      - MAE: {mae:.2f} {'hab' if nombre_target == 'Población' else 'lumens'}")
        print(f"      - MAPE: {mape:.2f}%")
        
        # Guardar modelo físico (.pkl)
        filename = f"motor_{nombre_target.lower()}_{nombre_segmento.lower().replace(' ', '_')}.pkl"
        joblib.dump(model, os.path.join(MODEL_DIR, filename))
        
        return model

    # 3. Ejecución de los 4 procesos especialistas
    m_pob_rural = fit_especialista(df_rural, 'pob_2023', "ESPAÑA RURAL", "Población")
    m_luz_rural = fit_especialista(df_rural, 'luz_act', "ESPAÑA RURAL", "Economía_Luz")
    
    m_pob_urbano = fit_especialista(df_urbano, 'pob_2023', "ESPAÑA URBANA", "Población")
    m_luz_urbano = fit_especialista(df_urbano, 'luz_act', "ESPAÑA URBANA", "Economía_Luz")

    # Devolvemos los features sin errores de ámbito
    return m_pob_rural, features

import plotly.graph_objects as go

def generar_simulador_interactivo_2030(model, df_rural, features):
    print("\n📊 Generando Simulador de Escenarios GeoLúmica...")
    
    # Tomamos el "Pueblo Media" de la España Rural
    base_val = df_rural[features].mean().to_frame().T
    
    # Rango de inversión (-50% a +100%)
    inversion = np.linspace(0.5, 2.0, 20)
    resultados = []

    for inv in inversion:
        sim = base_val.copy()
        # Simulamos que la inversión mejora la conectividad y las empresas
        sim['Indice_Conectividad'] *= inv
        sim['emp_por_hab'] *= inv
        
        # Aplicamos logs como en el entreno
        sim['pob_2023'] = np.log1p(sim['pob_2023'])
        sim['pob_2018'] = np.log1p(sim['pob_2018'])
        
        pred_log = model.predict(sim)[0]
        resultados.append(np.expm1(pred_log))

    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=(inversion - 1) * 100, 
        y=resultados,
        mode='lines+markers',
        line=dict(color='#d73027', width=4),
        hovertemplate="Inversión: %{x}%<br>Pob. Estimada: %{y:.0f} hab<extra></extra>"
    ))

    fig.update_layout(
        title="<b>Simulador de Políticas Públicas: Impacto en Población (2030)</b>",
        xaxis_title="Variación en Inversión/Conectividad (%)",
        yaxis_title="Población Proyectada (Hab)",
        template="plotly_white"
    )

    path = os.path.join(FIGURES_DIR, "simulador_escenarios_2030.html")
    fig.write_html(path)
    print(f"✅ Simulador guardado en: {path}")

if __name__ == "__main__":
    # 1. Extracción PRO (Lags + Aceleración)
    df_gold = extraer_gold_regresion_pro()
    
    # 2. Entrenamiento Dual Segmentado
    # Esta función ahora imprime los 4 modelos por terminal
    motor_pob_rural, features_list = entrenar_regresion_segmentada_pro(df_gold)
    
    # 3. Simulador interactivo (Usando el motor rural que es el foco del proyecto)
    df_solo_rural = df_gold[df_gold['pob_2023'] <= 50000]
    generar_simulador_interactivo_2030(motor_pob_rural, df_solo_rural, features_list)