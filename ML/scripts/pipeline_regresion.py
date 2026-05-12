import pandas as pd
import numpy as np
import os
import joblib
from sqlalchemy import create_engine
from sklearn.ensemble import RandomForestRegressor
from sklearn.model_selection import RandomizedSearchCV
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score

# --- CONFIGURACIÓN DE RUTAS ---
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
MODEL_DIR = os.path.abspath(os.path.join(BASE_DIR, "..", "models", "modelos_exportados"))
os.makedirs(MODEL_DIR, exist_ok=True)

def extraer_gold_regresion_pro():
    print("🏗️ Construyendo Nueva Matriz Gold GeoLúmica (Panel 2018-2023)...")
    engine = create_engine("mysql+pymysql://bd_rvm_gelumica:Rio45Abc@10.151.30.2:3306/bd_rvm_gelumica")
    
    # 1. ESQUELETO: 8131 municipios x 6 años
    df_geo = pd.read_sql("SELECT muni_key AS LAU_ID, muni_display FROM dim_geografia", engine).drop_duplicates('LAU_ID')
    años = pd.DataFrame({'Anio': [2018, 2019, 2020, 2021, 2022, 2023]})
    df_master = df_geo.assign(key=1).merge(años.assign(key=1), on='key').drop('key', axis=1)

    # 2. CARGA Y AGREGACIÓN DE LAS 8 DIMENSIONES
    def cargar(q, col, func='mean'):
        df = pd.read_sql(q, engine)
        return df.groupby(['muni_id_join', 'Anio'])[col].agg(func).reset_index().rename(columns={'muni_id_join': 'LAU_ID'})

    print("📥 Cargando y procesando las 8 dimensiones...")
    d_dem = cargar("SELECT muni_id_join, Anio, Total FROM fact_demografia", 'Total', 'sum')
    d_v   = cargar("SELECT muni_id_join, Anio, mean FROM fact_viirs", 'mean', 'mean')
    d_pib = cargar("SELECT muni_id_join, Anio, pib FROM fact_renta", 'pib', 'mean')
    d_emp = cargar("SELECT muni_id_join, Anio, num_empresas_transporte FROM fact_empresas", 'num_empresas_transporte', 'sum')
    d_con = cargar("SELECT muni_id_join, Anio, Indice_Conectividad FROM fact_conectividad", 'Indice_Conectividad', 'mean')
    d_mig = cargar("SELECT muni_id_join, Anio, migracion_total FROM fact_migracion", 'migracion_total', 'sum')
    d_osm = pd.read_sql("SELECT muni_id_join AS LAU_ID, stations_density_km2, mean_distance_km_to_station FROM fact_osm", engine).groupby('LAU_ID').mean().reset_index()

    # 3. MERGE PANEL (Left Join para no perder municipios)
    for df in [d_dem, d_v, d_pib, d_emp, d_con, d_mig]:
        df_master = df_master.merge(df, on=['LAU_ID', 'Anio'], how='left')
    df_master = df_master.merge(d_osm, on='LAU_ID', how='left')

    # Renombrar para el modelo
    df_master.rename(columns={'Total':'pob', 'mean':'luz', 'num_empresas_transporte':'emp', 'Indice_Conectividad':'con', 'migracion_total':'mig'}, inplace=True)

    # 4. DINÁMICA TEMPORAL (t -> t+1)
    df_master = df_master.sort_values(['LAU_ID', 'Anio'])
    df_master['next_pob'] = df_master.groupby('LAU_ID')['pob'].shift(-1)
    df_master['next_luz'] = df_master.groupby('LAU_ID')['luz'].shift(-1)
    
    df_master['delta_pob'] = (df_master['next_pob'] - df_master['pob']) / df_master['pob']
    df_master['delta_luz'] = (df_master['next_luz'] - df_master['luz']) / df_master['luz']
    df_master['eficiencia_luz_pib'] = np.where(df_master['pib'] > 0, df_master['luz'] / df_master['pib'], 2.1)

    df_master = df_master[df_master['Anio'] < 2023].replace([np.inf, -np.inf], 0).fillna(0)
    print(f"✅ Matriz Gold lista: {len(df_master)} registros.")
    return df_master

def optimizar_y_entrenar(X, y, nombre):
    print(f"\n🔎 [Tuning] {nombre}...")
    param_dist = {'n_estimators': [100, 200], 'max_depth': [10, 20, None], 'min_samples_split': [2, 5], 'bootstrap': [True]}
    rf = RandomForestRegressor(random_state=42)
    search = RandomizedSearchCV(rf, param_dist, n_iter=7, cv=3, n_jobs=-1, scoring='r2', random_state=42)
    search.fit(X, y)
    print(f"   🏆 Mejores Parámetros: {search.best_params_}")
    return search.best_estimator_

def evaluar(modelo, X, base_t, actual_t1, nombre, es_pob=True):
    pred_delta = modelo.predict(X)
    pred_abs = base_t * (1 + pred_delta)
    mae = mean_absolute_error(actual_t1, pred_abs)
    rmse = np.sqrt(mean_squared_error(actual_t1, pred_abs))
    r2 = r2_score(actual_t1, pred_abs)
    u = "hab" if es_pob else "lumens"
    print(f"   📊 Rendimiento {nombre}: MAE: {mae:.2f} {u} | RMSE: {rmse:.2f} | R²: {r2:.4f}")

if __name__ == '__main__':
    df = extraer_gold_regresion_pro()
    cols_X = ['pob', 'luz', 'pib', 'emp', 'con', 'mig', 'eficiencia_luz_pib', 'stations_density_km2', 'mean_distance_km_to_station']

    for seg, name, crit in [(df[df['pob'] <= 50000], "RURAL", "Rural"), (df[df['pob'] > 50000], "URBANO", "Urbana")]:
        if seg.empty: continue
        print(f"\n--- ENTRENANDO SEGMENTO {name} ---")
        X = seg[cols_X].copy()
        X['pob'] = np.log1p(X['pob'])
        X['luz'] = np.log1p(X['luz'])

        # Población
        m_pob = optimizar_y_entrenar(X, seg['delta_pob'], f"Población {crit}")
        evaluar(m_pob, X, seg['pob'], seg['next_pob'], f"Población {crit}")
        joblib.dump(m_pob, os.path.join(MODEL_DIR, f"motor_población_españa_{crit.lower()}.pkl"))

        # Luz/Economía
        m_luz = optimizar_y_entrenar(X, seg['delta_luz'], f"Economía {crit}")
        evaluar(m_luz, X, seg['luz'], seg['next_luz'], f"Economía {crit}", False)
        joblib.dump(m_luz, os.path.join(MODEL_DIR, f"motor_economía_luz_españa_{crit.lower()}.pkl"))

    print("\n🚀 ¡Hecho! Todos los motores exportados con métricas validadas.")