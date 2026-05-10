from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import pandas as pd
import numpy as np
import joblib
import os
from sqlalchemy import create_engine

# --- 1. CONFIGURACIÓN ---
app = FastAPI(title="GeoLúmica API", description="Motor Predictivo y Búsqueda")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], 
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
MODEL_DIR = os.path.abspath(os.path.join(BASE_DIR, "models", "modelos_exportados"))

print("⏳ Arrancando Motores GeoLúmica y calculando ingeniería de datos...")

# A. Cargar el Modelo (.pkl)
try:
    motor_pob = joblib.load(os.path.join(MODEL_DIR, "motor_población_españa_rural.pkl")) 
    print("✅ Modelo Predictivo de Regresión Cargado.")
except Exception as e:
    print(f"⚠️ Aviso: No se pudo cargar el modelo. Error: {e}")

# B. Construcción de la Matriz Gold PRO en Memoria
try:
    # ⚠️ PON AQUÍ TU CONTRASEÑA REAL DE MARIADB
    DB_URL = "mysql+pymysql://bd_rvm_gelumica:Rio45Abc@10.151.30.2:3306/bd_rvm_gelumica"
    engine = create_engine(DB_URL)
    
    # Leemos las tablas tal como hacías en pipeline_regresion.py
    # Añadimos 'muni_display' a la primera consulta para la barra de búsqueda de React
    df_geo = pd.read_sql("SELECT muni_key AS muni_id_join, muni_display FROM dim_geografia", engine)
    df_dem = pd.read_sql("SELECT muni_id_join, Anio, Total FROM fact_demografia", engine)
    df_v   = pd.read_sql("SELECT muni_id_join, Anio, mean AS luz FROM fact_viirs", engine)
    df_pib = pd.read_sql("SELECT muni_id_join, Anio, pib FROM fact_renta", engine)
    df_emp = pd.read_sql("SELECT muni_id_join, Anio, num_empresas_transporte FROM fact_empresas", engine)
    df_con = pd.read_sql("SELECT muni_id_join, Anio, Indice_Conectividad FROM fact_conectividad", engine)
    df_mig = pd.read_sql("SELECT muni_id_join, Anio, migracion_total FROM fact_migracion", engine)
    df_osm = pd.read_sql("SELECT muni_id_join, stations_density_km2, mean_distance_km_to_station FROM fact_osm", engine)

    # Limpiamos IDs
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

    # --- LUZ Y OTRAS VARIABLES ---
    v_stats = df_v.groupby('muni_id_join')['luz'].agg(luz_act='last', luz_his='first').reset_index()
    df_master = df_master.merge(v_stats, on='muni_id_join', how='left')

    df_master = df_master.merge(df_pib.groupby('muni_id_join')['pib'].last().reset_index().rename(columns={'pib': 'pib_act'}), on='muni_id_join', how='left')
    df_master = df_master.merge(df_emp.groupby('muni_id_join')['num_empresas_transporte'].last(), on='muni_id_join', how='left')
    df_master = df_master.merge(df_con.groupby('muni_id_join')['Indice_Conectividad'].last(), on='muni_id_join', how='left')
    df_master = df_master.merge(df_mig.groupby('muni_id_join')['migracion_total'].last(), on='muni_id_join', how='left')
    df_master = df_master.merge(df_osm.drop_duplicates('muni_id_join'), on='muni_id_join', how='left')

    # Cálculos finales exigidos por el modelo
    df_master['eficiencia_luz_pib'] = df_master['pib_act'] / (df_master['luz_act'] + 0.1)
    df_master['emp_por_hab'] = df_master['num_empresas_transporte'] / (df_master['pob_2023'] + 1)
    df_master['mean_distance_km_to_station'] = df_master['mean_distance_km_to_station'].fillna(100.0)
    
    df_master = df_master.fillna(0)
    
    # Renombramos para estandarizar la API
    df_master.rename(columns={'muni_id_join': 'LAU_ID'}, inplace=True)
    
    print(f"✅ Matriz Gold PRO calculada y alojada en memoria: {len(df_master)} municipios listos.")

except Exception as e:
    print(f"⚠️ Error conectando a BD: {e}")
    df_master = None


# --- 3. ESTRUCTURAS DE DATOS ACTUALIZADAS ---
class SimulacionRequest(BaseModel):
    lau_id: str
    inversion_conectividad_pct: float 
    estimulo_empresas_pct: float
    migracion_pct: float      # <--- Faltaba esta
    pib_estimulo_pct: float   # <--- Y esta

# --- 4. ENDPOINT DE SIMULACIÓN CORREGIDO ---
@app.post("/simulate")
def simular_escenario(req: SimulacionRequest):
    if df_master is None or df_master.empty:
        raise HTTPException(status_code=500, detail="Matriz no cargada")

    muni_data = df_master[df_master['LAU_ID'] == req.lau_id]
    if muni_data.empty: 
        raise HTTPException(status_code=404, detail="Municipio no encontrado")
    
    row = muni_data.iloc[0].copy()
    pob_actual = int(row['pob_2023'])
    
    # --- APLICAR EL IMPACTO DE LOS SLIDERS ---
    # 1. Conectividad
    row['Indice_Conectividad'] *= (1 + req.inversion_conectividad_pct / 100)
    
    # 2. Empresas (Impacta en la ratio por habitante)
    row['emp_por_hab'] *= (1 + req.estimulo_empresas_pct / 100)
    
    # 3. Migración (Afecta directamente al stock de personas)
    # req.migracion_pct viene del slider (0-100). Lo escalamos para el modelo.
    row['migracion_total'] += (req.migracion_pct * 5) 
    
    # 4. PIB / Eficiencia (Impacta en la vitalidad económica)
    row['eficiencia_luz_pib'] *= (1 + req.pib_estimulo_pct / 100)

    # --- PREDICCIÓN REAL CON EL MODELO ---
    # Estas son las 8 columnas que definiste en pipeline_regresion.py
    cols = [
        'pob_2023', 'pob_2018', 'aceleracion_pob', 'eficiencia_luz_pib', 
        'emp_por_hab', 'Indice_Conectividad', 'mean_distance_km_to_station', 'migracion_total'
    ]
    
    input_df = pd.DataFrame([row])[cols]
    
    # IMPORTANTE: Aplicar logaritmos antes de predecir (como en el entrenamiento)
    input_df['pob_2023'] = np.log1p(input_df['pob_2023'])
    input_df['pob_2018'] = np.log1p(input_df['pob_2018'])
    
    try:
        # El modelo predice en escala logarítmica
        pred_log = motor_pob.predict(input_df)[0]
        # Convertimos de nuevo a habitantes reales
        pob_2030 = int(np.expm1(pred_log))
    except Exception as e:
        print(f"Error en predict: {e}")
        pob_2030 = pob_actual

    # Generar la serie temporal para la gráfica interactiva (2023 -> 2030)
    años = [2023, 2024, 2025, 2026, 2027, 2028, 2029, 2030]
    evolucion = []
    for i, año in enumerate(años):
        progreso = i / (len(años) - 1)
        # Interpolación simple para dibujar la curva en React
        valor = int(pob_actual + (pob_2030 - pob_actual) * progreso)
        evolucion.append({"year": str(año), "poblacion": valor})

    return {
        "lau_id": req.lau_id,
        "poblacion_base": pob_actual,
        "poblacion_proyectada_2030": pob_2030,
        "variacion_absoluta": pob_2030 - pob_actual,
        "evolucion": evolucion,
        "perfil_estrategico": str(row.get('Perfil_Final', 'Análisis en curso'))
    }


# api.py (Parte del endpoint /simulate)
@app.post("/simulate")
def simular_escenario(req: SimulacionRequest):
    muni_data = df_master[df_master['LAU_ID'] == req.lau_id]
    if muni_data.empty: raise HTTPException(status_code=404, detail="No encontrado")
    
    row = muni_data.iloc[0].copy()
    pob_actual = int(row['pob_2023'])
    
    # --- APLICAR TODOS LOS SLIDERS ---
    # Simulamos impacto en las variables del modelo
    row['Indice_Conectividad'] *= (1 + req.inversion_conectividad_pct / 100)
    row['emp_por_hab'] *= (1 + req.estimulo_empresas_pct / 100)
    row['migracion_total'] += (req.migracion_pct * 10) # Ajuste manual de flujo migratorio
    row['eficiencia_luz_pib'] *= (1 + req.pib_estimulo_pct / 100)

    # --- PREDICCIÓN REAL ---
    cols = ['pob_2023', 'pob_2018', 'aceleracion_pob', 'eficiencia_luz_pib', 'emp_por_hab', 'Indice_Conectividad', 'mean_distance_km_to_station', 'migracion_total']
    input_df = pd.DataFrame([row])[cols]
    
    # Logaritmos (Igual que en el entrenamiento)
    input_df['pob_2023'] = np.log1p(input_df['pob_2023'])
    input_df['pob_2018'] = np.log1p(input_df['pob_2018'])
    
    try:
        pred_log = motor_pob.predict(input_df)[0]
        pob_2030 = int(np.expm1(pred_log))
    except:
        pob_2030 = pob_actual # Fallback
    
    # --- GENERAR SERIE TEMPORAL (Evolución año a año) ---
    # Creamos una curva suave entre 2023 y 2030
    años = [2023, 2024, 2025, 2026, 2027, 2028, 2029, 2030]
    evolucion = []
    for i, año in enumerate(años):
        # Interpolación con un poco de "ruido" o curva para que sea realista
        peso = i / (len(años) - 1)
        valor = int(pob_actual + (pob_2030 - pob_actual) * peso)
        evolucion.append({"year": str(año), "poblacion": valor})

    return {
        "lau_id": req.lau_id,
        "poblacion_base": pob_actual,
        "poblacion_proyectada_2030": pob_2030,
        "variacion_absoluta": pob_2030 - pob_actual,
        "evolucion": evolucion, # ¡Esto es lo que pedías para la gráfica!
        "indice_vitalidad": round((pob_2030/pob_actual) * 100, 1)
    }