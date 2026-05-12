import pandas as pd
import numpy as np
import os
import joblib
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from sqlalchemy import create_engine

# --- CONFIGURACIÓN ---
app = FastAPI(title="GeoLúmica API - Motor de Simulación Táctica")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
# Ruta relativa a los modelos (ML/models/modelos_exportados)
MODEL_DIR = os.path.abspath(os.path.join(BASE_DIR, "models", "modelos_exportados"))

# --- 1. CARGA DE MOTORES (PKL) ---
try:
    motores = {
        "pob_rural": joblib.load(os.path.join(MODEL_DIR, "motor_población_españa_rural.pkl")),
        "pob_urbano": joblib.load(os.path.join(MODEL_DIR, "motor_población_españa_urbana.pkl")),
        "luz_rural": joblib.load(os.path.join(MODEL_DIR, "motor_economía_luz_españa_rural.pkl")),
        "luz_urbano": joblib.load(os.path.join(MODEL_DIR, "motor_economía_luz_españa_urbana.pkl")),
    }
    print("✅ Motores de Regresión cargados y listos.")
except Exception as e:
    print(f"⚠️ Alerta: No se pudieron cargar los modelos. Ejecuta primero el entrenamiento. Error: {e}")

# --- 2. CARGA DE DATOS MAESTROS (Snapshot 2023) ---
# --- 2. CARGA DE DATOS MAESTROS (Snapshot 2023 con limpieza de duplicados) ---
try:
    engine = create_engine("mysql+pymysql://bd_rvm_gelumica:Rio45Abc@10.151.30.2:3306/bd_rvm_gelumica")
    
    # Cargamos y colapsamos cada dimensión para que solo haya 1 fila por municipio
    # Así evitamos que Madrid salga duplicado en el buscador
    d_dem = pd.read_sql("SELECT muni_id_join AS LAU_ID, Total AS pob FROM fact_demografia WHERE Anio = 2023", engine)
    d_dem = d_dem.groupby('LAU_ID')['pob'].sum().reset_index()

    d_v = pd.read_sql("SELECT muni_id_join AS LAU_ID, mean AS luz FROM fact_viirs WHERE Anio = 2023", engine)
    d_v = d_v.groupby('LAU_ID')['luz'].mean().reset_index()

    d_pib = pd.read_sql("SELECT muni_id_join AS LAU_ID, pib FROM fact_renta WHERE Anio = 2023", engine)
    d_pib = d_pib.groupby('LAU_ID')['pib'].mean().reset_index()

    # ... repetimos con el resto (empresas, conectividad, migración)
    d_emp = pd.read_sql("SELECT muni_id_join AS LAU_ID, num_empresas_transporte AS emp FROM fact_empresas WHERE Anio = 2023", engine)
    d_emp = d_emp.groupby('LAU_ID')['emp'].sum().reset_index()

    d_con = pd.read_sql("SELECT muni_id_join AS LAU_ID, Indice_Conectividad AS con FROM fact_conectividad WHERE Anio = 2023", engine)
    d_con = d_con.groupby('LAU_ID')['con'].mean().reset_index()

    d_mig = pd.read_sql("SELECT muni_id_join AS LAU_ID, migracion_total AS mig FROM fact_migracion WHERE Anio = 2023", engine)
    d_mig = d_mig.groupby('LAU_ID')['mig'].sum().reset_index()

    # OSM e información geográfica
    df_geo = pd.read_sql("SELECT muni_key AS LAU_ID, muni_display FROM dim_geografia", engine).drop_duplicates('LAU_ID')
    d_osm = pd.read_sql("SELECT muni_id_join AS LAU_ID, stations_density_km2, mean_distance_km_to_station FROM fact_osm", engine).groupby('LAU_ID').mean().reset_index()

    # Unimos todo en la Matriz Maestra ÚNICA
    df_master = df_geo.merge(d_dem, on='LAU_ID', how='inner')
    for d in [d_v, d_pib, d_emp, d_con, d_mig, d_osm]:
        df_master = df_master.merge(d, on='LAU_ID', how='left')

    df_master.fillna(0, inplace=True)
    # Ahora sí: drop_duplicates final por seguridad
    df_master = df_master.drop_duplicates(subset=['LAU_ID'])
    print(f"✅ Dashboard sincronizado: {len(df_master)} municipios únicos.")
except Exception as e:
    print(f"❌ Error DB: {e}")

# --- 3. MODELOS DE DATOS ---
class SimulacionReq(BaseModel):
    lau_id: str
    inversion_conectividad_pct: float
    estimulo_empresas_pct: float
    migracion_pct: float
    pib_estimulo_pct: float

# --- 4. ENDPOINTS ---

@app.get("/search")
def buscar(q: str):
    mask = (df_master['muni_display'].str.contains(q, case=False, na=False)) | \
           (df_master['LAU_ID'].str.contains(q, case=False, na=False))
    res = df_master[mask].sort_values(by='pob', ascending=False).head(10)
    return {"resultados": res[['LAU_ID', 'muni_display']].to_dict(orient='records')}

@app.post("/simulate")
def simular(req: SimulacionReq):
    row_data = df_master[df_master['LAU_ID'] == req.lau_id]
    if row_data.empty: raise HTTPException(status_code=404, detail="Municipio no encontrado")
    
    muni = row_data.iloc[0].copy()
    pob_base = float(muni['pob'])
    luz_base = float(muni['luz'])

    # 1. Preparar vector X para el modelo (incluyendo el impacto de los sliders)
    # Recalculamos eficiencia si el PIB cambia por el slider
    pib_simulado = muni['pib'] * (1 + req.pib_estimulo_pct / 100)
    eficiencia_sim = (luz_base / pib_simulado) if pib_simulado > 0 else muni['eficiencia_luz_pib']

    X_input = pd.DataFrame([{
        'pob': pob_base,
        'luz': luz_base,
        'pib': pib_simulado,
        'emp': muni['emp'] * (1 + req.estimulo_empresas_pct / 100),
        'con': muni['con'] * (1 + req.inversion_conectividad_pct / 100),
        'mig': muni['mig'] * (1 + req.migracion_pct / 100),
        'eficiencia_luz_pib': eficiencia_sim,
        'stations_density_km2': muni['stations_density_km2'],
        'mean_distance_km_to_station': muni['mean_distance_km_to_station']
    }])

    # Transformación logarítmica (Obligatorio: misma que en el entreno)
    X_input['pob'] = np.log1p(X_input['pob'])
    X_input['luz'] = np.log1p(X_input['luz'])

    # 2. Selección de motor por segmento
    sufijo = "urbano" if pob_base > 50000 else "rural"
    
    try:
        # Predicción del Delta interanual (%)
        delta_pob = float(motores[f"pob_{sufijo}"].predict(X_input)[0])
        delta_luz = float(motores[f"luz_{sufijo}"].predict(X_input)[0])

        # Proyección Táctica a 7 años (2023 -> 2030)
        pob_2030 = int(pob_base * (1 + delta_pob * 7))
        luz_2030 = round(luz_base * (1 + delta_luz * 7), 2)
    except:
        pob_2030, luz_2030 = pob_base, luz_base

    # 3. Generar serie temporal para los gráficos de React
    años = list(range(2023, 2031))
    ev_pob, ev_luz = [], []
    for i, anio in enumerate(años):
        factor = i / 7
        ev_pob.append({"year": str(anio), "valor": int(pob_base + (pob_2030 - pob_base) * factor)})
        ev_luz.append({"year": str(anio), "valor": round(luz_base + (luz_2030 - luz_base) * factor, 2)})

    return {
        "muni_display": muni['muni_display'],
        "poblacion_base": int(pob_base),
        "poblacion_proyectada": pob_2030,
        "luz_proyectada": luz_2030,
        "evolucion_pob": ev_pob,
        "evolucion_luz": ev_luz,
        "segmento": sufijo.upper()
    }