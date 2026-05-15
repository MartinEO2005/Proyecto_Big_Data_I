import pandas as pd
import numpy as np
import os
import joblib
import json
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
MODEL_DIR = os.path.abspath(os.path.join(BASE_DIR, "models", "modelos_exportados"))

# --- 1. CARGA DE MOTORES (PKL Originales) ---
try:
    motores = {
        "pob_rural": joblib.load(os.path.join(MODEL_DIR, "motor_población_españa_rural.pkl")),
        "pob_urbano": joblib.load(os.path.join(MODEL_DIR, "motor_población_españa_urbana.pkl")),
        "luz_rural": joblib.load(os.path.join(MODEL_DIR, "motor_economía_luz_españa_rural.pkl")),
        "luz_urbano": joblib.load(os.path.join(MODEL_DIR, "motor_economía_luz_españa_urbana.pkl")),
        "clasificador": joblib.load(os.path.join(MODEL_DIR, "gradient_boosting_perfiles.pkl")),
        "mapping": joblib.load(os.path.join(MODEL_DIR, "mapping_perfiles.pkl"))
    }
    print("✅ Motores de Regresión y Clasificación listos.")
except Exception as e:
    print(f"⚠️ Error cargando modelos: {e}")

# --- Carga del mapa JSON para los colores e info ---
try:
    with open(os.path.join(MODEL_DIR, "geolumica_metadata.json"), 'r', encoding='utf-8') as f:
        meta = json.load(f)
    mapa_clusters = meta.get("municipios", {})
    colores_perfiles = meta.get("configuracion", {}).get("colores", {})
    stats_clusters = meta.get("estadisticas_perfiles", {})
    print(f"✅ Mapa coloreado cargado ({len(mapa_clusters)} municipios).")
except Exception as e:
    print(f"⚠️ Aviso: No se encontró geolumica_metadata.json. Mapa será gris.")
    mapa_clusters, colores_perfiles, stats_clusters = {}, {}, {}

# --- 2. CARGA DE DATOS MAESTROS (INTACTO) ---
try:
    engine = create_engine("mysql+pymysql://bd_rvm_gelumica:Rio45Abc@10.151.30.2:3306/bd_rvm_gelumica")
    
    d_dem = pd.read_sql("SELECT muni_id_join AS LAU_ID, Total AS pob FROM fact_demografia WHERE Anio = 2023", engine).groupby('LAU_ID')['pob'].sum().reset_index()
    d_v = pd.read_sql("SELECT muni_id_join AS LAU_ID, mean AS luz FROM fact_viirs WHERE Anio = 2023", engine).groupby('LAU_ID')['luz'].mean().reset_index()
    d_pib = pd.read_sql("SELECT muni_id_join AS LAU_ID, pib FROM fact_renta WHERE Anio = 2023", engine).groupby('LAU_ID')['pib'].mean().reset_index()
    d_emp = pd.read_sql("SELECT muni_id_join AS LAU_ID, num_empresas_transporte AS emp FROM fact_empresas WHERE Anio = 2023", engine).groupby('LAU_ID')['emp'].sum().reset_index()
    d_con = pd.read_sql("SELECT muni_id_join AS LAU_ID, Indice_Conectividad AS con FROM fact_conectividad WHERE Anio = 2023", engine).groupby('LAU_ID')['con'].mean().reset_index()
    d_mig = pd.read_sql("SELECT muni_id_join AS LAU_ID, migracion_total AS mig FROM fact_migracion WHERE Anio = 2023", engine).groupby('LAU_ID')['mig'].sum().reset_index()
    df_geo = pd.read_sql("SELECT muni_key AS LAU_ID, muni_display FROM dim_geografia", engine).drop_duplicates('LAU_ID')
    d_osm = pd.read_sql("SELECT muni_id_join AS LAU_ID, stations_density_km2, mean_distance_km_to_station FROM fact_osm", engine).groupby('LAU_ID').mean().reset_index()

    df_master = df_geo.merge(d_dem, on='LAU_ID', how='inner')
    for d in [d_v, d_pib, d_emp, d_con, d_mig, d_osm]:
        df_master = df_master.merge(d, on='LAU_ID', how='left')

    df_master.fillna(0, inplace=True)
    df_master.drop_duplicates(subset=['LAU_ID'], inplace=True)
    df_master['eficiencia_luz_pib'] = np.where(df_master['pib'] > 0, df_master['luz'] / df_master['pib'], 0)
    print(f"✅ Dashboard sincronizado: {len(df_master)} municipios únicos.")
except Exception as e:
    print(f"❌ Error DB: {e}")

class SimulacionReq(BaseModel):
    lau_id: str
    inversion_conectividad_pct: float
    estimulo_empresas_pct: float
    migracion_pct: float
    pib_estimulo_pct: float

@app.get("/clusters/all")
def get_all_clusters():
    return {k: {"perfil": v} for k, v in mapa_clusters.items()}

@app.get("/search")
def buscar(q: str):
    mask = (df_master['muni_display'].str.contains(q, case=False, na=False)) | \
           (df_master['LAU_ID'].str.contains(q, case=False, na=False))
    res = df_master[mask].sort_values(by='pob', ascending=False).head(10)
    
    # LA SOLUCIÓN: Devolvemos LAU_ID exactamente como lo lee tu React
    return {"resultados": res[['LAU_ID', 'muni_display']].to_dict(orient='records')}

@app.post("/simulate")
def simular(req: SimulacionReq):
    row_data = df_master[df_master['LAU_ID'] == req.lau_id]
    if row_data.empty: raise HTTPException(status_code=404, detail="Municipio no encontrado")
    
    muni = row_data.iloc[0].copy()
    pob_base = float(muni['pob'])
    luz_base = float(muni['luz'])

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

    X_input['pob'] = np.log1p(X_input['pob'])
    X_input['luz'] = np.log1p(X_input['luz'])

    sufijo = "urbano" if pob_base > 50000 else "rural"
    
    try:
        delta_pob = float(motores[f"pob_{sufijo}"].predict(X_input)[0])
        delta_luz = float(motores[f"luz_{sufijo}"].predict(X_input)[0])
        pob_2030 = int(pob_base * (1 + delta_pob * 7))
        luz_2030 = round(luz_base * (1 + delta_luz * 7), 2)
    except:
        pob_2030, luz_2030 = pob_base, luz_base

    años = list(range(2023, 2031))
    ev_pob, ev_luz = [], []
    for i, anio in enumerate(años):
        factor = i / 7
        ev_pob.append({"year": str(anio), "valor": int(pob_base + (pob_2030 - pob_base) * factor)})
        ev_luz.append({"year": str(anio), "valor": round(luz_base + (luz_2030 - luz_base) * factor, 2)})

    # --- EL FIX DEL DRIVER CRÍTICO ---
    perfil_final = mapa_clusters.get(req.lau_id, "Perfil Desconocido")
    driver_critico = "Calculando..."
    top_drivers = []
    
    try:
        clf = motores["clasificador"]
        modelo_final = clf.named_steps[list(clf.named_steps.keys())[-1]] if hasattr(clf, 'named_steps') else clf
        
        importancias = modelo_final.feature_importances_
        
        # Leemos los nombres de las variables DIRECTAMENTE DEL MODELO, no de nuestra entrada
        if hasattr(modelo_final, 'feature_names_in_'):
            features_reales = modelo_final.feature_names_in_
        elif hasattr(clf, 'feature_names_in_'):
            features_reales = clf.feature_names_in_
        else:
            features_reales = [f"Variable_{i}" for i in range(len(importancias))]
            
        idx_max = np.argmax(importancias)
        driver_critico = f"{features_reales[idx_max].replace('_', ' ').title()} ({round(importancias[idx_max]*100, 1)}%)"
        
        # Sacamos el Top 4 para la nueva gráfica
        indices_top = np.argsort(importancias)[::-1][:4]
        for i in indices_top:
            top_drivers.append({
                "nombre": features_reales[i].replace('_', ' ').title(),
                "peso": round(importancias[i]*100, 1)
            })
    except Exception as e:
        print(f"Error extrayendo drivers: {e}")

    return {
        "muni_display": muni['muni_display'],
        "poblacion_base": int(pob_base),
        "poblacion_proyectada": pob_2030,
        "luz_proyectada": luz_2030,
        "evolucion_pob": ev_pob,
        "evolucion_luz": ev_luz,
        "segmento": sufijo.upper(),
        "perfil_estrategico": perfil_final,
        "color_cluster": colores_perfiles.get(perfil_final, "#cbd5e1"),
        "driver_critico": driver_critico,
        "top_drivers": top_drivers
    }