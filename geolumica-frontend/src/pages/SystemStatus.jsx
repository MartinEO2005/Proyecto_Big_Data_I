// src/pages/SystemStatus.jsx
import React, { useState, useEffect } from 'react';
import { Database, BrainCircuit, ShieldCheck, Cpu, HardDrive, Network, Layers, BarChart2 } from 'lucide-react';
import NavBar from '../components/layout/NavBar'; // <-- IMPORTAMOS LA NAVBAR

export default function SystemStatus({ currentView, setView }) {
  const [healthData, setHealthData] = useState(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    fetch('http://127.0.0.1:8000/health')
      .then(res => res.json())
      .then(data => {
        setHealthData(data);
        setLoading(false);
      })
      .catch(err => {
        console.error("Error al conectar con el centro de diagnóstico:", err);
        setLoading(false);
      });
  }, []);

  const classificationReport = [
    { perfil: "1 - Despoblación Grave (Riesgo Crítico)", precision: 0.95, recall: 0.94, f1: 0.95, support: 365 },
    { perfil: "2 - Pérdida Moderada (Rural en Retroceso)", precision: 0.97, recall: 0.99, f1: 0.98, support: 342 },
    { perfil: "3 - Estancamiento Rural (Declive Suave)", precision: 0.97, recall: 0.98, f1: 0.97, support: 738 },
    { perfil: "4 - Población Estable (Núcleos Tradicionales)", precision: 0.95, recall: 0.87, f1: 0.90, support: 126 },
    { perfil: "5 - Fuerte Crecimiento (Zonas de Expansión)", precision: 0.82, recall: 0.78, f1: 0.80, support: 18 },
    { perfil: "6 - Grandes Ciudades (Municipios Aislados)", precision: 1.00, recall: 1.00, f1: 1.00, support: 31 },
    { perfil: "7 - Enormes Centros (Motores Regionales)", precision: 0.71, recall: 0.71, f1: 0.71, support: 7 },
  ];

  return (
    <div className="min-h-screen bg-slate-50 text-[#03132B] font-sans">
      
      {/* RENDERIZAMOS LA NAVBAR AQUÍ PARA NO PERDERLA NUNCA */}
      <NavBar currentView={currentView} setView={setView} />
      
      {/* CONTENEDOR PRINCIPAL DEL STATUS */}
      <div className="p-8">
        
        <header className="mb-8 border-b border-slate-200 pb-6">
          <div className="flex items-center gap-4">
            <div className="p-3 bg-[#03132B] text-[#efa748] rounded-2xl shadow-sm">
              <Cpu size={32} />
            </div>
            <div>
              <h1 className="text-2xl font-black uppercase tracking-wider">Control del sistema de motores ML</h1>
              <p className="text-xs font-bold text-slate-400 uppercase tracking-widest mt-1"> Diagnósticos en Tiempo Real de los ultimos resultados</p>
            </div>
          </div>
        </header>

        <div className="grid grid-cols-12 gap-6">
          {/* COLUMNA IZQUIERDA: CONTROL DE INGESTA */}
          <div className="col-span-12 lg:col-span-5 space-y-6">
            <div className="bg-white p-6 rounded-[2rem] border border-slate-200 shadow-sm">
              <h2 className="text-[11px] font-black uppercase tracking-[0.2em] text-slate-400 mb-4 flex items-center gap-2">
                <Database size={16} className="text-[#03132B]" /> Ingesta y Conectores de Datos
              </h2>
              <div className="space-y-3">
                <div className="flex justify-between items-center bg-slate-50 p-3 rounded-xl border border-slate-100">
                  <div className="flex items-center gap-3">
                    <div className="w-2.5 h-2.5 bg-emerald-500 rounded-full shadow-[0_0_6px_rgba(16,185,129,0.5)]"></div>
                    <span className="text-xs font-black">VIIRS (Radiometría de Luces)</span>
                  </div>
                  <span className="text-[9px] font-bold text-slate-500 bg-white px-2 py-1 rounded border border-slate-200 uppercase">Mensual</span>
                </div>
                <div className="flex justify-between items-center bg-slate-50 p-3 rounded-xl border border-slate-100">
                  <div className="flex items-center gap-3">
                    <div className="w-2.5 h-2.5 bg-emerald-500 rounded-full shadow-[0_0_6px_rgba(16,185,129,0.5)]"></div>
                    <span className="text-xs font-black">OpenStreetMap</span>
                  </div>
                  <span className="text-[9px] font-bold text-slate-500 bg-white px-2 py-1 rounded border border-slate-200 uppercase"> Snapshot Diario</span>
                </div>
                <div className="flex justify-between items-center bg-slate-50 p-3 rounded-xl border border-slate-100">
                  <div className="flex items-center gap-3">
                    <div className="w-2.5 h-2.5 bg-emerald-500 rounded-full shadow-[0_0_6px_rgba(16,185,129,0.5)]"></div>
                    <span className="text-xs font-black">Google Earth Engine</span>
                  </div>
                  <span className="text-[9px] font-bold text-slate-500 bg-white px-2 py-1 rounded border border-slate-200 uppercase">(API calls)</span>
                </div>
                <div className="flex justify-between items-center bg-slate-50 p-3 rounded-xl border border-slate-100">
                  <div className="flex items-center gap-3">
                    <div className="w-2.5 h-2.5 bg-amber-500 rounded-full shadow-[0_0_6px_rgba(245,158,11,0.5)]"></div>
                    <span className="text-xs font-black">INE (Censo Demográfico)</span>
                  </div>
                  <span className="text-[9px] font-bold text-slate-500 bg-white px-2 py-1 rounded border border-slate-200 uppercase">Anual/Mensual</span>
                </div>
              </div>
            </div>

            <div className="bg-white p-6 rounded-[2rem] border border-slate-200 shadow-sm">
              <h2 className="text-[11px] font-black uppercase tracking-[0.2em] text-slate-400 mb-4 flex items-center gap-2">
                <HardDrive size={16} className="text-[#03132B]" /> Estado del Motor Predictivo (`system_health`)
              </h2>
              {loading ? (
                <p className="text-xs font-bold text-slate-400 animate-pulse">Consultando logs de entrenamiento...</p>
              ) : healthData?.modelos ? (
                <div className="space-y-3">
                  {Object.entries(healthData.modelos).map(([modelo, datos]) => (
                    <div key={modelo} className="bg-[#03132B] p-3.5 rounded-xl border border-slate-800 flex flex-col gap-1 shadow-inner">
                      <div className="flex justify-between items-center">
                        <span className="text-xs font-black text-white">{modelo}</span>
                        <span className="text-[9px] font-bold text-slate-400 uppercase tracking-wider">{datos.fecha_entrenamiento.split(' ')[0]}</span>
                      </div>
                      <span className="text-[10px] font-black text-[#efa748] tracking-wider uppercase mt-1">
                        {datos.metrica}
                      </span>
                    </div>
                  ))}
                </div>
              ) : (
                <p className="text-xs font-bold text-amber-600">Aviso: El archivo JSON no se encontró o la API está offline.</p>
              )}
            </div>
          </div>

          {/* COLUMNA DERECHA: REPORTE MATRICIAL Y METODOLOGÍA */}
          <div className="col-span-12 lg:col-span-7 space-y-6">
            <div className="bg-white p-6 rounded-[2rem] border border-slate-200 shadow-sm flex flex-col">
              <div className="flex justify-between items-center mb-4">
                <h2 className="text-[11px] font-black uppercase tracking-[0.2em] text-slate-400 flex items-center gap-2">
                  <BarChart2 size={16} className="text-[#03132B]" /> Eficacia Algorítmica (Gradient Boosting)
                </h2>
                <div className="bg-emerald-500 text-white font-black text-[10px] px-3 py-1 rounded-full uppercase tracking-widest shadow-sm">
                  Global Accuracy: 96%
                </div>
              </div>

              <div className="overflow-x-auto">
                <table className="w-full text-left border-collapse">
                  <thead>
                    <tr className="border-b border-slate-200">
                      <th className="py-2.5 text-[10px] font-black uppercase tracking-wider text-slate-400">Perfil Territorial</th>
                      <th className="py-2.5 text-[10px] font-black uppercase tracking-wider text-slate-400 text-center">Precisión</th>
                      <th className="py-2.5 text-[10px] font-black uppercase tracking-wider text-slate-400 text-center">Recall</th>
                      <th className="py-2.5 text-[10px] font-black uppercase tracking-wider text-slate-400 text-center">F1-Score</th>
                    </tr>
                  </thead>
                  <tbody>
                    {classificationReport.map((row, idx) => (
                      <tr key={idx} className="border-b border-slate-100 hover:bg-slate-50 transition-colors">
                        <td className="py-2.5 text-[11px] font-black text-[#03132B]">{row.perfil}</td>
                        <td className="py-2.5 text-xs font-bold text-center text-slate-600">{(row.precision * 100).toFixed(0)}%</td>
                        <td className="py-2.5 text-xs font-bold text-center text-slate-600">{(row.recall * 100).toFixed(0)}%</td>
                        <td className="py-2.5 text-xs font-black text-center text-[#03132B]">{(row.f1).toFixed(2)}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </div>

            <div className="bg-white p-6 rounded-[2rem] border border-slate-200 shadow-sm space-y-4">
              <h2 className="text-[11px] font-black uppercase tracking-[0.2em] text-slate-400 flex items-center gap-2">
                <Network size={16} className="text-[#03132B]" /> Arquitectura del Pipeline
              </h2>
              <div className="grid grid-cols-1 md:grid-cols-2 gap-4 text-xs leading-relaxed">
                <div className="p-4 bg-slate-50 rounded-2xl border border-slate-100">
                  <div className="flex items-center gap-2 font-black mb-1 text-[#03132B]">
                    <Layers size={14} className="text-[#efa748]" />
                    <span>Reducción Dimensional & K-Means</span>
                  </div>
                  <p className="text-slate-500 font-medium">
                    Aislamos núcleos mayores a 48.000 hab. Reducción PCA a 10 componentes para mitigar multicolinealidad. K-Means configurado en un óptimo de K=6 clústeres respaldado por la cohesión del Silhouette Score de 0.188.
                  </p>
                </div>
                <div className="p-4 bg-slate-50 rounded-2xl border border-slate-100">
                  <div className="flex items-center gap-2 font-black mb-1 text-[#03132B]">
                    <ShieldCheck size={14} className="text-[#efa748]" />
                    <span>'Efecto Contagio' (Spatial Lags)</span>
                  </div>
                  <p className="text-slate-500 font-medium">
                    Integración de la librería Geopandas para detectar contigüidad espacial. El Gradient Boosting no lee los municipios aislados, sino que entiende su entorno vecinal (Spatial Lags) aumentando la exactitud al 96%.
                  </p>
                </div>
              </div>
            </div>
          </div>
        </div>
      </div>
      
    </div>
  );
}