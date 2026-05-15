// components/dashboard/PredictiveDashboard.jsx
import React, { useState, useEffect, useCallback } from 'react';
import { ResponsiveContainer, BarChart, Bar, XAxis, YAxis, Tooltip as RechartsTooltip, Cell } from 'recharts';
import MunicipalitySearch from './modules/MunicipalitySearch';
import KpiCards from './modules/KpiCards';
import PolicySimulator from './modules/PolicySimulator';
import ImpactChart from './modules/ImpactChart';
import ClusterMap from './modules/ClusterMap';

export default function PredictiveDashboard() {
  const [loading, setLoading] = useState(false);
  
  const [municipioActual, setMunicipioActual] = useState({
    lau_id: "02081", 
    nombre: "Villarrobledo"
  });

  const [valoresSimulacion, setValoresSimulacion] = useState({
    inversionTransporte: 0, 
    estimuloEmpresas: 0, 
    migracion_pct: 0, 
    pib_estimulo_pct: 0
  });

  const [resultados, setResultados] = useState({
    poblacion5y: 0,
    variacionAbsoluta: 0,
    dataGrafica: [],
    segmento: "RURAL",
    perfil_estrategico: "Cargando...",
    driver_critico: "Calculando...",
    color_cluster: "#94a3b8",
    top_drivers: [] 
  });

  const [allClusters, setAllClusters] = useState({});

  useEffect(() => {
    fetch('http://127.0.0.1:8000/clusters/all')
      .then(res => res.json())
      .then(data => setAllClusters(data))
      .catch(err => console.error("Error al cargar clusters:", err));
  }, []);

  const getColorForProfile = useCallback((perfil) => {
    if (!perfil) return "#94a3b8"; 
    if (perfil.startsWith("1 -")) return "#d73027"; 
    if (perfil.startsWith("2 -")) return "#f46d43"; 
    if (perfil.startsWith("3 -")) return "#fdae61"; 
    if (perfil.startsWith("4 -")) return "#fee090"; 
    if (perfil.startsWith("5 -")) return "#abd9e9"; 
    if (perfil.startsWith("6 -")) return "#74add1"; 
    if (perfil.startsWith("7 -")) return "#4575b4"; 
    return "#94a3b8";
  }, []);

  const ejecutarSimulacion = useCallback(async (id, vals) => {
    setLoading(true);
    try {
      const res = await fetch('http://127.0.0.1:8000/simulate', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          lau_id: id,
          inversion_conectividad_pct: vals.inversionTransporte,
          estimulo_empresas_pct: vals.estimuloEmpresas,
          migracion_pct: vals.migracion_pct,
          pib_estimulo_pct: vals.pib_estimulo_pct
        })
      });

      if (!res.ok) throw new Error("Error en el motor ML");
      const data = await res.json();

      const seriesFusionada = data.evolucion_pob.map((p, i) => ({
        year: p.year, 
        pob: p.valor, 
        luz: data.evolucion_luz[i]?.valor || 0
      }));

      setResultados({
        poblacion5y: data.poblacion_proyectada,
        variacionAbsoluta: data.poblacion_proyectada - data.poblacion_base,
        dataGrafica: seriesFusionada,
        segmento: data.segmento,
        perfil_estrategico: data.perfil_estrategico,
        driver_critico: data.driver_critico,
        color_cluster: getColorForProfile(data.perfil_estrategico),
        top_drivers: data.top_drivers || []
      });
    } catch (err) {
      console.error("Error en simulación:", err);
    } finally {
      setLoading(false);
    }
  }, [getColorForProfile]);

  useEffect(() => {
    const timer = setTimeout(() => ejecutarSimulacion(municipioActual.lau_id, valoresSimulacion), 300);
    return () => clearTimeout(timer);
  }, [valoresSimulacion, municipioActual.lau_id, ejecutarSimulacion]);

  const handleSliderChange = (field, val) => {
    setValoresSimulacion(prev => ({ ...prev, [field]: val }));
  };

  const handleSelectMunicipio = (m) => {
    setMunicipioActual({ 
      lau_id: m.LAU_ID || m.muni_key, 
      nombre: m.muni_display
    });
    setValoresSimulacion({ inversionTransporte: 0, estimuloEmpresas: 0, migracion_pct: 0, pib_estimulo_pct: 0 });
  };

  return (
    <div className="h-full w-full bg-slate-50 flex flex-col overflow-hidden text-slate-900 font-sans relative">
      <header className="bg-white border-b border-slate-100 px-4 py-1.5 flex items-center justify-between shrink-0 shadow-sm z-20">
        <div className="flex flex-col">
          <h1 className="text-lg font-black italic tracking-tighter leading-none text-indigo-900">GEOLÚMICA</h1>
          <span className="text-[6px] font-bold text-indigo-500 uppercase tracking-[0.4em]">Predictive Engine</span>
        </div>
        <MunicipalitySearch onSelect={handleSelectMunicipio} />
      </header>

      <main className="flex-1 p-3 flex flex-col gap-3 min-h-0 overflow-hidden">
        <div className="shrink-0">
          <KpiCards datos={resultados} />
        </div>

        <div className="flex-1 grid grid-cols-12 gap-4 min-h-0 overflow-hidden">
          
          <div className="col-span-5 flex flex-col gap-3 min-h-0 overflow-hidden">
            <div className="flex-1 bg-white rounded-[1.5rem] border border-slate-200 p-1 flex flex-col items-center justify-center relative shadow-sm overflow-hidden">
              <ClusterMap 
                municipioActual={municipioActual} 
                allClusters={allClusters} 
              />
            </div>

            {/* NUEVA GRÁFICA DE BARRAS RECHARTS: TOP DRIVERS */}
            {resultados.top_drivers && resultados.top_drivers.length > 0 && (
              <div className="bg-white rounded-[1.5rem] p-4 shadow-sm border border-slate-200 shrink-0 h-44 flex flex-col relative overflow-hidden">
                <div className="flex justify-between items-center mb-2">
                  <h3 className="text-[10px] font-black uppercase tracking-[0.2em] text-slate-400">Radiografía: Variables de Impacto</h3>
                </div>
                <div className="flex-1 w-full min-h-0">
                  <ResponsiveContainer width="100%" height="100%">
                    <BarChart 
                      data={resultados.top_drivers} 
                      layout="vertical" 
                      margin={{ top: 0, right: 30, left: -20, bottom: 0 }}
                    >
                      <XAxis type="number" hide />
                      <YAxis 
                        dataKey="nombre" 
                        type="category" 
                        width={120} 
                        tick={{ fontSize: 9, fontWeight: 'bold', fill: '#64748b' }} 
                        axisLine={false} 
                        tickLine={false} 
                      />
                      <RechartsTooltip 
                        cursor={{fill: '#f8fafc'}} 
                        contentStyle={{borderRadius: '8px', fontSize: '11px', fontWeight: 'bold', border: 'none', boxShadow: '0 4px 6px -1px rgb(0 0 0 / 0.1)'}} 
                        formatter={(value) => [`${value}%`, 'Peso en el Modelo']}
                      />
                      <Bar dataKey="peso" radius={[0, 4, 4, 0]} barSize={16}>
                        {resultados.top_drivers.map((entry, index) => (
                          <Cell key={`cell-${index}`} fill={index === 0 ? '#6366f1' : '#cbd5e1'} />
                        ))}
                      </Bar>
                    </BarChart>
                  </ResponsiveContainer>
                </div>
              </div>
            )}
          </div>

          <div className="col-span-7 flex flex-col gap-3 min-h-0 overflow-hidden">
            <div className="shrink-0 w-full">
              <PolicySimulator values={valoresSimulacion} onChange={handleSliderChange} />
            </div>
            <div className="flex-1 relative min-h-0">
              <div className="absolute inset-0">
                <ImpactChart data={resultados.dataGrafica} loading={loading} />
              </div>
            </div>
          </div>
        </div>
      </main>
    </div>
  );
}