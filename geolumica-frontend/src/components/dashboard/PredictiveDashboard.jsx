// components/dashboard/PredictiveDashboard.jsx
import React, { useState, useEffect, useCallback } from 'react';
import MunicipalitySearch from './modules/MunicipalitySearch';
import KpiCards from './modules/KpiCards';
import PolicySimulator from './modules/PolicySimulator';
import ImpactChart from './modules/ImpactChart';
import ClusterMap from './modules/ClusterMap';

export default function PredictiveDashboard() {
  const [loading, setLoading] = useState(false);
  
  // 1. Estado del municipio (Ahora solo necesita el ID y el Nombre, como en tu DB)
  const [municipioActual, setMunicipioActual] = useState({
    lau_id: "02081", 
    nombre: "Villarrobledo"
  });

  const [valoresSimulacion, setValoresSimulacion] = useState({
    inversionTransporte: 0, estimuloEmpresas: 0, migracion_pct: 0, pib_estimulo_pct: 0
  });

  const [resultados, setResultados] = useState({
    poblacion5y: 25400,
    variacionAbsoluta: 0,
    dataGrafica: [],
    segmento: "RURAL",
    perfil_estrategico: "2 - Zonas Rurales Estables (Modelo Inercial)",
    driver_critico: "Calculando Driver táctico...",
    color_cluster: "#fdae61" 
  });

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
      const res = await fetch('http://localhost:8000/simulate', {
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

      if (!res.ok) throw new Error("Error en la respuesta del motor ML");
      const data = await res.json();

      const seriesFusionada = data.evolucion_pob.map((p, i) => ({
        year: p.year, pob: p.valor, luz: data.evolucion_luz[i]?.valor || 0
      }));

      const perfilApi = data.perfil_estrategico || resultados.perfil_estrategico;

      setResultados(prev => ({
        ...prev,
        poblacion5y: data.poblacion_proyectada,
        variacionAbsoluta: data.poblacion_proyectada - data.poblacion_base,
        dataGrafica: seriesFusionada,
        segmento: data.segmento || prev.segmento,
        perfil_estrategico: perfilApi,
        driver_critico: data.driver_critico || "Conectividad (-14% Impacto)", 
        color_cluster: getColorForProfile(perfilApi) 
      }));
    } catch (err) {
      console.error("Error táctico:", err);
    } finally {
      setLoading(false);
    }
  }, [resultados.perfil_estrategico, getColorForProfile]);

  useEffect(() => {
    const timer = setTimeout(() => ejecutarSimulacion(municipioActual.lau_id, valoresSimulacion), 300);
    return () => clearTimeout(timer);
  }, [valoresSimulacion, municipioActual.lau_id, ejecutarSimulacion]);

  const handleSliderChange = (field, val) => {
    setValoresSimulacion(prev => ({ ...prev, [field]: val }));
  };

  const handleSelectMunicipio = (m) => {
    // Adiós al escudo anti-crash y las coordenadas manuales. 
    // Solo le pasamos el muni_key (que equivale al LAU_ID).
    setMunicipioActual({ 
      lau_id: m.muni_key, 
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
          <div className="col-span-5 flex flex-col min-h-0 overflow-hidden">
            <div className="flex-1 bg-white rounded-[1.5rem] border border-slate-200 p-1 flex flex-col items-center justify-center relative shadow-sm overflow-hidden">
              <ClusterMap 
                municipio={municipioActual} 
                perfil={resultados.perfil_estrategico} 
                color={resultados.color_cluster} 
              />
            </div>
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