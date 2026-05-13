// components/dashboard/PredictiveDashboard.jsx
import React, { useState, useEffect, useCallback } from 'react';
import MunicipalitySearch from './modules/MunicipalitySearch';
import KpiCards from './modules/KpiCards';
import PolicySimulator from './modules/PolicySimulator';
import ImpactChart from './modules/ImpactChart';

export default function PredictiveDashboard() {
  const [loading, setLoading] = useState(false);
  
  const [municipioActual, setMunicipioActual] = useState({
    lau_id: "02081", nombre: "Villarrobledo"
  });

  const [valoresSimulacion, setValoresSimulacion] = useState({
    inversionTransporte: 0, estimuloEmpresas: 0, migracion_pct: 0, pib_estimulo_pct: 0
  });

  const [resultados, setResultados] = useState({
    poblacion5y: 25400, variacionAbsoluta: 0, dataGrafica: [], segmento: "RURAL"
  });

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

      setResultados({
        poblacion5y: data.poblacion_proyectada,
        variacionAbsoluta: data.poblacion_proyectada - data.poblacion_base,
        dataGrafica: seriesFusionada,
        segmento: data.segmento || "RURAL"
      });
    } catch (err) {
      console.error("Error táctico:", err);
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    const timer = setTimeout(() => ejecutarSimulacion(municipioActual.lau_id, valoresSimulacion), 300);
    return () => clearTimeout(timer);
  }, [valoresSimulacion, municipioActual.lau_id, ejecutarSimulacion]);

  const handleSliderChange = (field, val) => {
    setValoresSimulacion(prev => ({ ...prev, [field]: val }));
  };

  const handleSelectMunicipio = (m) => {
    setMunicipioActual({ lau_id: m.muni_key, nombre: m.muni_display });
    setValoresSimulacion({ inversionTransporte: 0, estimuloEmpresas: 0, migracion_pct: 0, pib_estimulo_pct: 0 });
  };

  return (
    /* AQUÍ ESTÁ EL FIX: 'h-full' asegura que no invada el espacio del ViewManager */
    <div className="h-full w-full bg-slate-50 flex flex-col overflow-hidden text-slate-900 font-sans relative">
      
      <header className="bg-white border-b border-slate-100 px-4 py-1.5 flex items-center justify-between shrink-0 shadow-sm z-20">
        <div className="flex flex-col">
          <h1 className="text-lg font-black italic tracking-tighter leading-none text-indigo-900">GEOLUMICA</h1>
          <span className="text-[6px] font-bold text-indigo-500 uppercase tracking-[0.4em]">Predictive Engine</span>
        </div>
        <MunicipalitySearch onSelect={handleSelectMunicipio} />
      </header>

      <main className="flex-1 p-3 flex flex-col gap-3 min-h-0 overflow-hidden">
        
        {/* FILA 1: KPIs */}
        <div className="shrink-0">
          <KpiCards datos={resultados} />
        </div>

        {/* FILA 2: MAPA IZQ / SLIDERS + GRÁFICA DER */}
        <div className="flex-1 grid grid-cols-12 gap-4 min-h-0 overflow-hidden">
          
          {/* LADO IZQUIERDO: MAPA */}
          <div className="col-span-5 flex flex-col min-h-0 overflow-hidden">
            <div className="flex-1 bg-slate-900 rounded-[1.5rem] border border-slate-800 p-4 flex flex-col items-center justify-center relative shadow-inner overflow-hidden">
              <div className="absolute top-4 left-4 flex items-center gap-2">
                <div className="w-1.5 h-1.5 bg-emerald-400 rounded-full animate-pulse shadow-[0_0_8px_rgba(52,211,153,0.8)]"></div>
                <span className="text-[8px] font-black text-slate-300 uppercase tracking-widest">Map Viewport</span>
              </div>
              <div className="flex flex-col items-center gap-3 opacity-30">
                 <div className="w-10 h-10 border border-dashed border-slate-500 rounded-full animate-spin-slow"></div>
                 <p className="text-[8px] font-bold text-slate-400 uppercase tracking-[0.3em] text-center px-4">
                   Espacio K-Means
                 </p>
              </div>
            </div>
          </div>

          {/* LADO DERECHO: SLIDERS + GRÁFICA */}
          <div className="col-span-7 flex flex-col gap-3 min-h-0 overflow-hidden">
            
            {/* SLIDERS (Ahora más reducidos y compactos) */}
            <div className="shrink-0 w-full">
              <PolicySimulator values={valoresSimulacion} onChange={handleSliderChange} />
            </div>
            
            {/* GRÁFICA (Absoluta para forzarla a respetar los límites) */}
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