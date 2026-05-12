// components/dashboard/PredictiveDashboard.jsx
import React, { useState, useEffect, useCallback } from 'react';
import MunicipalitySearch from './modules/MunicipalitySearch';
import KpiCards from './modules/KpiCards';
import PolicySimulator from './modules/PolicySimulator';
import ImpactChart from './modules/ImpactChart';

export default function PredictiveDashboard() {
  const [loading, setLoading] = useState(false);
  
  // 1. Estado del municipio seleccionado
  const [municipioActual, setMunicipioActual] = useState({
    lau_id: "28079", // Madrid por defecto
    nombre: "Madrid"
  });

  // 2. Estado de los sliders (Valores locales para fluidez total)
  const [valoresSimulacion, setValoresSimulacion] = useState({
    inversionTransporte: 0,
    estimuloEmpresas: 0,
    migracion_pct: 0,
    pib_estimulo_pct: 0
  });

  // 3. Estado de los resultados (Lo que devuelve la API)
  const [resultados, setResultados] = useState({
    poblacion5y: 0,
    variacionAbsoluta: 0,
    evolucion_pob: [],
    evolucion_luz: [],
    segmento: "URBANA"
  });

  // Función para llamar a la API de simulación
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

      if (!res.ok) throw new Error("Error en el servidor");
      const data = await res.json();

      setResultados({
        poblacion5y: data.poblacion_proyectada,
        variacionAbsoluta: data.poblacion_proyectada - data.poblacion_base,
        evolucion_pob: data.evolucion_pob || [],
        evolucion_luz: data.evolucion_luz || [],
        segmento: data.segmento || "URBANA"
      });
    } catch (err) {
      console.error("Fallo táctico en simulación:", err);
    } finally {
      setLoading(false);
    }
  }, []);

  // EFECTO DE DEBOUNCE: Para que los sliders no se bloqueen
  useEffect(() => {
    const timer = setTimeout(() => {
      ejecutarSimulacion(municipioActual.lau_id, valoresSimulacion);
    }, 300); // Espera 300ms tras el último movimiento del slider
    return () => clearTimeout(timer);
  }, [valoresSimulacion, municipioActual.lau_id, ejecutarSimulacion]);

  // Manejador de cambios en sliders (Actualización visual inmediata)
  const handleSliderChange = (field, val) => {
    setValoresSimulacion(prev => ({ ...prev, [field]: val }));
  };

  // Manejador de selección de municipio
  const handleSelectMunicipio = (m) => {
    setMunicipioActual({
      lau_id: m.muni_key,
      nombre: m.muni_display
    });
    // Reiniciamos sliders al cambiar de municipio para ver la base limpia
    setValoresSimulacion({
      inversionTransporte: 0,
      estimuloEmpresas: 0,
      migracion_pct: 0,
      pib_estimulo_pct: 0
    });
  };

  return (
    <div className="flex flex-col h-screen bg-slate-50 overflow-hidden">
      <header className="shrink-0 p-6 pb-0 flex justify-between items-center">
        <div className="space-y-1">
          <h1 className="text-2xl font-black text-slate-900 tracking-tight">GeoLúmica <span className="text-indigo-600">Predictiva</span></h1>
          <p className="text-xs font-bold text-slate-400 uppercase tracking-widest flex items-center gap-2">
            <span className="w-2 h-2 bg-emerald-500 rounded-full animate-pulse"></span>
            Simulación de Crecimiento Municipal: {municipioActual.nombre}
          </p>
        </div>
        
        <MunicipalitySearch onSelect={handleSelectMunicipio} />
      </header>

      <main className="flex-1 p-6 flex flex-col gap-6 min-h-0">
        <KpiCards datos={resultados} municipio={municipioActual} />

        <div className="flex-1 grid grid-cols-12 gap-6 min-h-0">
          {/* PANEL IZQUIERDO: Sliders */}
          <div className="col-span-4 flex flex-col gap-4">
            <PolicySimulator values={valoresSimulacion} onChange={handleSliderChange} />
            <div className="flex-1 bg-slate-200/40 rounded-[2rem] border-2 border-dashed border-slate-300 flex items-center justify-center">
              <p className="text-[10px] font-black text-slate-400 uppercase tracking-widest">Capa Geoespacial Pendiente</p>
            </div>
          </div>

          {/* PANEL DERECHO: Gráficas de Dinámica Temporal */}
          <div className="col-span-8 flex flex-col gap-4">
            <ImpactChart 
              title="Evolución Demográfica (Población)" 
              data={resultados.evolucion_pob} 
              color="#6366f1" 
              loading={loading} 
            />
            <ImpactChart 
              title="Evolución Económica (Luz Nocturna)" 
              data={resultados.evolucion_luz} 
              color="#f59e0b" 
              loading={loading} 
            />
          </div>
        </div>
      </main>
    </div>
  );
}