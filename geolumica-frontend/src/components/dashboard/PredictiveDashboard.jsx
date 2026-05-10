// src/components/dashboard/PredictiveDashboard.jsx
import React, { useState, useEffect } from 'react';
import MunicipalitySearch from './modules/MunicipalitySearch';
import KpiCards from './modules/KpiCards';
import PolicySimulator from './modules/PolicySimulator';
import ImpactChart from './modules/ImpactChart';
import { Map as MapIcon } from 'lucide-react';

export default function PredictiveDashboard() {
  const [loading, setLoading] = useState(false);
  
  // ESTADO INICIAL COMPLETO (Evita que la pantalla salga en blanco)
  const [municipioActual, setMunicipioActual] = useState({
    lau_id: "02081", 
    nombre: "Villarrobledo", 
    perfilEstrategico: "3 - Estancamiento Rural", 
    poblacionBase: 25400
  });

  const [simulacion, setSimulacion] = useState({
    inversionTransporte: 0,
    estimuloEmpresas: 0,
    migracion_pct: 0,
    pib_estimulo_pct: 0,
    poblacion5y: 25400,
    variacionAbsoluta: 0,
    evolucion: [
      { year: "2023", poblacion: 25400 },
      { year: "2030", poblacion: 25400 }
    ] // Importante: serie temporal inicial para que la gráfica no crashee
  });

  const buscarYSimular = async (lau_id, inv, est, mig, pib) => {
    setLoading(true);
    try {
      const response = await fetch('http://127.0.0.1:8000/simulate', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ 
          lau_id, 
          inversion_conectividad_pct: inv, 
          estimulo_empresas_pct: est,
          migracion_pct: mig,
          pib_estimulo_pct: pib
        }),
      });
      const data = await response.json();
      
      setSimulacion(prev => ({ 
        ...prev, 
        poblacion5y: data.poblacion_proyectada_2030, 
        variacionAbsoluta: data.variacion_absoluta,
        evolucion: data.evolucion,
        inversionTransporte: inv,
        estimuloEmpresas: est,
        migracion_pct: mig,
        pib_estimulo_pct: pib
      }));
      
      setMunicipioActual(prev => ({ 
        ...prev, 
        poblacionBase: data.poblacion_base, 
        perfilEstrategico: data.perfil_estrategico 
      }));
    } catch (err) { 
      console.error("Error en la API:", err); 
    } finally { 
      setLoading(false); 
    }
  };

  const handleSliderChange = (field, value) => {
    // Calculamos los nuevos valores antes de enviarlos
    const v = {
      inv: field === 'inversionTransporte' ? value : simulacion.inversionTransporte,
      est: field === 'estimuloEmpresas' ? value : simulacion.estimuloEmpresas,
      mig: field === 'migracion_pct' ? value : simulacion.migracion_pct,
      pib: field === 'pib_estimulo_pct' ? value : simulacion.pib_estimulo_pct
    };
    
    buscarYSimular(municipioActual.lau_id, v.inv, v.est, v.mig, v.pib);
  };

  useEffect(() => { 
    buscarYSimular(municipioActual.lau_id, 0, 0, 0, 0); 
  }, []);

  return (
    <div className="h-full p-8 flex flex-col gap-6 overflow-hidden bg-slate-50/50">
      <header className="flex justify-between items-end">
        <div>
          <h1 className="text-3xl font-bold text-slate-800 tracking-tight">GeoLúmica: <span className="text-indigo-600">{municipioActual.nombre}</span></h1>
          <p className="text-slate-500">Inteligencia Territorial • Escenarios 2030</p>
        </div>
        <MunicipalitySearch onSelect={(m) => {
          setMunicipioActual(prev => ({ ...prev, lau_id: m.muni_key, nombre: m.muni_display }));
          buscarYSimular(m.muni_key, 0, 0, 0, 0);
        }} />
      </header>

      <KpiCards datos={simulacion} municipio={municipioActual} />

      <div className="flex-1 grid grid-cols-12 gap-6 min-h-0">
        <div className="col-span-7 bg-white rounded-3xl shadow-sm border border-slate-100 flex flex-col overflow-hidden relative">
          {/* Aquí irá el mapa, por ahora dejamos el placeholder */}
          <div className="flex-1 bg-slate-100 flex items-center justify-center">
            <div className="text-center text-slate-400">
              <MapIcon size={48} className="mx-auto mb-2 opacity-20" />
              <p className="font-medium text-sm italic">Capa de Clustering Activa</p>
            </div>
          </div>
        </div>

        <div className="col-span-5 flex flex-col gap-6 min-h-0">
          <PolicySimulator values={simulacion} onChange={handleSliderChange} />
          <ImpactChart loading={loading} data={simulacion.evolucion} />
        </div>
      </div>
    </div>
  );
}