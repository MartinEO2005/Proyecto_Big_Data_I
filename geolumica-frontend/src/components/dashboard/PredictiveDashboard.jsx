// src/components/dashboard/PredictiveDashboard.jsx
import React, { useState } from 'react';
import { Users, Zap, Target, AlertCircle, Map as MapIcon, BarChart } from 'lucide-react';

// --- COMPONENTES STUB (Temporales para que no haya errores) ---

const KPIsTacticos = ({ datos, municipio }) => (
  <div className="grid grid-cols-4 gap-4">
    <div className="bg-white p-4 rounded-lg shadow-sm border-l-4 border-blue-500">
      <div className="flex items-center gap-2 text-gray-600 text-sm mb-2"><Users size={16} /> Población Estimada (+5 años)</div>
      <div className="text-2xl font-bold">{datos.poblacion5y.toLocaleString()}</div>
    </div>
    <div className="bg-white p-4 rounded-lg shadow-sm border-l-4 border-yellow-500">
      <div className="flex items-center gap-2 text-gray-600 text-sm mb-2"><Zap size={16} /> Radiancia Nocturna (+5 años)</div>
      <div className="text-2xl font-bold">{datos.luzNocturna5y} avg_rad</div>
    </div>
    <div className="bg-white p-4 rounded-lg shadow-sm border-l-4 border-purple-500">
      <div className="flex items-center gap-2 text-gray-600 text-sm mb-2"><Target size={16} /> Perfil Estratégico</div>
      <div className="text-xl font-bold text-purple-700">{municipio.perfilEstrategico}</div>
    </div>
    <div className="bg-white p-4 rounded-lg shadow-sm border-l-4 border-red-500">
      <div className="flex items-center gap-2 text-gray-600 text-sm mb-2"><AlertCircle size={16} /> Driver Crítico</div>
      <div className="text-xl font-bold text-red-600">{municipio.driverCritico}</div>
    </div>
  </div>
);

const SimulatorPanel = ({ parametros, onChange }) => (
  <div className="bg-white rounded-lg shadow-sm p-4 h-full">
    <h3 className="font-semibold border-b pb-2 mb-4">Simulador de Políticas</h3>
    <div className="flex flex-col gap-6">
      <div>
        <label className="text-sm font-medium flex justify-between">
          <span>Score de Conectividad</span>
          <span className="text-blue-600">{parametros.conectividad} / 100</span>
        </label>
        <input type="range" min="0" max="100" value={parametros.conectividad} onChange={(e) => onChange({ conectividad: parseInt(e.target.value) })} className="w-full mt-2" />
      </div>
      <div>
        <label className="text-sm font-medium flex justify-between">
          <span>Inversión en Red Transporte</span>
          <span className="text-orange-600">+{parametros.inversionTransporte}%</span>
        </label>
        <input type="range" min="0" max="200" value={parametros.inversionTransporte} onChange={(e) => onChange({ inversionTransporte: parseInt(e.target.value) })} className="w-full mt-2" />
      </div>
    </div>
  </div>
);

const MapClusterLayer = () => (
  <div className="bg-slate-800 rounded-lg h-full min-h-[400px] flex flex-col items-center justify-center text-slate-400">
    <MapIcon size={48} className="mb-4 opacity-50" />
    <p>Componente del Mapa de Clústeres (Deck.gl/Leaflet)</p>
    <p className="text-sm opacity-70">Esperando carga de polígonos GeoJSON...</p>
  </div>
);

const FeatureImportance = () => (
  <div className="bg-white rounded-lg shadow-sm p-4">
    <h3 className="font-semibold border-b pb-2 mb-4 flex items-center gap-2"><BarChart size={18}/> Feature Importance</h3>
    <div className="text-sm text-gray-500">Gráfica de variables predictivas en construcción...</div>
  </div>
);

// --- COMPONENTE PRINCIPAL (Orquestador) ---

export default function PredictiveDashboard() {
  // Estado inicial simulado
  const [municipioActual, setMunicipioActual] = useState({
    lau_id: "28079",
    nombre: "Madrid",
    perfilEstrategico: "Motor Económico",
    driverCritico: "Tasa Migratoria"
  });

  const [simulacion, setSimulacion] = useState({
    poblacion5y: 3300000,
    luzNocturna5y: 145.2,
    conectividad: 85,
    inversionTransporte: 50
  });

  const handleSimulacionChange = (nuevosParametros) => {
    setSimulacion({ ...simulacion, ...nuevosParametros });
    // Aquí conectaremos con Python en el futuro
  };

  return (
    <div className="flex flex-col gap-6 h-full">
      <header>
        <h1 className="text-2xl font-bold text-slate-800">Panel Predictivo Estratégico</h1>
        <p className="text-slate-500">Simulación demográfica y económica a 5 años (Random Forest & K-Means)</p>
      </header>

      {/* Fila 1: KPIs */}
      <KPIsTacticos datos={simulacion} municipio={municipioActual} />

      {/* Fila 2: Mapa y Simulador */}
      <div className="grid grid-cols-3 gap-6 flex-1">
        <div className="col-span-2 bg-white rounded-lg shadow-sm p-4 flex flex-col">
          <h3 className="font-semibold mb-4">Clasificación Territorial</h3>
          <MapClusterLayer />
        </div>
        <div className="flex flex-col gap-6">
          <SimulatorPanel parametros={simulacion} onChange={handleSimulacionChange} />
          <FeatureImportance />
        </div>
      </div>
    </div>
  );
}