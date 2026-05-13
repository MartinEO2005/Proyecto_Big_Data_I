// components/dashboard/modules/KpiCards.jsx
import React from 'react';
import { Users, Zap, Map, Fingerprint } from 'lucide-react';

export default function KpiCards({ datos }) {
  // Extraemos el color del perfil basado en tu diccionario de Python
  const getProfileColor = (perfil) => {
    if (perfil?.includes('Despoblación Grave')) return 'red';
    if (perfil?.includes('Fuerte Crecimiento')) return 'sky';
    if (perfil?.includes('Grandes Ciudades')) return 'blue';
    return 'amber'; // Default para estancamiento
  };

  const profileColor = getProfileColor(datos.perfil_estrategico);

  return (
    <div className="grid grid-cols-12 gap-3">
      {/* KPIs Cuantitativos (Simulación) */}
      <div className="col-span-3 bg-white p-3 rounded-2xl shadow-xs border border-slate-100 flex items-center gap-3">
        <div className="p-2 bg-indigo-50 text-indigo-600 rounded-lg"><Users size={16}/></div>
        <div className="min-w-0">
          <p className="text-[9px] text-slate-400 font-bold uppercase truncate">Pob. 2030</p>
          <h2 className="text-sm font-black text-slate-800">{datos.poblacion5y.toLocaleString()}</h2>
        </div>
      </div>
      
      <div className="col-span-3 bg-white p-3 rounded-2xl shadow-xs border border-slate-100 flex items-center gap-3">
        <div className="p-2 bg-emerald-50 text-emerald-600 rounded-lg"><Zap size={16}/></div>
        <div className="min-w-0">
          <p className="text-[9px] text-slate-400 font-bold uppercase truncate">Variación</p>
          <h2 className="text-sm font-black text-slate-800">{datos.variacionAbsoluta > 0 ? '+' : ''}{datos.variacionAbsoluta.toLocaleString()}</h2>
        </div>
      </div>

      {/* KPIs Cualitativos (Clustering & Clasificación) - Ocupan más espacio para leerse bien */}
      <div className="col-span-6 flex gap-3">
        <div className={`flex-1 bg-${profileColor}-50 p-3 rounded-2xl border border-${profileColor}-100 flex items-center gap-3`}>
          <div className={`p-2 bg-${profileColor}-100 text-${profileColor}-600 rounded-lg`}><Map size={16}/></div>
          <div className="min-w-0">
            <p className={`text-[9px] text-${profileColor}-500 font-bold uppercase truncate`}>Perfil Estratégico</p>
            <h2 className={`text-xs font-black text-${profileColor}-900 truncate`}>{datos.perfil_estrategico || "Calculando Cluster..."}</h2>
          </div>
        </div>

        <div className="flex-1 bg-slate-900 p-3 rounded-2xl border border-slate-800 flex items-center gap-3 shadow-md">
          <div className="p-2 bg-slate-800 text-indigo-400 rounded-lg"><Fingerprint size={16}/></div>
          <div className="min-w-0">
            <p className="text-[9px] text-slate-500 font-bold uppercase truncate">Driver Crítico</p>
            <h2 className="text-xs font-black text-white truncate">{datos.driver_critico || "Analizando Variables..."}</h2>
          </div>
        </div>
      </div>
    </div>
  );
}