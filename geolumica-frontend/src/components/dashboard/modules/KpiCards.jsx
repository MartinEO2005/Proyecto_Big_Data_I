// components/dashboard/modules/KpiCards.jsx
import React from 'react';
import { Users, TrendingUp, Map, Fingerprint, Activity } from 'lucide-react';

export default function KpiCards({ datos }) {
  const clusterColor = datos.color_cluster || '#cbd5e1';
  const textColorDark = '#03132B';

  return (
    <div className="flex flex-wrap lg:flex-nowrap gap-2">
      {/* 1. Población */}
      <div className="flex-1 bg-white p-2.5 rounded-xl shadow-sm border border-slate-100 flex items-center gap-2">
        <div className="p-2 rounded-lg bg-[#03132B] text-white shrink-0"><Users size={16}/></div>
        <div className="min-w-0">
          <p className="text-[8px] text-slate-400 font-bold uppercase truncate">Población 2030</p>
          <h2 className="text-xs font-black text-[#03132B]">{datos.poblacion5y?.toLocaleString()}</h2>
        </div>
      </div>
      
      {/* 2. Variación */}
      <div className="flex-1 bg-white p-2.5 rounded-xl shadow-sm border border-slate-100 flex items-center gap-2">
        <div className="p-2 rounded-lg bg-[#96551f15] text-[#96551f] shrink-0"><TrendingUp size={16}/></div>
        <div className="min-w-0">
          <p className="text-[8px] text-slate-400 font-bold uppercase truncate">Var. Habitantes</p>
          <h2 className="text-xs font-black text-[#96551f]">{datos.variacionAbsoluta > 0 ? '+' : ''}{datos.variacionAbsoluta?.toLocaleString()}</h2>
        </div>
      </div>

      {/* 3. Desarrollo Urbano */}
      <div className="flex-1 bg-white p-2.5 rounded-xl shadow-sm border border-slate-100 flex items-center gap-2">
        <div className="p-2 rounded-lg bg-[#F6A24415] text-[#F6A244] shrink-0"><Activity size={16}/></div>
        <div className="min-w-0">
          <p className="text-[8px] text-slate-400 font-bold uppercase truncate">Desarrollo Urbano</p>
          <h2 className="text-xs font-black text-[#F6A244]">{datos.variacionEconomica > 0 ? '+' : ''}{datos.variacionEconomica}%</h2>
        </div>
      </div>

      {/* 4. Perfil del Cluster (TRUNCADO Y SIEMPRE TEXTO OSCURO) */}
      <div 
        className="flex-[1.2] p-2.5 rounded-xl border flex items-center gap-2 shadow-sm min-w-0"
        style={{ backgroundColor: `${clusterColor}40`, borderColor: `${clusterColor}80` }}
      >
        <div className="p-1.5 rounded-lg bg-[#00000015] text-[#03132B] shrink-0"><Map size={16}/></div>
        <div className="min-w-0 overflow-hidden">
          <p className="text-[8px] font-bold uppercase opacity-60 text-[#03132B] truncate">Perfil del Clúster</p>
          <h2 className="text-[10px] font-black text-[#03132B] truncate" title={datos.perfil_estrategico}>
            {datos.perfil_estrategico || "Calculando..."}
          </h2>
        </div>
      </div>

      {/* 5. Driver Crítico */}
      <div className="flex-[1.2] p-2.5 rounded-xl border bg-[#efa748] border-[#d99236] flex items-center gap-2 shadow-sm min-w-0">
        <div className="p-1.5 rounded-lg bg-[#03132B] text-[#efa748] shrink-0"><Fingerprint size={16}/></div>
        <div className="min-w-0">
          <p className="text-[8px] font-bold uppercase opacity-70 text-[#03132B] truncate">Driver Crítico</p>
          <h2 className="text-[10px] font-black text-[#03132B] truncate">{datos.driver_critico || "Analizando..."}</h2>
        </div>
      </div>
    </div>
  );
}