// components/dashboard/modules/KpiCards.jsx
import React from 'react';
import { Users, Zap, Target } from 'lucide-react';

export default function KpiCards({ datos }) {
  const kpis = [
    { 
      label: "Población 2030", 
      valor: datos.poblacion5y.toLocaleString(), 
      suffix: "hab",
      icon: Users, 
      color: "indigo" 
    },
    { 
      label: "Variación", 
      valor: (datos.variacionAbsoluta > 0 ? '+' : '') + datos.variacionAbsoluta.toLocaleString(), 
      suffix: "hab",
      icon: Zap, 
      color: "emerald" 
    },
    { 
      label: "Eficiencia Luz/PIB", 
      valor: "84.2", 
      suffix: "%",
      icon: Target, 
      color: "blue" 
    }
  ];

  return (
    <div className="grid grid-cols-3 gap-4">
      {kpis.map((k, i) => (
        <div key={i} className="bg-white p-3 rounded-2xl border border-slate-100 flex items-center gap-3 shadow-sm">
          <div className={`p-2 bg-${k.color}-50 text-${k.color}-600 rounded-lg`}>
            <k.icon size={16} />
          </div>
          <div>
            <p className="text-[10px] font-bold text-slate-400 uppercase tracking-tight">{k.label}</p>
            <h2 className="text-sm font-black text-slate-800">
              {k.valor} <span className="text-[9px] font-medium text-slate-400">{k.suffix}</span>
            </h2>
          </div>
        </div>
      ))}
    </div>
  );
}