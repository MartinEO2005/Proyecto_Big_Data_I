// components/dashboard/modules/PolicySimulator.jsx
import React from 'react';
import { Sliders } from 'lucide-react';

export default function PolicySimulator({ values, onChange }) {
  const controls = [
    { label: "Conectividad", field: "inversionTransporte", color: "indigo", min: -50, max: 100 },
    { label: "Empresas", field: "estimuloEmpresas", color: "emerald", min: -50, max: 100 },
    { label: "Inmigración", field: "migracion_pct", color: "orange", min: -100, max: 100 },
    { label: "Renta/PIB", field: "pib_estimulo_pct", color: "blue", min: -20, max: 40 }
  ];

  return (
    <div className="bg-slate-900 text-white p-3 rounded-[1.25rem] shadow-xl border border-slate-800">
      <div className="flex items-center gap-1.5 mb-2.5">
        <Sliders size={12} className="text-indigo-400" />
        <h3 className="text-[8px] font-black uppercase tracking-widest text-slate-400">Simulador Táctico</h3>
      </div>
      
      <div className="grid grid-cols-2 gap-x-4 gap-y-2.5">
        {controls.map((c) => (
          <div key={c.field} className="space-y-1">
            <div className="flex justify-between items-center">
              <span className="text-[8px] font-bold uppercase tracking-wider text-slate-500">{c.label}</span>
              <span className={`text-[9px] font-black text-${c.color}-400`}>
                {values[c.field] > 0 ? '+' : ''}{values[c.field]}%
              </span>
            </div>
            <input
              type="range"
              min={c.min}
              max={c.max}
              value={values[c.field]}
              onChange={(e) => onChange(c.field, parseInt(e.target.value))}
              className="w-full h-1 bg-slate-800 rounded-lg appearance-none cursor-pointer accent-indigo-500 hover:accent-indigo-400 transition-all"
            />
          </div>
        ))}
      </div>
    </div>
  );
}