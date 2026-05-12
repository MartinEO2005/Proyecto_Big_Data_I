// src/components/dashboard/modules/PolicySimulator.jsx
import React from 'react';
import { Sliders } from 'lucide-react';

export default function PolicySimulator({ values, onChange }) {
  // Verifica que los controles tengan estos nombres de campo exactos
  const controls = [
  { label: "Conectividad", field: "inversionTransporte", color: "indigo" },
  { label: "Empresas", field: "estimuloEmpresas", color: "emerald" },
  { label: "Inmigración", field: "migracion_pct", color: "orange" },
  { label: "Impulso PIB", field: "pib_estimulo_pct", color: "blue" }
];

  return (
    <div className="bg-slate-900 text-white p-5 rounded-3xl shadow-xl border border-slate-800">
      <div className="flex items-center gap-2 mb-4">
        <Sliders size={16} className="text-indigo-400"/>
        <h3 className="text-xs font-bold uppercase tracking-widest text-slate-400">Simulador PRO</h3>
      </div>
      
      {/* GRID DE 2 COLUMNAS para ahorrar espacio */}
      <div className="grid grid-cols-2 gap-x-6 gap-y-4">
        {controls.map((c) => (
          <div key={c.field} className="space-y-1">
            <div className="flex justify-between text-[10px] font-bold uppercase text-slate-500">
              <span>{c.label}</span>
              <span className={`text-${c.color}-400`}>+{values[c.field]}%</span>
            </div>
            <input 
              type="range" min="0" max="100" step="5" value={values[c.field]} 
              onChange={(e) => onChange(c.field, Number(e.target.value))}
              className="w-full h-1 bg-slate-700 rounded-lg appearance-none cursor-pointer accent-indigo-500" 
            />
          </div>
        ))}
      </div>
    </div>
  );
}