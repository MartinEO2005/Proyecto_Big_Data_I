// src/components/dashboard/modules/PolicySimulator.jsx
import React from 'react';
import { Map as MapIcon, Target, Sliders, TrendingUp, Globe } from 'lucide-react';

const Control = ({ label, value, field, color, icon: Icon, onChange }) => (
  <div className="space-y-2">
    <div className="flex justify-between text-[10px] font-bold uppercase tracking-widest text-slate-400">
      <span className="flex items-center gap-1"><Icon size={12}/> {label}</span>
      <span className={`text-${color}-400`}>+{value}%</span>
    </div>
    <input 
      type="range" min="0" max="100" step="5" value={value} 
      onChange={(e) => onChange(field, Number(e.target.value))}
      className="w-full h-1.5 bg-slate-700 rounded-lg appearance-none cursor-pointer accent-indigo-500" 
    />
  </div>
);

export default function PolicySimulator({ values, onChange }) {
  return (
    <div className="bg-slate-900 text-white p-6 rounded-3xl shadow-xl">
      <h3 className="font-bold mb-6 flex items-center gap-2 text-indigo-400 text-sm uppercase tracking-wider">
        <Sliders size={18}/> Simulador de Políticas
      </h3>
      <div className="grid grid-cols-1 gap-5">
        <Control label="Conectividad" value={values.inversionTransporte} field="inversionTransporte" color="indigo" icon={MapIcon} onChange={onChange} />
        <Control label="Estímulo Empresas" value={values.estimuloEmpresas} field="estimuloEmpresas" color="emerald" icon={Target} onChange={onChange} />
        <Control label="Inmigración" value={values.migracion_pct} field="migracion_pct" color="orange" icon={Globe} onChange={onChange} />
        <Control label="Crecimiento PIB" value={values.pib_estimulo_pct} field="pib_estimulo_pct" color="blue" icon={TrendingUp} onChange={onChange} />
      </div>
    </div>
  );
}