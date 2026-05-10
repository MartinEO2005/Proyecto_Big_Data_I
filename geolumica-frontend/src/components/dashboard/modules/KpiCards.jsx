import React from 'react';
import { Users, Zap, Target } from 'lucide-react';

export default function KpiCards({ datos, municipio }) {
  return (
    <div className="grid grid-cols-1 md:grid-cols-3 gap-6">
      <div className="bg-white p-6 rounded-2xl shadow-sm border border-slate-100 flex items-center gap-4">
        <div className="p-3 bg-indigo-50 text-indigo-600 rounded-xl"><Users size={24}/></div>
        <div>
          <p className="text-sm text-slate-500 font-medium">Población Proyectada 2030</p>
          <h2 className="text-2xl font-bold text-slate-800">{datos.poblacion5y.toLocaleString()}</h2>
        </div>
      </div>
      <div className="bg-white p-6 rounded-2xl shadow-sm border border-slate-100 flex items-center gap-4">
        <div className="p-3 bg-emerald-50 text-emerald-600 rounded-xl"><Zap size={24}/></div>
        <div>
          <p className="text-sm text-slate-500 font-medium">Variación Absoluta</p>
          <h2 className="text-2xl font-bold text-slate-800">{datos.variacionAbsoluta > 0 ? '+' : ''}{datos.variacionAbsoluta.toLocaleString()} hab</h2>
        </div>
      </div>
      <div className="bg-white p-6 rounded-2xl shadow-sm border border-slate-100 flex items-center gap-4">
        <div className="p-3 bg-amber-50 text-amber-600 rounded-xl"><Target size={24}/></div>
        <div>
          <p className="text-sm text-slate-500 font-medium">Perfil Estratégico</p>
          <h2 className="text-lg font-bold text-slate-800 leading-tight">{municipio.perfilEstrategico}</h2>
        </div>
      </div>
    </div>
  );
}