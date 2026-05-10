// src/components/dashboard/ViewManager.jsx
import React, { useState } from 'react';
import { BrainCircuit, BarChart3 } from 'lucide-react';

export default function ViewManager({ childrenReact, childrenPowerBI }) {
  const [activeView, setActiveView] = useState('strategic'); // 'strategic' o 'operational'

  return (
    <div className="h-full flex flex-col">
      {/* Selector de Cuadros de Mando (Tabs) */}
      <div className="bg-white border-b px-8 py-2 flex justify-start gap-8">
        <button 
          onClick={() => setActiveView('strategic')}
          className={`flex items-center gap-2 py-2 text-sm font-bold transition-all border-b-2 ${
            activeView === 'strategic' 
            ? 'border-indigo-600 text-indigo-600' 
            : 'border-transparent text-slate-400 hover:text-slate-600'
          }`}
        >
          <BrainCircuit size={18} /> MODELO PREDICTIVO (ML)
        </button>
        <button 
          onClick={() => setActiveView('operational')}
          className={`flex items-center gap-2 py-2 text-sm font-bold transition-all border-b-2 ${
            activeView === 'operational' 
            ? 'border-blue-600 text-blue-600' 
            : 'border-transparent text-slate-400 hover:text-slate-600'
          }`}
        >
          <BarChart3 size={18} /> ANÁLISIS OPERATIVO (BI)
        </button>
      </div>

      {/* Contenedor de las vistas: h-full asegura que se adapte al espacio restante */}
      <div className="flex-1 overflow-hidden">
        {activeView === 'strategic' ? childrenReact : childrenPowerBI}
      </div>
    </div>
  );
}