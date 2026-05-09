import React, { useState } from 'react';
import { LayoutDashboard, BarChart3, Settings } from 'lucide-react';

const ViewManager = ({ childrenReact, childrenPowerBI }) => {
  const [activeView, setActiveView] = useState('strategic'); // 'strategic' (React) o 'operational' (PowerBI)

  return (
    <div className="flex flex-col h-screen bg-slate-50">
      {/* Selector de Entorno Superior */}
      <nav className="bg-white border-b px-6 py-3 flex justify-between items-center shadow-sm">
        <div className="flex items-center gap-2">
          <div className="w-8 h-8 bg-orange-500 rounded-lg flex items-center justify-center text-white font-bold">G</div>
          <span className="font-bold text-slate-800 tracking-tight">GEOLÚMICA <span className="text-orange-500">v3.0</span></span>
        </div>

        <div className="flex bg-slate-100 p-1 rounded-xl border">
          <button 
            onClick={() => setActiveView('strategic')}
            className={`flex items-center gap-2 px-4 py-2 rounded-lg text-sm font-semibold transition-all ${
              activeView === 'strategic' ? 'bg-white shadow-sm text-orange-600' : 'text-slate-500 hover:text-slate-700'
            }`}
          >
            <LayoutDashboard size={18} /> Simulación Estratégica (ML)
          </button>
          <button 
            onClick={() => setActiveView('operational')}
            className={`flex items-center gap-2 px-4 py-2 rounded-lg text-sm font-semibold transition-all ${
              activeView === 'operational' ? 'bg-white shadow-sm text-blue-600' : 'text-slate-500 hover:text-slate-700'
            }`}
          >
            <BarChart3 size={18} /> Análisis Operativo (BI)
          </button>
        </div>
        
        <button className="text-slate-400 hover:text-slate-600"><Settings size={20}/></button>
      </nav>

      {/* Contenedor de Contenido */}
      <div className="flex-1 overflow-auto">
        {activeView === 'strategic' ? (
          <div className="p-6 animate-in fade-in duration-500">{childrenReact}</div>
        ) : (
          <div className="h-full w-full animate-in slide-in-from-right-5 duration-300">
            {childrenPowerBI}
          </div>
        )}
      </div>
    </div>
  );
};

export default ViewManager;