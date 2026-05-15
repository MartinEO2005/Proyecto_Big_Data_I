// src/components/dashboard/ViewManager.jsx
import React, { useState } from 'react';
import { BrainCircuit, BarChart3 } from 'lucide-react';

export default function ViewManager({ childrenReact }) {
  const [activeView, setActiveView] = useState('strategic');

  // AQUÍ PEGARÁS EL ENLACE QUE TE PASE TU COMPAÑERO
  const powerBiUrl = "https://app.powerbi.com/view?r=eyJrIjoiZGY3YmZiZDYtZDZhNi00OGRmLTkxZDgtYTY3YjBlNzg5NmVhIiwidCI6IjljMzE3YzczLWEyOWUtNDdjZC04ODA5LTEyNjY2MGI5MDczYiJ9";

  return (
    <div className="h-full flex flex-col bg-slate-50">
      
      {/* Selector de Cuadros de Mando (Tabs) con Colores Corporativos */}
      <div className="bg-white border-b border-slate-200 px-8 py-0 flex justify-start gap-8 shrink-0 shadow-sm z-10">
        <button 
          onClick={() => setActiveView('strategic')}
          className={`flex items-center gap-2 py-3 text-[11px] font-black tracking-widest uppercase transition-all border-b-[3px] ${
            activeView === 'strategic' 
            ? 'border-[#efa748] text-[#03132B]' 
            : 'border-transparent text-slate-400 hover:text-[#03132B]'
          }`}
        >
          <BrainCircuit size={16} className={activeView === 'strategic' ? 'text-[#efa748]' : ''} /> 
          Modelo Predictivo (ML)
        </button>
        
        <button 
          onClick={() => setActiveView('operational')}
          className={`flex items-center gap-2 py-3 text-[11px] font-black tracking-widest uppercase transition-all border-b-[3px] ${
            activeView === 'operational' 
            ? 'border-[#03132B] text-[#03132B]' 
            : 'border-transparent text-slate-400 hover:text-[#03132B]'
          }`}
        >
          <BarChart3 size={16} className={activeView === 'operational' ? 'text-[#03132B]' : ''} /> 
          Análisis Operativo (BI)
        </button>
      </div>

      {/* Contenedor de Vistas (Superpuestas para no perder estado al cambiar) */}
      <div className="flex-1 min-h-0 relative">
        
        {/* VISTA 1: React / ML Dashboard (GeoLúmica) */}
        <div 
          className="absolute inset-0" 
          style={{ 
            visibility: activeView === 'strategic' ? 'visible' : 'hidden',
            opacity: activeView === 'strategic' ? 1 : 0,
            transition: 'opacity 0.2s ease-in-out'
          }}
        >
          {childrenReact}
        </div>
        
        {/* VISTA 2: PowerBI Dashboard */}
        <div 
          className="absolute inset-0 p-3" 
          style={{ 
            visibility: activeView === 'operational' ? 'visible' : 'hidden',
            opacity: activeView === 'operational' ? 1 : 0,
            transition: 'opacity 0.2s ease-in-out'
          }}
        >
          <div className="w-full h-full bg-white rounded-[1.5rem] shadow-sm border border-slate-200 overflow-hidden">
            {/* Si aún no tienes el link, esto muestra un aviso. Cuando lo tengas, descomenta el iframe */}
            
            {powerBiUrl.includes("AQUI_SU_CODIGO_LARGO") ? (
              <div className="w-full h-full flex flex-col items-center justify-center text-slate-400">
                <BarChart3 size={48} className="mb-4 opacity-20" />
                <p className="font-bold">Esperando enlace de Power BI...</p>
                <p className="text-xs mt-2">Dile a tu compañero que publique el .pbix y pegue aquí el iframe.</p>
              </div>
            ) : (
              <iframe 
                title="Panel Operativo GeoLúmica" 
                width="100%" 
                height="100%" 
                src={powerBiUrl} 
                frameBorder="0" 
                allowFullScreen="true"
                className="w-full h-full"
              ></iframe>
            )}

          </div>
        </div>

      </div>
    </div>
  );
}