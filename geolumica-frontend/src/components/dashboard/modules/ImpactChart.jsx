// components/dashboard/modules/ImpactChart.jsx
import React from 'react';
import { AreaChart, Area, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer } from 'recharts';

export default function ImpactChart({ title, data, color, loading }) {
  // Si no hay datos, mostramos un estado de carga elegante para evitar el crash
  if (!data || data.length === 0) {
    return (
      <div className="bg-white p-6 rounded-[2.5rem] shadow-sm border border-slate-100 flex-1 flex items-center justify-center">
        <div className="flex flex-col items-center gap-2">
          <div className="w-8 h-8 border-4 border-slate-100 border-t-indigo-500 rounded-full animate-spin"></div>
          <span className="text-[10px] font-bold text-slate-400 uppercase tracking-widest">Calculando Proyección...</span>
        </div>
      </div>
    );
  }

  return (
    <div className="bg-white p-6 rounded-[2.5rem] shadow-sm border border-slate-100 flex-1 flex flex-col min-h-0 relative group">
      <div className="flex justify-between items-center mb-6">
        <h3 className="text-[10px] font-black uppercase tracking-[0.2em] text-slate-400">{title}</h3>
        {loading && (
          <div className="flex items-center gap-2">
            <span className="w-2 h-2 bg-indigo-500 rounded-full animate-ping"></span>
            <span className="text-[10px] text-indigo-500 font-black uppercase tracking-widest">Sincronizando</span>
          </div>
        )}
      </div>
      
      <div className="flex-1 w-full">
        <ResponsiveContainer width="100%" height="100%">
          <AreaChart data={data} margin={{ top: 10, right: 10, left: -20, bottom: 0 }}>
            <defs>
              <linearGradient id={`color${color.replace('#', '')}`} x1="0" y1="0" x2="0" y2="1">
                <stop offset="5%" stopColor={color} stopOpacity={0.3}/>
                <stop offset="95%" stopColor={color} stopOpacity={0}/>
              </linearGradient>
            </defs>
            <CartesianGrid strokeDasharray="3 3" vertical={false} stroke="#f1f5f9" />
            <XAxis 
              dataKey="year" 
              axisLine={false} 
              tickLine={false} 
              tick={{fill: '#94a3b8', fontSize: 10, fontWeight: 'bold'}} 
              dy={10}
            />
            {/* El dominio 'auto' es la clave para que la gráfica se mueva con cambios pequeños */}
            <YAxis 
              hide 
              domain={['auto', 'auto']} 
            />
            <Tooltip 
              contentStyle={{ 
                borderRadius: '20px', 
                border: 'none', 
                boxShadow: '0 20px 25px -5px rgb(0 0 0 / 0.1)', 
                fontSize: '12px',
                fontWeight: 'bold',
                padding: '12px'
              }}
              cursor={{ stroke: color, strokeWidth: 2, strokeDasharray: '5 5' }}
            />
            <Area 
              type="monotone" 
              dataKey="valor" 
              stroke={color} 
              strokeWidth={4}
              fillOpacity={1} 
              fill={`url(#color${color.replace('#', '')})`} 
              animationDuration={500}
              isAnimationActive={true} // Obligamos a que la animación se dispare al cambiar datos
            />
          </AreaChart>
        </ResponsiveContainer>
      </div>
    </div>
  );
}