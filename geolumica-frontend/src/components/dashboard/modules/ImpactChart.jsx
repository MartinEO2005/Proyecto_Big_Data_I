// src/components/dashboard/modules/ImpactChart.jsx
import React from 'react';
import { BarChart as BarChartIcon } from 'lucide-react';
import { AreaChart, Area, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer } from 'recharts';

export default function ImpactChart({ data, loading }) {
  return (
    <div className="bg-white p-6 rounded-3xl shadow-sm border border-slate-100 flex-1 flex flex-col min-h-0 relative">
      <h3 className="font-bold text-slate-700 mb-6 flex items-center gap-2 text-sm uppercase tracking-wider">
        <BarChartIcon size={18} className="text-slate-400"/> Evolución Demográfica 2023-2030
      </h3>
      
      {loading && (
        <div className="absolute inset-0 bg-white/60 z-10 flex items-center justify-center backdrop-blur-[1px] font-bold text-indigo-600 rounded-3xl uppercase text-xs tracking-tighter">
          Procesando Modelos...
        </div>
      )}

      <div className="flex-1 w-full min-h-0">
        <ResponsiveContainer width="100%" height="100%">
          <AreaChart data={data} margin={{ top: 10, right: 10, left: -20, bottom: 0 }}>
            <defs>
              <linearGradient id="colorPob" x1="0" y1="0" x2="0" y2="1">
                <stop offset="5%" stopColor="#6366f1" stopOpacity={0.3}/>
                <stop offset="95%" stopColor="#6366f1" stopOpacity={0}/>
              </linearGradient>
            </defs>
            <CartesianGrid strokeDasharray="3 3" vertical={false} stroke="#f1f5f9" />
            <XAxis dataKey="year" axisLine={false} tickLine={false} tick={{fill: '#94a3b8', fontSize: 10}} dy={10} />
            <YAxis domain={['dataMin - 100', 'dataMax + 100']} hide />
            <Tooltip 
              contentStyle={{ borderRadius: '15px', border: 'none', boxShadow: '0 10px 15px -3px rgba(0,0,0,0.1)', fontSize: '12px' }}
              labelStyle={{ fontWeight: 'bold', color: '#6366f1' }}
            />
            <Area 
              type="monotone" 
              dataKey="poblacion" 
              stroke="#6366f1" 
              strokeWidth={4} 
              fillOpacity={1} 
              fill="url(#colorPob)" 
              animationDuration={800} 
            />
          </AreaChart>
        </ResponsiveContainer>
      </div>
    </div>
  );
}