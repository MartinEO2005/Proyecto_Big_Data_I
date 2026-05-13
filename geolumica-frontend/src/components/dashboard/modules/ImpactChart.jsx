import React from 'react';
import { 
  ComposedChart, Line, Area, XAxis, YAxis, 
  CartesianGrid, Tooltip, ResponsiveContainer, Legend 
} from 'recharts';

export default function ImpactChart({ data, loading }) {
  if (!data || data.length === 0) return null;

  return (
    <div className="bg-white p-5 rounded-[2rem] shadow-sm border border-slate-100 h-full flex flex-col relative">
      <div className="flex justify-between items-center mb-3">
        <h3 className="text-[10px] font-black uppercase tracking-[0.2em] text-slate-400">
          Correlación Dinámica: Población vs Economía
        </h3>
        {loading && <div className="w-2 h-2 bg-indigo-500 rounded-full animate-ping" />}
      </div>
      
      <div className="flex-1 w-full min-h-0">
        <ResponsiveContainer width="100%" height="100%">
          <ComposedChart data={data} margin={{ top: 10, right: 0, left: -15, bottom: 0 }}>
            <defs>
              <linearGradient id="colorPob" x1="0" y1="0" x2="0" y2="1">
                <stop offset="5%" stopColor="#6366f1" stopOpacity={0.2}/>
                <stop offset="95%" stopColor="#6366f1" stopOpacity={0}/>
              </linearGradient>
            </defs>
            
            <CartesianGrid strokeDasharray="3 3" vertical={false} stroke="#f8fafc" />
            
            <XAxis 
              dataKey="year" 
              axisLine={false} 
              tickLine={false} 
              tick={{fill: '#94a3b8', fontSize: 10, fontWeight: 'bold'}} 
              dy={10}
            />
            
            {/* EJE IZQUIERDO: Población (Sin decimales) */}
            <YAxis 
              yAxisId="left"
              orientation="left"
              domain={['auto', 'auto']}
              allowDecimals={false}
              tick={{fill: '#6366f1', fontSize: 9, fontWeight: 'bold'}}
              axisLine={false}
              tickLine={false}
            />

            {/* EJE DERECHO: Luz Nocturna */}
            <YAxis 
              yAxisId="right"
              orientation="right"
              domain={['auto', 'auto']}
              tick={{fill: '#f59e0b', fontSize: 9, fontWeight: 'bold'}}
              axisLine={false}
              tickLine={false}
            />
            
            <Tooltip 
              contentStyle={{ borderRadius: '12px', border: 'none', boxShadow: '0 10px 15px -3px rgba(0,0,0,0.1)', fontSize: '11px', fontWeight: 'bold' }}
            />
            
            <Legend 
              verticalAlign="top" 
              height={36} 
              iconType="circle" 
              wrapperStyle={{ fontSize: '10px', fontWeight: 'bold', textTransform: 'uppercase' }} 
            />

            {/* POBLACIÓN: Todo en un solo componente (Sombra + Línea + Puntos) */}
            <Area 
              yAxisId="left" 
              name="Población (Hab)" 
              type="monotone" 
              dataKey="pob" 
              fill="url(#colorPob)" 
              stroke="#6366f1" 
              strokeWidth={3}
              activeDot={{ r: 6, strokeWidth: 0 }}
              dot={{ r: 4, fill: '#6366f1', stroke: '#fff', strokeWidth: 2 }} 
            />

            {/* LUZ: Todo en un solo componente (Línea + Puntos) */}
            <Line 
              yAxisId="right" 
              name="Actividad Económica (Luz)" 
              type="monotone" 
              dataKey="luz" 
              stroke="#f59e0b" 
              strokeWidth={3} 
              strokeDasharray="5 5" 
              activeDot={{ r: 6, strokeWidth: 0 }}
              dot={{ r: 4, fill: '#f59e0b', stroke: '#fff', strokeWidth: 2 }} 
            />
            
          </ComposedChart>
        </ResponsiveContainer>
      </div>
    </div>
  );
}