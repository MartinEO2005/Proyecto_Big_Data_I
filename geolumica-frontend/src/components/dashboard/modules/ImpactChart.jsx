// components/dashboard/modules/ImpactChart.jsx
import React from 'react';
import { 
  ComposedChart, Line, XAxis, YAxis, 
  CartesianGrid, Tooltip, ResponsiveContainer, Legend 
} from 'recharts';

export default function ImpactChart({ data, loading }) {
  if (!data || data.length === 0) return null;

  return (
    <div className="bg-white p-5 rounded-[2rem] border border-[#96551f] h-full flex flex-col relative shadow-none">
      <div className="flex justify-between items-center mb-3">
        {/* Título en color Oscuro (casi negro) */}
        <h3 className="text-[10px] font-black uppercase tracking-[0.2em] text-[#03132B]">
          Predicción Dinámica: Población y Desarrollo Económico
        </h3>
        {loading && <div className="w-2 h-2 bg-[#efa748] rounded-full animate-ping" />}
      </div>
      
      <div className="flex-1 w-full min-h-0">
        <ResponsiveContainer width="100%" height="100%">
          <ComposedChart data={data} margin={{ top: 10, right: 0, left: -15, bottom: 0 }}>
            
            {/* CUADRÍCULA COMPLETA (Vertical y Horizontal activadas) */}
            <CartesianGrid 
              strokeDasharray="4 4" 
              vertical={true} 
              horizontal={true} 
              stroke="#e2e8f0" 
            />
            
            <XAxis 
              dataKey="year" 
              axisLine={false} 
              tickLine={false} 
              tick={{fill: '#94a3b8', fontSize: 10, fontWeight: 'bold'}} 
              dy={10}
            />
            
            {/* EJE IZQUIERDO: Población (Color Oscuro) */}
            <YAxis 
              yAxisId="left"
              orientation="left"
              domain={['auto', 'auto']} 
              allowDecimals={false}
              tick={{fill: '#03132B', fontSize: 9, fontWeight: 'bold'}}
              axisLine={false}
              tickLine={false}
            />

            {/* EJE DERECHO: Crecimiento Base 100 (Color Dorado) */}
            <YAxis 
              yAxisId="right"
              orientation="right"
              domain={[0, 'auto']}
              tick={{fill: '#efa748', fontSize: 9, fontWeight: 'bold'}}
              axisLine={false}
              tickLine={false}
            />
            
            <Tooltip 
              contentStyle={{ borderRadius: '8px', border: '1px solid #e2e8f0', boxShadow: 'none', fontSize: '11px', fontWeight: 'bold' }}
            />
            
            <Legend 
              verticalAlign="top" 
              height={36} 
              iconType="circle" 
              wrapperStyle={{ fontSize: '10px', fontWeight: 'bold', textTransform: 'uppercase' }} 
            />

            {/* LÍNEA DE POBLACIÓN (Ahora es una línea simple, sin sombra ni área de relleno) */}
            <Line 
              yAxisId="left" 
              name="Población (Hab)" 
              type="monotone" 
              dataKey="pob" 
              stroke="#03132B" 
              strokeWidth={3}
              activeDot={{ r: 6, fill: '#03132B', strokeWidth: 0 }}
              dot={{ r: 4, fill: '#03132B', stroke: '#fff', strokeWidth: 2 }} 
            />

            {/* LÍNEA DE DESARROLLO (Igual, limpia) */}
            <Line 
              yAxisId="right" 
              name="Índice Económico (Base 100)" 
              type="monotone" 
              dataKey="luz" 
              stroke="#efa748" 
              strokeWidth={4} 
              activeDot={{ r: 6, fill: '#efa748', strokeWidth: 0 }}
              dot={{ r: 4, fill: '#efa748', stroke: '#fff', strokeWidth: 2 }} 
            />
            
          </ComposedChart>
        </ResponsiveContainer>
      </div>
    </div>
  );
}