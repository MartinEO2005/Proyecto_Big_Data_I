// components/dashboard/modules/PolicySimulator.jsx
import React from 'react';
import { Sliders } from 'lucide-react';

export default function PolicySimulator({ values, onChange }) {
  // Añadimos descripciones base para generar los textos dinámicos
  const controls = [
    { label: "Transporte y Conectividad", field: "inversionTransporte", descBase: "infraestructuras y red de transporte", min: -50, max: 100 },
    { label: "Tejido Empresarial", field: "estimuloEmpresas", descBase: "el tejido empresarial local", min: -50, max: 100 },
    { label: "Inmigración", field: "migracion_pct", descBase: "la atracción de nuevos residentes", min: -100, max: 100 },
    { label: "Renta/PIB", field: "pib_estimulo_pct", descBase: "el poder adquisitivo per cápita", min: -20, max: 40 }
  ];

  // Función para generar la descripción en tiempo real
  const renderDescription = (val, descBase) => {
    if (val === 0) return `Mantiene estable ${descBase}.`;
    if (val > 0) return `Un aumento del ${val}% representa un fuerte impulso en ${descBase}.`;
    return `Una reducción del ${Math.abs(val)}% representa una contracción en ${descBase}.`;
  };

  return (
    <div className="bg-white p-4 rounded-[1.5rem] shadow-sm border border-[#96551f]">
      
      <div className="flex items-center gap-2 mb-4">
        <div className="p-1.5 bg-[#96551f15] rounded-lg">
          <Sliders size={14} className="text-[#96551f]" />
        </div>
        <h3 className="text-[10px] font-black uppercase tracking-widest text-[#03132B]">
          Simulador Táctico Predictivo
        </h3>
      </div>
      
      <div className="grid grid-cols-2 gap-x-6 gap-y-4">
        {controls.map((c) => (
          <div key={c.field} className="space-y-1.5">
            
            <div className="flex justify-between items-center">
              <span className="text-[10px] font-bold uppercase tracking-wider text-[#03132B]">
                {c.label}
              </span>
              <span className="text-[11px] font-black text-[#efa748]">
                {values[c.field] > 0 ? '+' : ''}{values[c.field]}%
              </span>
            </div>
            
            {/* Slider de color dorado (#efa748) */}
            <input
              type="range"
              min={c.min}
              max={c.max}
              value={values[c.field]}
              onChange={(e) => onChange(c.field, parseInt(e.target.value))}
              className="w-full h-1.5 bg-slate-100 rounded-lg appearance-none cursor-pointer accent-[#efa748] hover:opacity-80 transition-opacity"
            />
            
            {/* Descripción Dinámica */}
            <p className="text-[8px] text-slate-500 font-medium italic leading-tight">
              {renderDescription(values[c.field], c.descBase)}
            </p>
            
          </div>
        ))}
      </div>
    </div>
  );
}