// src/pages/Dashboard.jsx
import React from 'react';
import ViewManager from '../components/dashboard/ViewManager';
import PredictiveDashboard from '../components/dashboard/PredictiveDashboard';

export default function Dashboard() {
  return (
    <ViewManager 
      // Le pasamos el panel predictivo (React) a la vista estratégica
      childrenReact={<PredictiveDashboard />} 
      
      // Le pasamos el iframe de Power BI a la vista operativa
      childrenPowerBI={
        <div className="flex flex-col items-center justify-center h-full bg-slate-200 text-slate-500">
           <h2 className="text-xl font-bold mb-2">Contenedor Operativo (Power BI)</h2>
           <p>Aquí tu compañero incrustará el iframe del Modelo Estrella.</p>
        </div>
      } 
    />
  );
}