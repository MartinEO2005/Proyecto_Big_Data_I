// src/pages/Dashboard.jsx
import React from 'react';
import NavBar from '../components/layout/NavBar';
import ViewManager from '../components/dashboard/ViewManager';
import PredictiveDashboard from '../components/dashboard/PredictiveDashboard';

// AQUÍ ESTABA EL ERROR: Necesitamos recibir currentView y setView
export default function Dashboard({ currentView, setView }) { 
  return (
    <div className="h-screen w-full bg-slate-50 flex flex-col overflow-hidden">
      
      {/* 1. Tu NavBar original, ahora CON SUS PROPS CONECTADOS */}
      <div className="z-50 shadow-sm relative">
        <NavBar currentView={currentView} setView={setView} />
      </div>

      {/* 2. Área de Contenido Principal */}
      <main className="flex-1 overflow-hidden relative">
        <ViewManager 
          childrenReact={<PredictiveDashboard />} 
          childrenPowerBI={
            <div className="h-full w-full p-6">
              <div className="h-full w-full bg-white rounded-3xl border-2 border-dashed border-slate-200 flex flex-col items-center justify-center text-slate-400">
                <h2 className="text-xl font-bold mb-2 text-slate-600">Análisis Operativo (PowerBI)</h2>
                <p>Aquí incrustaremos el iframe del dashboard histórico.</p>
              </div>
            </div>
          } 
        />
      </main>
    </div>
  );
}