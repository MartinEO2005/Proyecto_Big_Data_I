import React, { useState } from 'react';
import Home from './pages/Home';
import Dashboard from './pages/Dashboard';
import About from './pages/About';
import Profile from './pages/Profile';
import AccessibilityPanel from './components/layout/AccessibilityPanel'; // IMPORTA EL PANEL AQUÍ

function App() {
  const [currentView, setCurrentView] = useState('home');

  return (
    <>
      {currentView === 'home' && <Home currentView={currentView} setView={setCurrentView} />}
      {currentView === 'dashboard' && <Dashboard currentView={currentView} setView={setCurrentView} />}
      {currentView === 'about' && <About currentView={currentView} setView={setCurrentView} />}
      {currentView === 'profile' && <Profile currentView={currentView} setView={setCurrentView} />} 
      
      {/* AÑADE EL COMPONENTE AQUÍ. Así flotará en todas las vistas */}
      <AccessibilityPanel />
    </>
  );
}

export default App;