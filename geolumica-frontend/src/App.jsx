// App.jsx
import React, { useState } from 'react';
import Home from './pages/Home';
import Dashboard from './pages/Dashboard';
import About from './pages/About';
import Profile from './pages/Profile';
import SystemStatus from './pages/SystemStatus';
import AccessibilityPanel from './components/layout/AccessibilityPanel';

function App() {
  const [currentView, setCurrentView] = useState('home');
  // Estado global de autenticación (falso por defecto)
  const [isAuthenticated, setIsAuthenticated] = useState(false);

  // El "Guardia" de Rutas Seguras
  const handleSetView = (view) => {
    // Si la vista es protegida ('dashboard' o 'status') y NO ha iniciado sesión...
    if ((view === 'dashboard' || view === 'status') && !isAuthenticated) {
      setCurrentView('profile'); // Redirige al perfil en vez de romperse
    } else {
      setCurrentView(view); // Todo correcto, entra a la página solicitada
    }
  };

  return (
    <>
      {/* Rutas Públicas */}
      {currentView === 'home' && <Home currentView={currentView} setView={handleSetView} />}
      {currentView === 'about' && <About currentView={currentView} setView={handleSetView} />}
      {currentView === 'profile' && (
        <Profile 
          currentView={currentView} 
          setView={handleSetView} 
          isAuthenticated={isAuthenticated} 
          setIsAuthenticated={setIsAuthenticated} 
        />
      )}

      {/* Rutas Privadas (Blindadas por el guardia) */}
      {currentView === 'dashboard' && <Dashboard currentView={currentView} setView={handleSetView} />}
      {currentView === 'status' && <SystemStatus currentView={currentView} setView={handleSetView} />}
      
      {/* Panel Global Flotante */}
      <AccessibilityPanel />
    </>
  );
}

export default App;