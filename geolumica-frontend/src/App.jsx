import React, { useState } from 'react';
import Home from './pages/Home';
import Dashboard from './pages/Dashboard';
import About from './pages/About';
import Profile from './pages/Profile';
import AccessibilityPanel from './components/layout/AccessibilityPanel';

function App() {
  const [currentView, setCurrentView] = useState('home');
  // NUEVO: Estado global de autenticación (falso por defecto)
  const [isAuthenticated, setIsAuthenticated] = useState(false);

  // NUEVO: El "Guardia". Intercepta cualquier intento de cambio de vista
  const handleSetView = (view) => {
    // Si intentas ir al dashboard y no estás logeado, te manda al perfil (login)
    if (view === 'dashboard' && !isAuthenticated) {
      setCurrentView('profile'); 
    } else {
      setCurrentView(view); // Si está todo bien, vas a donde querías
    }
  };

  return (
    <>
      {/* Pasamos handleSetView en lugar de setCurrentView para que pasen por el "Guardia" */}
      {currentView === 'home' && <Home currentView={currentView} setView={handleSetView} />}
      {currentView === 'dashboard' && <Dashboard currentView={currentView} setView={handleSetView} />}
      {currentView === 'about' && <About currentView={currentView} setView={handleSetView} />}
      
      {/* Al perfil le pasamos los estados de autenticación para que pueda modificarlos */}
      {currentView === 'profile' && (
        <Profile 
          currentView={currentView} 
          setView={handleSetView} 
          isAuthenticated={isAuthenticated} 
          setIsAuthenticated={setIsAuthenticated} 
        />
      )} 
      
      <AccessibilityPanel />
    </>
  );
}

export default App;