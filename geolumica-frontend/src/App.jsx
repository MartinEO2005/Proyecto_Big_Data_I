import React, { useState } from 'react';
import Home from './pages/Home';
import Dashboard from './pages/Dashboard';
import About from './pages/About';

function App() {
  const [currentView, setCurrentView] = useState('home');

  return (
    <>
      {currentView === 'home' && <Home currentView={currentView} setView={setCurrentView} />}
      {currentView === 'dashboard' && <Dashboard setView={setCurrentView} />}
      {currentView === 'about' && <About currentView={currentView} setView={setCurrentView} />}
    </>
  );
}

export default App;