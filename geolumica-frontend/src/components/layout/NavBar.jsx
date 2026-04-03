import React from 'react';
import logo from '../../assets/logo.png';

export default function NavBar({ currentView, setView }) {
  return (
    <nav style={{ 
      display: 'flex', 
      justifyContent: 'space-between', 
      padding: '0 40px', 
      backgroundColor: '#ffffff', 
      borderBottom: '2px solid #161311', 
      alignItems: 'center', 
      height: '75px' 
    }}>
      <div style={{ display: 'flex', alignItems: 'center', cursor: 'pointer' }} onClick={() => setView('home')}>
        <img src={logo} alt="GeoLúmica Logo" style={{ height: '60px', objectFit: 'contain' }} />
      </div>
      <div style={{ display: 'flex', gap: '35px', alignItems: 'center', fontWeight: '600', fontSize: '1rem' }}>
        
        <span 
          onClick={() => setView('home')}
          style={{ cursor: 'pointer', color: currentView === 'home' ? '#efa748' : '#161311', borderBottom: currentView === 'home' ? '2px solid #efa748' : 'none', paddingBottom: '4px', transition: 'color 0.2s' }} 
          onMouseOver={(e) => e.target.style.color = '#efa748'} 
          onMouseOut={(e) => e.target.style.color = currentView === 'home' ? '#efa748' : '#161311'}
        >
          Inicio
        </span>

        <span 
          onClick={() => setView('about')}
          style={{ cursor: 'pointer', color: currentView === 'about' ? '#efa748' : '#161311', borderBottom: currentView === 'about' ? '2px solid #efa748' : 'none', paddingBottom: '4px', transition: 'color 0.2s' }} 
          onMouseOver={(e) => e.target.style.color = '#efa748'} 
          onMouseOut={(e) => e.target.style.color = currentView === 'about' ? '#efa748' : '#161311'}
        >
          Sobre Nosotros
        </span>

        <span 
          onClick={() => setView('dashboard')}
          style={{ cursor: 'pointer', color: currentView === 'dashboard' ? '#efa748' : '#161311', borderBottom: currentView === 'dashboard' ? '2px solid #efa748' : 'none', paddingBottom: '4px', transition: 'color 0.2s' }} 
          onMouseOver={(e) => e.target.style.color = '#efa748'} 
          onMouseOut={(e) => e.target.style.color = currentView === 'dashboard' ? '#efa748' : '#161311'}
        >
          Dashboard
        </span>

        <span 
          onClick={() => setView('profile')}
          style={{ cursor: 'pointer', color: currentView === 'profile' ? '#efa748' : '#161311', borderBottom: currentView === 'profile' ? '2px solid #efa748' : 'none', paddingBottom: '4px', transition: 'color 0.2s' }} 
          onMouseOver={(e) => e.target.style.color = '#efa748'} 
          onMouseOut={(e) => e.target.style.color = currentView === 'profile' ? '#efa748' : '#161311'}
        >
          Mi Perfil
        </span>

        {currentView === 'dashboard' && (
          <input 
            type="text" 
            placeholder="Buscar LAU_ID o Municipio..." 
            style={{ padding: '8px 12px', borderRadius: '6px', border: '1px solid #ccc', width: '220px', marginLeft: '15px', fontFamily: 'inherit', fontSize: '0.9rem' }}
          />
        )}

      </div>
    </nav>
  );
}