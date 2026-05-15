// layout/NavBar.jsx
import React from 'react';
import logo from '../../assets/logo.png';
import { useTranslation } from 'react-i18next';

export default function NavBar({ currentView, setView }) {
  const { t } = useTranslation();

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
      {/* EL LOGO */}
      <button 
        className="nav-logo-btn"
        onClick={() => setView('home')}
        aria-label={t('nav.home')}
        style={{ display: 'flex', alignItems: 'center' }}
      >
        <img src={logo} alt="GeoLúmica Logo" style={{ height: '60px', objectFit: 'contain' }} />
      </button>

      <div style={{ display: 'flex', gap: '35px', alignItems: 'center', fontWeight: '600', fontSize: '1rem' }}>
        
        <button 
          className="nav-btn"
          onClick={() => setView('home')}
          style={{ color: currentView === 'home' ? '#efa748' : '#161311', borderBottom: currentView === 'home' ? '2px solid #efa748' : 'none', paddingBottom: '4px', transition: 'color 0.2s' }} 
          onMouseOver={(e) => e.currentTarget.style.color = '#efa748'} 
          onMouseOut={(e) => e.currentTarget.style.color = currentView === 'home' ? '#efa748' : '#161311'}
        >
          {t('nav.home')}
        </button>

        <button 
          className="nav-btn"
          onClick={() => setView('about')}
          style={{ color: currentView === 'about' ? '#efa748' : '#161311', borderBottom: currentView === 'about' ? '2px solid #efa748' : 'none', paddingBottom: '4px', transition: 'color 0.2s' }} 
          onMouseOver={(e) => e.currentTarget.style.color = '#efa748'} 
          onMouseOut={(e) => e.currentTarget.style.color = currentView === 'about' ? '#efa748' : '#161311'}
        >
          {t('nav.about')}
        </button>

        <button 
          className="nav-btn"
          onClick={() => setView('dashboard')}
          style={{ color: currentView === 'dashboard' ? '#efa748' : '#161311', borderBottom: currentView === 'dashboard' ? '2px solid #efa748' : 'none', paddingBottom: '4px', transition: 'color 0.2s' }} 
          onMouseOver={(e) => e.currentTarget.style.color = '#efa748'} 
          onMouseOut={(e) => e.currentTarget.style.color = currentView === 'dashboard' ? '#efa748' : '#161311'}
        >
          {t('nav.dashboard')}
        </button>

        <button 
          className="nav-btn"
          onClick={() => setView('profile')}
          style={{ color: currentView === 'profile' ? '#efa748' : '#161311', borderBottom: currentView === 'profile' ? '2px solid #efa748' : 'none', paddingBottom: '4px', transition: 'color 0.2s' }} 
          onMouseOver={(e) => e.currentTarget.style.color = '#efa748'} 
          onMouseOut={(e) => e.currentTarget.style.color = currentView === 'profile' ? '#efa748' : '#161311'}
        >
          {t('nav.profile')}
        </button>

        {/* ¡Buscador redundante eliminado de aquí! */}

      </div>
    </nav>
  );
}