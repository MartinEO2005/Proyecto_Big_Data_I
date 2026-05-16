// layout/NavBar.jsx
import React from 'react';
import logo from '../../assets/logo.png';
import { useTranslation } from 'react-i18next';
import { Activity } from 'lucide-react';

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
      height: '75px',
      position: 'relative',
      zIndex: 50
    }}>
      {/* EL LOGO */}
      <button 
        className="nav-logo-btn"
        onClick={() => setView('home')}
        style={{ display: 'flex', alignItems: 'center', background: 'none', border: 'none', cursor: 'pointer' }}
      >
        <img src={logo} alt="GeoLúmica Logo" style={{ height: '60px', objectFit: 'contain' }} />
      </button>

      {/* ENLACES DE NAVEGACIÓN */}
      <div style={{ display: 'flex', gap: '35px', alignItems: 'center', fontWeight: '600', fontSize: '1rem' }}>
        
        <button 
          className="nav-btn" onClick={() => setView('home')}
          style={{ color: currentView === 'home' ? '#efa748' : '#161311', transition: 'color 0.2s', background: 'none', border: 'none', cursor: 'pointer', borderBottom: currentView === 'home' ? '2px solid #efa748' : 'none', paddingBottom: '4px' }}
        >
          {t('nav.home')}
        </button>

        <button 
          className="nav-btn" onClick={() => setView('about')}
          style={{ color: currentView === 'about' ? '#efa748' : '#161311', transition: 'color 0.2s', background: 'none', border: 'none', cursor: 'pointer', borderBottom: currentView === 'about' ? '2px solid #efa748' : 'none', paddingBottom: '4px' }}
        >
          {t('nav.about')}
        </button>

        <button 
          className="nav-btn" onClick={() => setView('dashboard')}
          style={{ color: currentView === 'dashboard' ? '#efa748' : '#161311', transition: 'color 0.2s', background: 'none', border: 'none', cursor: 'pointer', borderBottom: currentView === 'dashboard' ? '2px solid #efa748' : 'none', paddingBottom: '4px' }}
        >
          {t('nav.dashboard')}
        </button>

        <button 
          className="nav-btn" onClick={() => setView('profile')}
          style={{ color: currentView === 'profile' ? '#efa748' : '#161311', transition: 'color 0.2s', background: 'none', border: 'none', cursor: 'pointer', borderBottom: currentView === 'profile' ? '2px solid #efa748' : 'none', paddingBottom: '4px' }}
        >
          {t('nav.profile')}
        </button>

        {/* NUEVA PÁGINA: AUDITORÍA DE SISTEMA / HEALTH (Simula estar disponible tras login en Dashboard) */}
        {(currentView === 'dashboard' || currentView === 'status' || currentView === 'profile') && (
          <button 
            onClick={() => setView('status')}
            className="flex items-center gap-1.5 px-3 py-1 rounded-md transition-colors text-[13px] font-black tracking-wider uppercase"
            style={{ 
              color: currentView === 'status' ? '#efa748' : '#03132B',
              backgroundColor: currentView === 'status' ? '#03132B' : 'transparent',
              border: '1px solid #03132B'
            }}
          >
            <Activity size={14} />
            System Status
          </button>
        )}

      </div>
    </nav>
  );
}