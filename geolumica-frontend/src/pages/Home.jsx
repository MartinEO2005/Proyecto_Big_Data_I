import React from 'react';
import { Satellite, Route, TrendingUp, ChevronRight } from 'lucide-react';
import NavBar from '../components/layout/NavBar';
import espanaNoche from '../assets/espana-noche.jpg';
import { useTranslation } from 'react-i18next'; // <-- 1. Importamos la librería de idiomas

export default function Home({ currentView, setView }) {
  const { t } = useTranslation(); // <-- 2. Activamos la función de traducción

  return (
    <div style={{ backgroundColor: '#fcfcfc', minHeight: '100vh', color: '#161311', fontFamily: '"Inter", "Segoe UI", Roboto, Helvetica, Arial, sans-serif' }}>
      
      <NavBar currentView={currentView} setView={setView} />

      {/* SECCIÓN HERO CON IMAGEN DE FONDO */}
      <main style={{ 
        position: 'relative',
        backgroundImage: `linear-gradient(to bottom, rgba(22, 19, 17, 0.85), rgba(22, 19, 17, 0.95)), url(${espanaNoche})`,
        backgroundSize: 'cover',
        backgroundPosition: 'center',
        color: '#ffffff', 
        textAlign: 'center', 
        padding: '120px 20px 180px 20px',
        borderBottom: '4px solid #efa748'
      }}>
        <div style={{ maxWidth: '900px', margin: '0 auto' }}>
          
          <div style={{ display: 'inline-block', backgroundColor: 'rgba(239, 167, 72, 0.1)', border: '1px solid rgba(239, 167, 72, 0.3)', padding: '8px 16px', borderRadius: '20px', color: '#efa748', fontWeight: 'bold', fontSize: '0.85rem', letterSpacing: '1px', marginBottom: '25px', textTransform: 'uppercase' }}>
            {t('home.badge')} {/* <-- TEXTO TRADUCIDO */}
          </div>
          
          <h1 style={{ fontSize: '4.5rem', marginBottom: '25px', letterSpacing: '-1.5px', fontWeight: '900', lineHeight: '1.1' }}>
            {t('home.title')} {/* <-- TEXTO TRADUCIDO */}
          </h1>
          
          <p style={{ fontSize: '1.25rem', marginBottom: '50px', color: '#d1d1d1', lineHeight: '1.7', maxWidth: '800px', margin: '0 auto 50px auto' }}>
            {t('home.subtitle')} {/* <-- TEXTO TRADUCIDO */}
          </p>
          
          <button 
            onClick={() => setView('dashboard')}
            style={{
              display: 'inline-flex',
              alignItems: 'center',
              gap: '10px',
              backgroundColor: '#efa748',
              color: '#161311',
              border: 'none',
              padding: '18px 45px',
              fontSize: '1.15rem',
              fontWeight: 'bold',
              borderRadius: '8px',
              cursor: 'pointer',
              boxShadow: '0 8px 25px rgba(239, 167, 72, 0.3)',
              transition: 'all 0.2s ease-in-out'
            }}
            onMouseOver={(e) => {
              e.currentTarget.style.transform = 'translateY(-3px)';
              e.currentTarget.style.boxShadow = '0 12px 30px rgba(239, 167, 72, 0.4)';
            }}
            onMouseOut={(e) => {
              e.currentTarget.style.transform = 'translateY(0)';
              e.currentTarget.style.boxShadow = '0 8px 25px rgba(239, 167, 72, 0.3)';
            }}
          >
            {t('home.openDashboard')} <ChevronRight size={20} /> {/* <-- TEXTO TRADUCIDO */}
          </button>
        </div>
      </main>

      {/* LOS 3 PILARES INFERIORES */}
      <section style={{ 
        display: 'flex', 
        justifyContent: 'center', 
        gap: '35px', 
        padding: '0 40px', 
        marginTop: '-90px', 
        position: 'relative', 
        zIndex: 10,
        paddingBottom: '100px'
      }}>
        {/* Pilar 1 */}
        <div 
          style={{ backgroundColor: '#ffffff', padding: '45px 35px', borderRadius: '16px', width: '30%', boxShadow: '0 15px 40px rgba(0,0,0,0.08)', textAlign: 'center', transition: 'transform 0.3s ease', borderTop: '4px solid #efa748' }}
          onMouseOver={(e) => e.currentTarget.style.transform = 'translateY(-10px)'}
          onMouseOut={(e) => e.currentTarget.style.transform = 'translateY(0)'}
        >
          <div style={{ backgroundColor: '#fff5e6', width: '85px', height: '85px', borderRadius: '50%', display: 'flex', justifyContent: 'center', alignItems: 'center', margin: '0 auto 25px auto' }}>
            <Satellite size={44} color="#efa748" />
          </div>
          <h3 style={{ color: '#161311', marginBottom: '15px', fontSize: '1.4rem', fontWeight: '800' }}>{t('home.pillar1.title')}</h3>
          <p style={{ fontSize: '1rem', color: '#666', lineHeight: '1.7' }}>{t('home.pillar1.desc')}</p>
        </div>

        {/* Pilar 2 */}
        <div 
          style={{ backgroundColor: '#ffffff', padding: '45px 35px', borderRadius: '16px', width: '30%', boxShadow: '0 15px 40px rgba(0,0,0,0.08)', textAlign: 'center', transition: 'transform 0.3s ease', borderTop: '4px solid #96551f' }}
          onMouseOver={(e) => e.currentTarget.style.transform = 'translateY(-10px)'}
          onMouseOut={(e) => e.currentTarget.style.transform = 'translateY(0)'}
        >
          <div style={{ backgroundColor: '#fdf4ec', width: '85px', height: '85px', borderRadius: '50%', display: 'flex', justifyContent: 'center', alignItems: 'center', margin: '0 auto 25px auto' }}>
            <Route size={44} color="#96551f" />
          </div>
          <h3 style={{ color: '#161311', marginBottom: '15px', fontSize: '1.4rem', fontWeight: '800' }}>{t('home.pillar2.title')}</h3>
          <p style={{ fontSize: '1rem', color: '#666', lineHeight: '1.7' }}>{t('home.pillar2.desc')}</p>
        </div>

        {/* Pilar 3 */}
        <div 
          style={{ backgroundColor: '#ffffff', padding: '45px 35px', borderRadius: '16px', width: '30%', boxShadow: '0 15px 40px rgba(0,0,0,0.08)', textAlign: 'center', transition: 'transform 0.3s ease', borderTop: '4px solid #efa748' }}
          onMouseOver={(e) => e.currentTarget.style.transform = 'translateY(-10px)'}
          onMouseOut={(e) => e.currentTarget.style.transform = 'translateY(0)'}
        >
          <div style={{ backgroundColor: '#fff5e6', width: '85px', height: '85px', borderRadius: '50%', display: 'flex', justifyContent: 'center', alignItems: 'center', margin: '0 auto 25px auto' }}>
            <TrendingUp size={44} color="#efa748" />
          </div>
          <h3 style={{ color: '#161311', marginBottom: '15px', fontSize: '1.4rem', fontWeight: '800' }}>{t('home.pillar3.title')}</h3>
          <p style={{ fontSize: '1rem', color: '#666', lineHeight: '1.7' }}>{t('home.pillar3.desc')}</p>
        </div>
      </section>

    </div>
  );
}