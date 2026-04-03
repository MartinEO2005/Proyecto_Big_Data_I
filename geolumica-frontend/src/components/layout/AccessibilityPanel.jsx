import React, { useState, useEffect } from 'react';
import { 
  Accessibility, X, Volume2, Contrast, 
  Type, PauseCircle, Globe 
} from 'lucide-react';
import { useTranslation } from 'react-i18next'; // <-- 1. IMPORTAMOS EL HOOK DE IDIOMAS

export default function AccessibilityPanel() {
  const { t, i18n } = useTranslation(); // <-- 2. ACTIVAMOS LA TRADUCCIÓN

  const [isOpen, setIsOpen] = useState(false);
  const [fontSize, setFontSize] = useState(100);
  
  const [highContrast, setHighContrast] = useState(false);
  const [dyslexiaMode, setDyslexiaMode] = useState(false);
  const [animationsPaused, setAnimationsPaused] = useState(false);

  useEffect(() => {
    document.documentElement.style.fontSize = `${fontSize}%`;
  }, [fontSize]);

  useEffect(() => {
    highContrast ? document.body.classList.add('high-contrast') : document.body.classList.remove('high-contrast');
  }, [highContrast]);

  useEffect(() => {
    dyslexiaMode ? document.body.classList.add('dyslexia-mode') : document.body.classList.remove('dyslexia-mode');
  }, [dyslexiaMode]);

  useEffect(() => {
    animationsPaused ? document.body.classList.add('pause-animations') : document.body.classList.remove('pause-animations');
  }, [animationsPaused]);

  const handleIncreaseFont = () => setFontSize(prev => Math.min(prev + 10, 150));
  const handleDecreaseFont = () => setFontSize(prev => Math.max(prev - 10, 80));

  // --- 3. LA FUNCIÓN QUE CAMBIA EL IDIOMA REALMENTE ---
  const toggleLanguage = () => {
    // Si el idioma actual es español, pasamos a inglés, y viceversa
    const nextLang = i18n.language === 'es' ? 'en' : 'es';
    i18n.changeLanguage(nextLang);
  };

  return (
    <>
      <button
        className="access-fab"
        onClick={() => setIsOpen(!isOpen)}
        aria-label={isOpen ? "Cerrar panel de accesibilidad" : "Abrir panel de accesibilidad"}
        aria-expanded={isOpen}
        style={{
          position: 'fixed',
          bottom: '30px',
          right: '30px',
          backgroundColor: '#efa748',
          color: '#161311',
          border: 'none',
          borderRadius: '50%',
          width: '60px',
          height: '60px',
          display: 'flex',
          justifyContent: 'center',
          alignItems: 'center',
          boxShadow: '0 4px 15px rgba(0,0,0,0.2)',
          cursor: 'pointer',
          zIndex: 9999,
          transition: 'transform 0.2s'
        }}
        onMouseOver={(e) => e.currentTarget.style.transform = 'scale(1.1)'}
        onMouseOut={(e) => e.currentTarget.style.transform = 'scale(1)'}
      >
        <Accessibility size={30} />
      </button>

      {isOpen && (
        <div style={{
          position: 'fixed',
          bottom: '100px',
          right: '30px',
          width: '320px',
          backgroundColor: '#fcfcfc',
          borderRadius: '12px',
          boxShadow: '0 10px 40px rgba(0,0,0,0.2)',
          border: '1px solid #eee',
          zIndex: 10000,
          padding: '20px',
          display: 'flex',
          flexDirection: 'column',
          fontFamily: '"Segoe UI", Roboto, Helvetica, Arial, sans-serif'
        }}>
          
          <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '20px', borderBottom: '2px solid #eee', paddingBottom: '10px' }}>
            <h3 style={{ fontSize: '1.2rem', fontWeight: '800', color: '#161311', margin: 0 }}>
              {t('accessibility.title')}
            </h3>
            <button 
              className="access-close"
              onClick={() => setIsOpen(false)}
              aria-label="Cerrar panel"
              style={{ background: 'transparent', border: 'none', cursor: 'pointer', color: '#888' }}
            >
              <X size={24} />
            </button>
          </div>

          <button className="access-btn" aria-label="Activar lector de pantalla">
            <span>{t('accessibility.screenReader')}</span>
            <Volume2 size={20} color="#161311" />
          </button>

          <button 
            className={`access-btn ${highContrast ? 'active' : ''}`}
            onClick={() => setHighContrast(!highContrast)}
            aria-pressed={highContrast}
          >
            <span>{t('accessibility.highContrast')}</span>
            <Contrast size={20} color={highContrast ? "#efa748" : "#161311"} />
          </button>

          <div className="access-btn" style={{ cursor: 'default' }}>
            <span>{t('accessibility.fontResizer')}</span>
            <div style={{ display: 'flex', alignItems: 'center', gap: '10px' }}>
              <button className="font-control-btn" onClick={handleDecreaseFont} aria-label="Reducir tamaño de letra">A-</button>
              <span style={{ fontSize: '0.85rem' }}>{fontSize}%</span>
              <button className="font-control-btn" onClick={handleIncreaseFont} aria-label="Aumentar tamaño de letra">A+</button>
            </div>
          </div>

          <button 
            className={`access-btn ${dyslexiaMode ? 'active' : ''}`}
            onClick={() => setDyslexiaMode(!dyslexiaMode)}
            aria-pressed={dyslexiaMode}
          >
            <span>{t('accessibility.dyslexiaFont')}</span>
            <Type size={20} color={dyslexiaMode ? "#efa748" : "#161311"} />
          </button>

          <button 
            className={`access-btn ${animationsPaused ? 'active' : ''}`}
            onClick={() => setAnimationsPaused(!animationsPaused)}
            aria-pressed={animationsPaused}
          >
            <span>{t('accessibility.pauseAnimations')}</span>
            <PauseCircle size={20} color={animationsPaused ? "#efa748" : "#161311"} />
          </button>

          <div style={{ margin: '15px 0', borderBottom: '1px solid #eee' }}></div>

          {/* --- 4. EL BOTÓN MÁGICO CON SU ONCLICK --- */}
          <button 
            className="access-btn" 
            onClick={toggleLanguage}
            aria-label="Cambiar idioma"
          >
            {/* Mostramos el texto del JSON + el código del idioma actual (ES o EN) */}
            <span>{t('accessibility.language')} ({i18n.language.toUpperCase()})</span>
            <Globe size={20} color="#161311" />
          </button>

        </div>
      )}
    </>
  );
}