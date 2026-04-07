import React, { useState, useEffect } from 'react';
import { 
  Accessibility, X, Volume2, Contrast, 
  Type, PauseCircle, Globe 
} from 'lucide-react';
import { useTranslation } from 'react-i18next';

export default function AccessibilityPanel() {
  const { t, i18n } = useTranslation();

  const [isOpen, setIsOpen] = useState(false);
  const [fontSize, setFontSize] = useState(100);
  
  const [highContrast, setHighContrast] = useState(false);
  const [dyslexiaMode, setDyslexiaMode] = useState(false);
  const [animationsPaused, setAnimationsPaused] = useState(false);
  
  // 1. NUEVO ESTADO PARA EL MODO LECTOR DE PANTALLA
  const [screenReader, setScreenReader] = useState(false);

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

// 2. EL MOTOR DE VOZ (LA MAGIA DE LA OPCIÓN B)
  useEffect(() => {
    // Si se apaga, callamos al robot inmediatamente
    if (!screenReader) {
      window.speechSynthesis.cancel();
      return;
    }

    let timeoutId; // Variable para controlar el retraso del audio

    // Función que hace hablar al navegador
    const speak = (text) => {
      if (!text || text.trim() === '') return;
      
      // 1. MANDAMOS CALLAR AL ROBOT INMEDIATAMENTE
      window.speechSynthesis.cancel(); 
      
      // 2. TRUCO: Esperamos 50ms para que el navegador limpie la memoria de audio
      clearTimeout(timeoutId);
      timeoutId = setTimeout(() => {
        const utterance = new SpeechSynthesisUtterance(text);
        utterance.lang = i18n.language === 'es' ? 'es-ES' : 'en-US';
        
        // 3. SUBIMOS LA VELOCIDAD: De 1.0 a 1.15 para que no se haga tan pesado en textos largos
        utterance.rate = 1.15; 
        
        window.speechSynthesis.speak(utterance);
      }, 50);
    };

    // El "Vigilante": detecta dónde pones el ratón o el foco del teclado
    const handleEvent = (e) => {
      const target = e.target;
      
      if (target.tagName.match(/^(BUTTON|A|H1|H2|H3|P|SPAN|IMG|INPUT)$/)) {
        let textToRead = target.getAttribute('aria-label') || target.alt || target.innerText || target.placeholder;
        speak(textToRead);
      }
    };

    document.body.addEventListener('focus', handleEvent, true); 
    document.body.addEventListener('mouseenter', handleEvent, true);

    return () => {
      document.body.removeEventListener('focus', handleEvent, true);
      document.body.removeEventListener('mouseenter', handleEvent, true);
      window.speechSynthesis.cancel();
      clearTimeout(timeoutId); // Limpiamos el temporizador al salir
    };
  }, [screenReader, i18n.language]);

  const handleIncreaseFont = () => setFontSize(prev => Math.min(prev + 10, 150));
  const handleDecreaseFont = () => setFontSize(prev => Math.max(prev - 10, 80));

  const toggleLanguage = () => {
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

          {/* 3. AÑADIMOS EL ONCLICK AL BOTÓN DEL LECTOR Y SU CLASE ACTIVE */}
          <button 
            className={`access-btn ${screenReader ? 'active' : ''}`} 
            onClick={() => setScreenReader(!screenReader)}
            aria-pressed={screenReader}
            aria-label={t('accessibility.screenReader')}
          >
            <span>{t('accessibility.screenReader')}</span>
            <Volume2 size={20} color={screenReader ? "#efa748" : "#161311"} />
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

          <button 
            className="access-btn" 
            onClick={toggleLanguage}
            aria-label="Cambiar idioma"
          >
            <span>{t('accessibility.language')} ({i18n.language.toUpperCase()})</span>
            <Globe size={20} color="#161311" />
          </button>

        </div>
      )}
    </>
  );
}