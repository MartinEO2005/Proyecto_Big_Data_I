import React, { useState, useEffect, useRef } from 'react';
import NavBar from '../components/layout/NavBar';
import { User } from 'lucide-react';
import { useTranslation } from 'react-i18next'; // <-- 1. Importamos la librería
import logo from '../assets/logo.png'; 
import logoNoaa from '../assets/logo-noaa.png';
import logoOsm from '../assets/logo-osm.png';
import logoIne from '../assets/logo-ine.png';
import espanaNoche from '../assets/espana-noche2.jpg'; 
import logoUem from '../assets/logo-uem.png'; 

// --- COMPONENTE MÁGICO PARA ANIMAR NÚMEROS ---
const AnimatedNumber = ({ target, duration = 2000, decimals = 0, useComma = false }) => {
  const [count, setCount] = useState(0);
  const [isVisible, setIsVisible] = useState(false);
  const domRef = useRef();

  useEffect(() => {
    const observer = new IntersectionObserver(entries => {
      if (entries[0].isIntersecting) {
        setIsVisible(true);
        observer.unobserve(domRef.current); 
      }
    });
    if (domRef.current) observer.observe(domRef.current);
    return () => observer.disconnect();
  }, []);

  useEffect(() => {
    if (!isVisible) return;
    let startTime = null;
    const step = (timestamp) => {
      if (!startTime) startTime = timestamp;
      const progress = Math.min((timestamp - startTime) / duration, 1);
      const easeOut = 1 - Math.pow(1 - progress, 3); 
      setCount(easeOut * target);

      if (progress < 1) {
        window.requestAnimationFrame(step);
      } else {
        setCount(target);
      }
    };
    window.requestAnimationFrame(step);
  }, [target, duration, isVisible]);

  const value = count.toFixed(decimals);
  return <span ref={domRef}>{useComma ? Number(value).toLocaleString('en-US') : value}</span>;
};

export default function About({ currentView, setView }) {
  const { t } = useTranslation(); // <-- 2. Activamos la traducción

  return (
    <div style={{ backgroundColor: '#fcfcfc', minHeight: '100vh', color: '#161311', fontFamily: '"Segoe UI", Roboto, Helvetica, Arial, sans-serif' }}>
      
      <NavBar currentView={currentView} setView={setView} />

      <main style={{ paddingBottom: '0' }}>
        
        <section style={{ maxWidth: '1000px', margin: '0 auto', padding: '80px 30px', display: 'flex', alignItems: 'flex-start', gap: '60px' }}>
          
          <div style={{ width: '300px', flexShrink: 0, marginTop: '15px' }}>
            <img src={logo} alt="GeoLúmica Logo Símbolo" style={{ width: '100%', height: '180px', objectFit: 'contain' }} />
          </div>

          <div style={{ flex: 1 }}>
            <h1 style={{ fontSize: '4.5rem', fontWeight: '900', letterSpacing: '-2px', lineHeight: '1', marginBottom: '35px', textTransform: 'uppercase' }}>
              {t('about.heroTitle1')}<br/>{t('about.heroTitle2')}
            </h1>
            
            <div style={{ fontSize: '1.05rem', lineHeight: '1.8', color: '#555', maxWidth: '700px', textAlign: 'justify' }}>
              <p style={{ marginBottom: '25px' }}>
                {t('about.heroP1')}
              </p>
              <p>
                {t('about.heroP2_1')}<span style={{ color: '#efa748', fontSize: '1.15rem', fontWeight: '600' }}>{t('about.heroP2_2')}</span>{t('about.heroP2_3')}<span style={{ color: '#96551f', fontSize: '1.15rem', fontWeight: '600' }}>{t('about.heroP2_4')}</span>{t('about.heroP2_5')}
              </p>
            </div>
          </div>
        </section>

        <section style={{ width: '100%', height: '55vh', position: 'relative', overflow: 'hidden', margin: '20px 0 80px 0' }}>
          <img 
            src={espanaNoche} 
            alt="Península Ibérica de Noche" 
            style={{ width: '100%', height: '100%', objectFit: 'cover', objectPosition: 'center' }} 
          />
        </section>

        <section style={{ maxWidth: '900px', margin: '0 auto', padding: '0 30px 90px 30px', display: 'flex', gap: '40px', alignItems: 'center' }}>
          <div style={{ fontSize: '4.5rem', color: '#efa748', lineHeight: '0.5', fontFamily: 'serif' }}>“</div>
          <div>
            <h2 style={{ fontSize: '2.2rem', fontWeight: '300', fontStyle: 'italic', lineHeight: '1.3', color: '#161311', marginBottom: '15px' }}>
              {t('about.quote1')}<span style={{ color: '#efa748' }}>{t('about.quote2')}</span>{t('about.quote3')}
            </h2>
            <p style={{ fontSize: '0.95rem', color: '#888', textTransform: 'uppercase', letterSpacing: '1px' }}>{t('about.quoteAuthor')}</p>
          </div>
        </section>

        <section style={{ backgroundColor: '#ffffff', padding: '90px 30px', borderTop: '1px solid #eee', borderBottom: '1px solid #eee' }}>
          <div style={{ maxWidth: '1000px', margin: '0 auto' }}>
            
            <h1 style={{ fontSize: '3.5rem', fontWeight: '900', letterSpacing: '-1.5px', marginBottom: '70px', textTransform: 'uppercase', textAlign: 'center' }}>
              {t('about.dataTitle')}
            </h1>

            <div style={{ display: 'grid', gridTemplateColumns: 'repeat(4, 1fr)', gap: '20px', marginBottom: '90px', textAlign: 'center' }}>
              <div>
                <p style={{ fontSize: '3.5rem', fontWeight: '900', color: '#161311', lineHeight: '1' }}>
                  <AnimatedNumber target={52} duration={2000} />
                </p>
                <p style={{ fontSize: '0.85rem', color: '#888', textTransform: 'uppercase', letterSpacing: '1px', marginTop: '10px' }}>{t('about.dataProvinces')}</p>
              </div>
              <div>
                <p style={{ fontSize: '3.5rem', fontWeight: '900', color: '#d88a24', lineHeight: '1' }}>
                  <AnimatedNumber target={8131} duration={2500} useComma={true} />
                </p>
                <p style={{ fontSize: '0.85rem', color: '#888', textTransform: 'uppercase', letterSpacing: '1px', marginTop: '10px' }}>{t('about.dataMunicipalities')}</p>
              </div>
              <div>
                <p style={{ fontSize: '3.5rem', fontWeight: '900', color: '#161311', lineHeight: '1' }}>
                  <AnimatedNumber target={4} duration={1500} />
                </p>
                <p style={{ fontSize: '0.85rem', color: '#888', textTransform: 'uppercase', letterSpacing: '1px', marginTop: '10px' }}>{t('about.dataPillars')}</p>
              </div>
              <div>
                <p style={{ fontSize: '3.5rem', fontWeight: '900', color: '#161311', lineHeight: '1' }}>
                  <AnimatedNumber target={99.9} duration={2000} decimals={1} />
                  <span style={{ fontSize: '2rem', color: '#efa748' }}>%</span>
                </p>
                <p style={{ fontSize: '0.85rem', color: '#888', textTransform: 'uppercase', letterSpacing: '1px', marginTop: '10px' }}>{t('about.dataCoverage')}</p>
              </div>
            </div>

            <div style={{ display: 'grid', gridTemplateColumns: 'repeat(3, 1fr)', gap: '40px', padding: '0 20px' }}>
              <div style={{ display: 'flex', flexDirection: 'column', alignItems: 'center', textAlign: 'center' }}>
                <img src={logoNoaa} alt="NOAA" style={{ height: '90px', marginBottom: '25px', objectFit: 'contain' }} />
                <p style={{ fontSize: '0.95rem', color: '#666', lineHeight: '1.7' }}>
                  {t('about.dataDesc1')}
                </p>
              </div>
              <div style={{ display: 'flex', flexDirection: 'column', alignItems: 'center', textAlign: 'center' }}>
                <img src={logoOsm} alt="OSM" style={{ height: '85px', marginBottom: '25px', objectFit: 'contain' }} />
                <p style={{ fontSize: '0.95rem', color: '#666', lineHeight: '1.7' }}>
                  {t('about.dataDesc2')}
                </p>
              </div>
              <div style={{ display: 'flex', flexDirection: 'column', alignItems: 'center', textAlign: 'center' }}>
                <img src={logoIne} alt="INE" style={{ height: '85px', marginBottom: '25px', objectFit: 'contain' }} />
                <p style={{ fontSize: '0.95rem', color: '#666', lineHeight: '1.7' }}>
                  {t('about.dataDesc3')}
                </p>
              </div>
            </div>

          </div>
        </section>

        <section style={{ maxWidth: '1000px', margin: '0 auto', padding: '90px 30px 40px 30px' }}>
          
          <div style={{ textAlign: 'center', marginBottom: '70px' }}>
            <h1 style={{ fontSize: '3.5rem', fontWeight: '900', letterSpacing: '-1.5px', marginBottom: '20px', textTransform: 'uppercase' }}>
              {t('about.teamTitle')}
            </h1>
            <p style={{ fontSize: '1.1rem', lineHeight: '1.7', color: '#555', maxWidth: '600px', margin: '0 auto' }}>
              {t('about.teamDesc')}
            </p>
          </div>
          
          <div style={{ display: 'grid', gridTemplateColumns: 'repeat(3, 1fr)', gap: '35px', alignItems: 'start' }}>
            
            <div style={{ backgroundColor: '#ffffff', padding: '30px', borderRadius: '12px', boxShadow: '0 8px 25px rgba(0,0,0,0.06)', textAlign: 'center', border: '2px solid #efa748' }}>
              <div style={{ backgroundColor: '#f5f5f5', border: '3px solid #ededec', height: '120px', width: '120px', borderRadius: '50%', display: 'flex', justifyContent: 'center', alignItems: 'center', margin: '0 auto 20px auto', overflow: 'hidden' }}>
                <User size={48} color="#ccc" />
              </div>
              <h4 style={{ fontWeight: '700', fontSize: '1.2rem', color: '#161311', marginBottom: '8px' }}>Martín Eduardo<br/>Otero Di Lorenzo</h4>
              <p style={{ color: '#efa748', textTransform: 'uppercase', letterSpacing: '1px', fontSize: '0.8rem', fontWeight: '700', lineHeight: '1.6' }}>
                {t('about.roleMartin1')} <br/> {t('about.roleMartin2')}
              </p>
            </div>

            <div style={{ backgroundColor: '#ffffff', padding: '30px', borderRadius: '12px', boxShadow: '0 8px 25px rgba(0,0,0,0.04)', textAlign: 'center', marginTop: '45px', border: '2px solid #161311' }}>
              <div style={{ backgroundColor: '#f5f5f5', border: '3px solid #ededec', height: '120px', width: '120px', borderRadius: '50%', display: 'flex', justifyContent: 'center', alignItems: 'center', margin: '0 auto 20px auto', overflow: 'hidden' }}>
                <User size={48} color="#ccc" />
              </div>
              <h4 style={{ fontWeight: '600', fontSize: '1.2rem', color: '#161311', marginBottom: '8px' }}>Iker Arredondo<br/>Molina</h4>
              <p style={{ color: '#efa748', textTransform: 'uppercase', letterSpacing: '1px', fontSize: '0.8rem', fontWeight: '600' }}>{t('about.roleDS')}</p>
            </div>

            <div style={{ backgroundColor: '#ffffff', padding: '30px', borderRadius: '12px', boxShadow: '0 8px 25px rgba(0,0,0,0.04)', textAlign: 'center', marginTop: '-20px', border: '2px solid #161311' }}>
              <div style={{ backgroundColor: '#f5f5f5', border: '3px solid #ededec', height: '120px', width: '120px', borderRadius: '50%', display: 'flex', justifyContent: 'center', alignItems: 'center', margin: '0 auto 20px auto', overflow: 'hidden' }}>
                <User size={48} color="#ccc" />
              </div>
              <h4 style={{ fontWeight: '600', fontSize: '1.2rem', color: '#161311', marginBottom: '8px' }}>Fernando<br/>Sánchez</h4>
              <p style={{ color: '#efa748', textTransform: 'uppercase', letterSpacing: '1px', fontSize: '0.8rem', fontWeight: '600' }}>{t('about.roleDS')}</p>
            </div>

          </div>
        </section>

      </main>

      <footer style={{ backgroundColor: '#fcfcfc', borderTop: '1px solid #eee', padding: '60px 20px 40px 20px', textAlign: 'center', marginTop: '40px' }}>
        <div style={{ display: 'flex', flexDirection: 'column', alignItems: 'center', gap: '20px' }}>
          <img src={logoUem} alt="Universidad Europea Madrid" style={{ height: '70px', objectFit: 'contain', opacity: '0.9' }} />
          <p style={{ color: '#888', fontSize: '0.9rem', letterSpacing: '0.5px' }}>
            {t('about.footer')}
          </p>
        </div>
      </footer>

    </div>
  );
}