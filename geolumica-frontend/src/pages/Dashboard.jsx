import React, { useState } from 'react';
import { 
  Map as MapIcon, 
  Activity, 
  TrendingUp, 
  Zap, 
  BarChart3, 
  LayoutDashboard, 
  ExternalLink,
  PieChart as PieIcon,
  Info
} from 'lucide-react';
import NavBar from '../components/layout/NavBar';

// Componente auxiliar para la gráfica de Feature Importance
const FeatureImportance = () => {
  const features = [
    { name: 'Luz Nocturna (VIIRS)', value: 85, color: '#efa748' },
    { name: 'Conectividad Ferroviaria', value: 65, color: '#96551f' },
    { name: 'Densidad Demográfica', value: 45, color: '#161311' },
    { name: 'Consumo Eléctrico', value: 30, color: '#ccc' },
  ];

  return (
    <div style={{ padding: '15px' }}>
      {features.map((f) => (
        <div key={f.name} style={{ marginBottom: '12px' }}>
          <div style={{ display: 'flex', justifyContent: 'space-between', fontSize: '0.8rem', marginBottom: '4px' }}>
            <span>{f.name}</span>
            <span style={{ fontWeight: 'bold' }}>{f.value}%</span>
          </div>
          <div style={{ backgroundColor: '#eee', height: '8px', borderRadius: '4px', overflow: 'hidden' }}>
            <div style={{ backgroundColor: f.color, width: `${f.value}%`, height: '100%' }}></div>
          </div>
        </div>
      ))}
    </div>
  );
};

export default function Dashboard({ currentView, setView }) {
  // Estado para controlar qué vista mostrar
  const [activeTab, setActiveTab] = useState('react'); // 'react' o 'powerbi'

  return (
    <div style={{ backgroundColor: '#ededec', minHeight: '100vh', color: '#161311', fontFamily: '"Inter", "Segoe UI", sans-serif' }}>
      <NavBar currentView={currentView} setView={setView} />
      
      <main style={{ padding: '20px 30px', display: 'flex', flexDirection: 'column', gap: '20px' }}>
        
        {/* CABECERA Y SELECTOR DE VISTA */}
        <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
          <div>
            <h1 style={{ fontSize: '1.5rem', fontWeight: '900', margin: 0 }}>PANEL GEOESTADÍSTICO</h1>
            <p style={{ fontSize: '0.85rem', color: '#666' }}>Análisis de segmentación municipal y desarrollo económico</p>
          </div>
          
          <div style={{ backgroundColor: '#ffffff', padding: '5px', borderRadius: '10px', boxShadow: '0 2px 5px rgba(0,0,0,0.05)', display: 'flex', gap: '5px' }}>
            <button 
              onClick={() => setActiveTab('react')}
              style={{
                padding: '8px 16px', border: 'none', borderRadius: '6px', cursor: 'pointer', display: 'flex', alignItems: 'center', gap: '8px',
                backgroundColor: activeTab === 'react' ? '#efa748' : 'transparent',
                color: activeTab === 'react' ? '#161311' : '#666',
                fontWeight: '600', transition: 'all 0.2s'
              }}
            >
              <LayoutDashboard size={18} /> GeoLúmica React
            </button>
            <button 
              onClick={() => setActiveTab('powerbi')}
              style={{
                padding: '8px 16px', border: 'none', borderRadius: '6px', cursor: 'pointer', display: 'flex', alignItems: 'center', gap: '8px',
                backgroundColor: activeTab === 'powerbi' ? '#0078d4' : 'transparent',
                color: activeTab === 'powerbi' ? '#ffffff' : '#666',
                fontWeight: '600', transition: 'all 0.2s'
              }}
            >
              <BarChart3 size={18} /> Microsoft Power BI
            </button>
          </div>
        </div>

        {activeTab === 'react' ? (
          /* --- VISTA REACT (PROPIA) --- */
          <>
            <div style={{ display: 'grid', gridTemplateColumns: 'repeat(4, 1fr)', gap: '15px' }}>
              <div style={{ backgroundColor: '#ffffff', padding: '15px', borderRadius: '8px', boxShadow: '0 2px 5px rgba(0,0,0,0.05)', borderTop: '4px solid #efa748' }}>
                <p style={{ color: '#666', fontSize: '0.8rem', marginBottom: '5px', fontWeight: 'bold' }}>Motores Económicos</p>
                <h2 style={{ fontSize: '1.5rem', display: 'flex', alignItems: 'center', gap: '8px' }}><Zap size={20} color="#efa748"/> 1,240 <span style={{ fontSize: '0.7rem', color: '#666' }}>municipios</span></h2>
              </div>
              <div style={{ backgroundColor: '#ffffff', padding: '15px', borderRadius: '8px', boxShadow: '0 2px 5px rgba(0,0,0,0.05)', borderTop: '4px solid #96551f' }}>
                <p style={{ color: '#666', fontSize: '0.8rem', marginBottom: '5px', fontWeight: 'bold' }}>Zonas en Expansión</p>
                <h2 style={{ fontSize: '1.5rem', display: 'flex', alignItems: 'center', gap: '8px' }}><TrendingUp size={20} color="#96551f"/> 842 <span style={{ fontSize: '0.7rem', color: '#666' }}>municipios</span></h2>
              </div>
              <div style={{ backgroundColor: '#ffffff', padding: '15px', borderRadius: '8px', boxShadow: '0 2px 5px rgba(0,0,0,0.05)', borderTop: '4px solid #161311' }}>
                <p style={{ color: '#666', fontSize: '0.8rem', marginBottom: '5px', fontWeight: 'bold' }}>Núcleos Rurales Estancados</p>
                <h2 style={{ fontSize: '1.5rem', display: 'flex', alignItems: 'center', gap: '8px' }}><Activity size={20} color="#161311"/> 5,431 <span style={{ fontSize: '0.7rem', color: '#666' }}>municipios</span></h2>
              </div>
              <div style={{ backgroundColor: '#ffebee', padding: '15px', borderRadius: '8px', boxShadow: '0 2px 5px rgba(0,0,0,0.05)', borderTop: '4px solid #f44336' }}>
                <p style={{ color: '#c62828', fontSize: '0.8rem', marginBottom: '5px', fontWeight: 'bold' }}>Anomalías Detectadas (DBSCAN)</p>
                <h2 style={{ fontSize: '1.5rem', color: '#c62828', display: 'flex', alignItems: 'center', gap: '8px' }}><Info size={20}/> 12</h2>
              </div>
            </div>

            <div style={{ display: 'grid', gridTemplateColumns: '2fr 1fr', gap: '15px', minHeight: '500px' }}>
              {/* Mapa de Clusters */}
              <div style={{ backgroundColor: '#ffffff', borderRadius: '8px', boxShadow: '0 2px 5px rgba(0,0,0,0.05)', display: 'flex', flexDirection: 'column' }}>
                <div style={{ padding: '12px 15px', borderBottom: '1px solid #eee', fontWeight: 'bold', fontSize: '0.9rem', display: 'flex', justifyContent: 'space-between' }}>
                  <span>Mapa de Clusters por LAU_ID (K-Means)</span>
                  <span style={{ fontSize: '0.75rem', color: '#efa748' }}>Capa: Radiancia Lumínica + INE</span>
                </div>
                <div style={{ flex: 1, backgroundColor: '#111', borderRadius: '0 0 8px 8px', position: 'relative', display: 'flex', justifyContent: 'center', alignItems: 'center' }}>
                   {/* Aquí iría el componente de Deck.gl / Mapbox */}
                   <div style={{ textAlign: 'center', color: '#444' }}>
                    <MapIcon size={64} style={{ marginBottom: '10px' }} />
                    <p>Visualización Espacial Activa</p>
                   </div>
                   {/* Leyenda Mock */}
                   <div style={{ position: 'absolute', bottom: '20px', left: '20px', backgroundColor: 'rgba(255,255,255,0.9)', padding: '10px', borderRadius: '5px', fontSize: '0.75rem' }}>
                      <div style={{ display: 'flex', alignItems: 'center', gap: '5px', marginBottom: '5px' }}><div style={{ width: 10, height: 10, backgroundColor: '#efa748', borderRadius: '50%' }}></div> Motor Económico</div>
                      <div style={{ display: 'flex', alignItems: 'center', gap: '5px', marginBottom: '5px' }}><div style={{ width: 10, height: 10, backgroundColor: '#96551f', borderRadius: '50%' }}></div> Residencial Expansión</div>
                      <div style={{ display: 'flex', alignItems: 'center', gap: '5px' }}><div style={{ width: 10, height: 10, backgroundColor: '#161311', borderRadius: '50%' }}></div> Rural Estancado</div>
                   </div>
                </div>
              </div>

              {/* Insights de ML */}
              <div style={{ display: 'flex', flexDirection: 'column', gap: '15px' }}>
                <div style={{ backgroundColor: '#ffffff', borderRadius: '8px', boxShadow: '0 2px 5px rgba(0,0,0,0.05)', padding: '15px', flex: 1 }}>
                  <h4 style={{ margin: '0 0 15px 0', fontSize: '0.9rem', borderBottom: '1px solid #eee', paddingBottom: '10px' }}>Importancia de Características (PCA)</h4>
                  <FeatureImportance />
                </div>
                <div style={{ backgroundColor: '#ffffff', borderRadius: '8px', boxShadow: '0 2px 5px rgba(0,0,0,0.05)', padding: '15px', flex: 1 }}>
                  <h4 style={{ margin: '0 0 15px 0', fontSize: '0.9rem', borderBottom: '1px solid #eee', paddingBottom: '10px' }}>Distribución de Perfiles</h4>
                  <div style={{ display: 'flex', justifyContent: 'center', alignItems: 'center', height: '150px', color: '#999' }}>
                    <PieIcon size={40} />
                    <p style={{ fontSize: '0.8rem', marginLeft: '10px' }}>Distribución de Clusters por Municipios</p>
                  </div>
                </div>
              </div>
            </div>
          </>
        ) : (
          /* --- VISTA POWER BI (EMBEDDED) --- */
          <div style={{ backgroundColor: '#ffffff', borderRadius: '8px', boxShadow: '0 10px 30px rgba(0,0,0,0.1)', height: 'calc(100vh - 200px)', display: 'flex', flexDirection: 'column', overflow: 'hidden' }}>
            <div style={{ padding: '15px', backgroundColor: '#0078d4', color: 'white', display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
              <div style={{ display: 'flex', alignItems: 'center', gap: '10px' }}>
                <BarChart3 size={20} />
                <span style={{ fontWeight: 'bold' }}>GeoLúmica Report - Analytics Pro</span>
              </div>
              <button style={{ backgroundColor: 'rgba(255,255,255,0.2)', border: 'none', color: 'white', padding: '5px 10px', borderRadius: '4px', cursor: 'pointer', display: 'flex', alignItems: 'center', gap: '5px', fontSize: '0.8rem' }}>
                Abrir en Power BI <ExternalLink size={14} />
              </button>
            </div>
            <div style={{ flex: 1, backgroundColor: '#f0f0f0', display: 'flex', justifyContent: 'center', alignItems: 'center' }}>
              {/* Aquí es donde se pega el iframe de Power BI */}
              <div style={{ textAlign: 'center', color: '#666', maxWidth: '400px' }}>
                <div style={{ backgroundColor: '#ddd', width: '80px', height: '80px', borderRadius: '50%', display: 'flex', justifyContent: 'center', alignItems: 'center', margin: '0 auto 20px auto' }}>
                  <BarChart3 size={40} color="#0078d4" />
                </div>
                <h3>Contenedor de Power BI</h3>
                <p style={{ fontSize: '0.9rem' }}>Pega aquí el código <code>iframe</code> de "Publicar en la web" o usa el SDK de <code>powerbi-client-react</code> para una integración profesional con tokens.</p>
              </div>
            </div>
          </div>
        )}
      </main>
    </div>
  );
}