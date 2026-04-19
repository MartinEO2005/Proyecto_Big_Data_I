import React from 'react';
import { Map as MapIcon, Activity, AlertTriangle, TrendingUp, Zap, CheckCircle } from 'lucide-react';
import NavBar from '../components/layout/NavBar';

export default function Dashboard({ currentView, setView }) {
  return (
    <div style={{ backgroundColor: '#ededec', minHeight: '100vh', color: '#161311', fontFamily: '"Inter", "Segoe UI", sans-serif' }}>
      
      <NavBar currentView={currentView} setView={setView} />

      <main style={{ padding: '20px 30px', display: 'flex', flexDirection: 'column', gap: '15px' }}>
        
        <div style={{ display: 'grid', gridTemplateColumns: 'repeat(4, 1fr)', gap: '15px' }}>
          <div style={{ backgroundColor: '#ffffff', padding: '15px', borderRadius: '8px', boxShadow: '0 2px 5px rgba(0,0,0,0.05)', borderTop: '4px solid #efa748' }}>
            <p style={{ color: '#666', fontSize: '0.8rem', marginBottom: '5px', fontWeight: 'bold' }}>Luminosidad Media</p>
            <h2 style={{ fontSize: '1.5rem', display: 'flex', alignItems: 'center', gap: '8px' }}><Zap size={20} color="#efa748"/> 12.3</h2>
          </div>
          <div style={{ backgroundColor: '#ffffff', padding: '15px', borderRadius: '8px', boxShadow: '0 2px 5px rgba(0,0,0,0.05)', borderTop: '4px solid #96551f' }}>
            <p style={{ color: '#666', fontSize: '0.8rem', marginBottom: '5px', fontWeight: 'bold' }}>Densidad Transporte</p>
            <h2 style={{ fontSize: '1.5rem', display: 'flex', alignItems: 'center', gap: '8px' }}><Activity size={20} color="#96551f"/> 500</h2>
          </div>
          <div style={{ backgroundColor: '#e8f5e9', padding: '15px', borderRadius: '8px', boxShadow: '0 2px 5px rgba(0,0,0,0.05)', borderTop: '4px solid #4caf50' }}>
            <p style={{ color: '#2e7d32', fontSize: '0.8rem', marginBottom: '5px', fontWeight: 'bold' }}>Proyección Poblacional</p>
            <h2 style={{ fontSize: '1.5rem', color: '#2e7d32', display: 'flex', alignItems: 'center', gap: '8px' }}><TrendingUp size={20}/> +2.5%</h2>
          </div>
          <div style={{ backgroundColor: '#ffebee', padding: '15px', borderRadius: '8px', boxShadow: '0 2px 5px rgba(0,0,0,0.05)', borderTop: '4px solid #f44336' }}>
            <p style={{ color: '#c62828', fontSize: '0.8rem', marginBottom: '5px', fontWeight: 'bold' }}>Alertas Activas</p>
            <h2 style={{ fontSize: '1.5rem', color: '#c62828', display: 'flex', alignItems: 'center', gap: '8px' }}><AlertTriangle size={20}/> 3</h2>
          </div>
        </div>

        <div style={{ display: 'grid', gridTemplateColumns: '2fr 1fr', gap: '15px', minHeight: '450px' }}>
          <div style={{ backgroundColor: '#ffffff', borderRadius: '8px', boxShadow: '0 2px 5px rgba(0,0,0,0.05)', display: 'flex', flexDirection: 'column' }}>
            <div style={{ padding: '12px 15px', borderBottom: '1px solid #eee', fontWeight: 'bold', fontSize: '0.9rem' }}>
              Widget de Mapa (España - Clustering Municipal)
            </div>
            <div style={{ flex: 1, backgroundColor: '#f5f5f5', display: 'flex', justifyContent: 'center', alignItems: 'center', borderBottomLeftRadius: '8px', borderBottomRightRadius: '8px' }}>
              <div style={{ textAlign: 'center', color: '#999' }}>
                <MapIcon size={48} style={{ margin: '0 auto 10px auto' }} />
                <p style={{ fontSize: '0.9rem' }}>El renderizado del mapa (Mapbox/Deck.gl) irá aquí</p>
              </div>
            </div>
          </div>

          <div style={{ backgroundColor: '#ffffff', borderRadius: '8px', boxShadow: '0 2px 5px rgba(0,0,0,0.05)', display: 'flex', flexDirection: 'column' }}>
            <div style={{ padding: '12px 15px', borderBottom: '1px solid #eee', fontWeight: 'bold', fontSize: '0.9rem' }}>
              Panel de Machine Learning
            </div>
            <div style={{ padding: '15px', display: 'flex', flexDirection: 'column', gap: '15px', flex: 1 }}>
              <div style={{ border: '1px dashed #ccc', flex: 1, borderRadius: '5px', display: 'flex', justifyContent: 'center', alignItems: 'center', color: '#999' }}>
                <p style={{ fontSize: '0.85rem' }}>Gráfico de Crecimiento Económico (PCA)</p>
              </div>
              <div style={{ border: '1px dashed #ccc', flex: 1, borderRadius: '5px', display: 'flex', justifyContent: 'center', alignItems: 'center', color: '#999' }}>
                <p style={{ fontSize: '0.85rem' }}>Matriz de Correlación (DBSCAN Outliers)</p>
              </div>
            </div>
          </div>
        </div>

        <div style={{ display: 'grid', gridTemplateColumns: '1fr 2fr', gap: '15px' }}>
          <div style={{ backgroundColor: '#ffffff', padding: '15px', borderRadius: '8px', boxShadow: '0 2px 5px rgba(0,0,0,0.05)' }}>
            <h4 style={{ marginBottom: '12px', fontSize: '0.95rem' }}>Estado del Sistema</h4>
            <p style={{ display: 'flex', alignItems: 'center', gap: '8px', marginBottom: '8px', fontSize: '0.85rem' }}><CheckCircle size={16} color="#4caf50" /> API Satélite: Online</p>
            <p style={{ display: 'flex', alignItems: 'center', gap: '8px', marginBottom: '8px', fontSize: '0.85rem' }}><CheckCircle size={16} color="#4caf50" /> API Transporte: Online</p>
            <p style={{ display: 'flex', alignItems: 'center', gap: '8px', fontSize: '0.85rem' }}><CheckCircle size={16} color="#4caf50" /> Motor Predicciones: Online</p>
          </div>

          <div style={{ backgroundColor: '#ffffff', padding: '15px', borderRadius: '8px', boxShadow: '0 2px 5px rgba(0,0,0,0.05)' }}>
            <h4 style={{ marginBottom: '12px', fontSize: '0.95rem' }}>Alertas Recientes (Detección de Anomalías)</h4>
            <p style={{ display: 'flex', alignItems: 'center', gap: '8px', color: '#c62828', marginBottom: '8px', fontSize: '0.85rem' }}><AlertTriangle size={16} /> Caída poblacional crítica detectada en Zona A (2026-03-28)</p>
            <p style={{ display: 'flex', alignItems: 'center', gap: '8px', color: '#efa748', marginBottom: '8px', fontSize: '0.85rem' }}><AlertTriangle size={16} /> Declive económico proyectado en Región X (2026-03-25)</p>
            <p style={{ display: 'flex', alignItems: 'center', gap: '8px', color: '#c62828', fontSize: '0.85rem' }}><AlertTriangle size={16} /> Disminución severa de intensidad lumínica en Municipio Y (2026-03-24)</p>
          </div>
        </div>
      </main>

    </div>
  );
}