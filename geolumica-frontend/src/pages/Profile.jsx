import React from 'react';
import NavBar from '../components/layout/NavBar';
import { 
  User, Mail, Briefcase, Map, Bookmark, 
  Shield, LogOut, ChevronRight, Zap, Building, Lock, FileText
} from 'lucide-react';
// IMPORTAMOS EL MAPA COLORIDO
import mapaColorido from '../assets/mapa-colorido.png';

export default function Profile({ currentView, setView }) {
  return (
    <div style={{ backgroundColor: '#fcfcfc', minHeight: '100vh', color: '#161311', fontFamily: '"Segoe UI", Roboto, Helvetica, Arial, sans-serif' }}>
      
      <NavBar currentView={currentView} setView={setView} />

      <main style={{ paddingBottom: '100px' }}>
        
        {/* BANNER SUPERIOR CON EL MAPA COLORIDO */}
        <div style={{ 
          height: '220px', 
          position: 'relative',
          backgroundImage: `linear-gradient(to right, rgba(22, 19, 17, 0.6), rgba(22, 19, 17, 0.8)), url(${mapaColorido})`,
          backgroundSize: 'cover',
          backgroundPosition: 'center',
          borderBottom: '4px solid #efa748'
        }}>
          <div style={{ position: 'absolute', right: '10%', top: '20%', opacity: 0.1, color: '#efa748' }}>
            <Map size={150} />
          </div>
        </div>

        {/* CONTENEDOR PRINCIPAL DEL PERFIL */}
        <div style={{ maxWidth: '1100px', margin: '0 auto', padding: '0 30px' }}>
          
          {/* CABECERA DEL PERFIL */}
          <div style={{ display: 'flex', alignItems: 'flex-end', marginTop: '-75px', marginBottom: '40px', gap: '30px' }}>
            
            {/* AVATAR (Z-INDEX: 10) */}
            <div style={{ 
              backgroundColor: '#ffffff', 
              border: '6px solid #fcfcfc', 
              height: '150px', 
              width: '150px', 
              borderRadius: '50%', 
              display: 'flex', 
              justifyContent: 'center', 
              alignItems: 'center',
              boxShadow: '0 10px 25px rgba(0,0,0,0.08)',
              position: 'relative',
              zIndex: 10
            }}>
              <User size={65} color="#ccc" />
            </div>

            {/* INFO PRINCIPAL (¡ARREGLO DE Z-INDEX AQUÍ!) */}
            <div style={{ paddingBottom: '15px', position: 'relative', zIndex: 11 }}>
              <h1 style={{ fontSize: '2rem', fontWeight: '900', letterSpacing: '-1px', lineHeight: '1', marginBottom: '8px', color: '#161311' }}>
                Martin Otero
              </h1>
              <p style={{ color: '#efa748', textTransform: 'uppercase', letterSpacing: '1.5px', fontSize: '0.8rem', fontWeight: '700' }}>
                Planificadora Regional (Regional Planner)
              </p>
            </div>

            {/* BOTONES DE ACCIÓN (Derecha) */}
            <div style={{ marginLeft: 'auto', paddingBottom: '15px', display: 'flex', gap: '15px', position: 'relative', zIndex: 11 }}>
              <button style={{ backgroundColor: '#161311', color: '#fff', border: 'none', padding: '10px 20px', borderRadius: '8px', fontWeight: '600', cursor: 'pointer', display: 'flex', alignItems: 'center', gap: '8px', transition: 'all 0.2s' }} onMouseOver={(e) => e.currentTarget.style.backgroundColor = '#efa748'} onMouseOut={(e) => e.currentTarget.style.backgroundColor = '#161311'}>
                <User size={18} /> Editar Perfil
              </button>
            </div>
          </div>

          {/* GRID DE CONTENIDO (1fr 2fr) */}
          <div style={{ display: 'grid', gridTemplateColumns: '1fr 2fr', gap: '40px', alignItems: 'start' }}>
            
            {/* COLUMNA IZQUIERDA: Info Personal, Empresa y Seguridad */}
            <div style={{ display: 'flex', flexDirection: 'column', gap: '30px' }}>
              
              {/* Tarjeta de Información de Usuario */}
              <div style={{ backgroundColor: '#ffffff', padding: '30px', borderRadius: '12px', boxShadow: '0 8px 25px rgba(0,0,0,0.04)', border: '1px solid #eee' }}>
                <h3 style={{ fontSize: '1.2rem', fontWeight: '700', marginBottom: '25px', borderBottom: '2px solid #f5f5f5', paddingBottom: '10px' }}>Información de Cuenta</h3>
                
                <div style={{ display: 'flex', flexDirection: 'column', gap: '20px' }}>
                  <div style={{ display: 'flex', alignItems: 'center', gap: '15px' }}>
                    <div style={{ backgroundColor: '#fff5e6', padding: '10px', borderRadius: '8px' }}><Mail size={20} color="#efa748" /></div>
                    <div>
                      <p style={{ fontSize: '0.8rem', color: '#888', textTransform: 'uppercase', letterSpacing: '1px', marginBottom: '2px' }}>Email corporativo</p>
                      <p style={{ fontWeight: '600', color: '#333' }}>l.navarro@georesearch.es</p>
                    </div>
                  </div>
                  
                  <div style={{ display: 'flex', alignItems: 'center', gap: '15px' }}>
                    <div style={{ backgroundColor: '#fdf4ec', padding: '10px', borderRadius: '8px' }}><Building size={20} color="#96551f" /></div>
                    <div>
                      <p style={{ fontSize: '0.8rem', color: '#888', textTransform: 'uppercase', letterSpacing: '1px', marginBottom: '2px' }}>Empresa / Propósito</p>
                      <p style={{ fontWeight: '600', color: '#333' }}>GeoLúmica Research</p>
                    </div>
                  </div>
                </div>
              </div>

              {/* Tarjeta de Nivel de Suscripción */}
              <div style={{ backgroundColor: '#f8fbff', padding: '30px', borderRadius: '12px', boxShadow: '0 8px 25px rgba(0,0,0,0.04)', border: '1px solid #e3f2fd' }}>
                <h3 style={{ fontSize: '1.2rem', fontWeight: '700', marginBottom: '15px', color: '#161311' }}>Nivel de Suscripción</h3>
                
                <div style={{ display: 'flex', alignItems: 'center', gap: '10px', marginBottom: '15px' }}>
                  <Shield size={24} color="#1976d2" />
                  <span style={{ fontWeight: '800', fontSize: '1.1rem', color: '#1976d2' }}>Government Enterprise Tier</span>
                </div>
                <p style={{ color: '#555', fontSize: '0.9rem', lineHeight: '1.5' }}>
                  Tu organización tiene acceso completo a métricas de luminosidad, conectividad OSM y proyecciones demográficas a nivel municipal.
                </p>
              </div>

              {/* Tarjeta de Seguridad (Basado en el mockup) */}
              <div style={{ backgroundColor: '#ffffff', padding: '30px', borderRadius: '12px', boxShadow: '0 8px 25px rgba(0,0,0,0.04)', border: '1px solid #eee' }}>
                <h3 style={{ fontSize: '1.2rem', fontWeight: '700', marginBottom: '20px' }}>Seguridad</h3>
                
                <button style={{ width: '100%', backgroundColor: '#fff', border: '1px solid #ccc', padding: '12px', borderRadius: '8px', fontWeight: '600', cursor: 'pointer', display: 'flex', alignItems: 'center', justifyContent: 'center', gap: '10px', transition: 'all 0.2s', color: '#333' }} onMouseOver={(e) => e.currentTarget.style.backgroundColor = '#f5f5f5'} onMouseOut={(e) => e.currentTarget.style.backgroundColor = '#fff'}>
                  <Lock size={18} /> Cambiar Contraseña
                </button>
              </div>

              {/* Botón de Logout */}
              <button style={{ backgroundColor: '#fff0f0', color: '#d32f2f', border: '1px solid #ffcdd2', padding: '15px', borderRadius: '8px', fontWeight: '600', cursor: 'pointer', display: 'flex', alignItems: 'center', justifyContent: 'center', gap: '10px', transition: 'all 0.2s' }} onMouseOver={(e) => e.currentTarget.style.backgroundColor = '#ffebee'} onMouseOut={(e) => e.currentTarget.style.backgroundColor = '#fff0f0'}>
                <LogOut size={18} /> Cerrar Sesión
              </button>

            </div>

            {/* COLUMNA DERECHA: Regiones Guardadas y Actividad */}
            <div style={{ display: 'flex', flexDirection: 'column', gap: '30px' }}>
              
              {/* Tarjeta de Regiones Guardadas (Saved Regions) */}
              <div style={{ backgroundColor: '#ffffff', padding: '40px', borderRadius: '12px', boxShadow: '0 8px 25px rgba(0,0,0,0.04)', border: '1px solid #eee' }}>
                <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '30px' }}>
                  <h3 style={{ fontSize: '1.4rem', fontWeight: '800', color: '#161311' }}>Regiones Guardadas</h3>
                  <button style={{ backgroundColor: 'transparent', border: 'none', color: '#efa748', fontWeight: 'bold', cursor: 'pointer' }}>+ Añadir Región</button>
                </div>
                
                <div style={{ display: 'flex', flexDirection: 'column', gap: '15px' }}>
                  
                  {/* Región 1 */}
                  <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', padding: '15px 20px', backgroundColor: '#f9f9f9', borderRadius: '8px', borderLeft: '4px solid #efa748', transition: 'background-color 0.2s', cursor: 'pointer' }} onMouseOver={(e) => e.currentTarget.style.backgroundColor = '#f0f0f0'} onMouseOut={(e) => e.currentTarget.style.backgroundColor = '#f9f9f9'}>
                    <div style={{ display: 'flex', alignItems: 'center', gap: '15px' }}>
                      <Bookmark size={20} color="#efa748" />
                      <span style={{ fontWeight: '600', fontSize: '1.05rem', color: '#333' }}>Castilla y León - Zona A</span>
                    </div>
                    <ChevronRight size={20} color="#ccc" />
                  </div>

                  {/* Región 2 */}
                  <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', padding: '15px 20px', backgroundColor: '#f9f9f9', borderRadius: '8px', borderLeft: '4px solid #96551f', transition: 'background-color 0.2s', cursor: 'pointer' }} onMouseOver={(e) => e.currentTarget.style.backgroundColor = '#f0f0f0'} onMouseOut={(e) => e.currentTarget.style.backgroundColor = '#f9f9f9'}>
                    <div style={{ display: 'flex', alignItems: 'center', gap: '15px' }}>
                      <Bookmark size={20} color="#96551f" />
                      <span style={{ fontWeight: '600', fontSize: '1.05rem', color: '#333' }}>Andalucía - Municipio X</span>
                    </div>
                    <ChevronRight size={20} color="#ccc" />
                  </div>

                  {/* Región 3 */}
                  <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', padding: '15px 20px', backgroundColor: '#f9f9f9', borderRadius: '8px', borderLeft: '4px solid #161311', transition: 'background-color 0.2s', cursor: 'pointer' }} onMouseOver={(e) => e.currentTarget.style.backgroundColor = '#f0f0f0'} onMouseOut={(e) => e.currentTarget.style.backgroundColor = '#f9f9f9'}>
                    <div style={{ display: 'flex', alignItems: 'center', gap: '15px' }}>
                      <Bookmark size={20} color="#161311" />
                      <span style={{ fontWeight: '600', fontSize: '1.05rem', color: '#333' }}>Extremadura - Distrito Rural Y</span>
                    </div>
                    <ChevronRight size={20} color="#ccc" />
                  </div>

                </div>
              </div>

              {/* Tarjeta de Timeline de Usuario */}
              <div style={{ backgroundColor: '#ffffff', padding: '40px', borderRadius: '12px', boxShadow: '0 8px 25px rgba(0,0,0,0.04)', border: '1px solid #eee' }}>
                <h3 style={{ fontSize: '1.4rem', fontWeight: '800', marginBottom: '30px', color: '#161311' }}>Actividad Reciente</h3>
                
                {/* TIMELINE VISUAL (Adaptado al usuario) */}
                <div style={{ display: 'flex', flexDirection: 'column', gap: '25px', position: 'relative' }}>
                  <div style={{ position: 'absolute', left: '19px', top: '10px', bottom: '10px', width: '2px', backgroundColor: '#eee', zIndex: 1 }}></div>

                  {/* Item 1 */}
                  <div style={{ display: 'flex', gap: '20px', position: 'relative', zIndex: 2 }}>
                    <div style={{ backgroundColor: '#fff', border: '3px solid #efa748', width: '40px', height: '40px', borderRadius: '50%', display: 'flex', justifyContent: 'center', alignItems: 'center', flexShrink: 0 }}>
                      <FileText size={16} color="#efa748" />
                    </div>
                    <div style={{ paddingTop: '5px' }}>
                      <p style={{ fontWeight: '700', color: '#161311', marginBottom: '5px', fontSize: '1.05rem' }}>Reporte de conectividad descargado</p>
                      <p style={{ color: '#666', fontSize: '0.95rem', lineHeight: '1.5' }}>Descargaste el informe comparativo de densidad ferroviaria para "Extremadura - Distrito Rural Y".</p>
                      <p style={{ color: '#aaa', fontSize: '0.8rem', marginTop: '8px', fontWeight: '600' }}>Hace 2 horas</p>
                    </div>
                  </div>

                  {/* Item 2 */}
                  <div style={{ display: 'flex', gap: '20px', position: 'relative', zIndex: 2 }}>
                    <div style={{ backgroundColor: '#fff', border: '3px solid #161311', width: '40px', height: '40px', borderRadius: '50%', display: 'flex', justifyContent: 'center', alignItems: 'center', flexShrink: 0 }}>
                      <Bookmark size={16} color="#161311" />
                    </div>
                    <div style={{ paddingTop: '5px' }}>
                      <p style={{ fontWeight: '700', color: '#161311', marginBottom: '5px', fontSize: '1.05rem' }}>Nueva región añadida a favoritos</p>
                      <p style={{ color: '#666', fontSize: '0.95rem', lineHeight: '1.5' }}>Añadiste "Castilla y León - Zona A" a tu lista de monitoreo continuo de riesgo demográfico.</p>
                      <p style={{ color: '#aaa', fontSize: '0.8rem', marginTop: '8px', fontWeight: '600' }}>Ayer, 16:30</p>
                    </div>
                  </div>

                  {/* Item 3 */}
                  <div style={{ display: 'flex', gap: '20px', position: 'relative', zIndex: 2 }}>
                    <div style={{ backgroundColor: '#fff', border: '3px solid #ccc', width: '40px', height: '40px', borderRadius: '50%', display: 'flex', justifyContent: 'center', alignItems: 'center', flexShrink: 0 }}>
                      <Map size={16} color="#888" />
                    </div>
                    <div style={{ paddingTop: '5px' }}>
                      <p style={{ fontWeight: '700', color: '#161311', marginBottom: '5px', fontSize: '1.05rem' }}>Consulta en Mapa Interactivo</p>
                      <p style={{ color: '#666', fontSize: '0.95rem', lineHeight: '1.5' }}>Visualizaste la capa de luminosidad VIIRS sobrepuesta con los nodos OSM de "Andalucía - Municipio X".</p>
                      <p style={{ color: '#aaa', fontSize: '0.8rem', marginTop: '8px', fontWeight: '600' }}>14 Mar, 09:15</p>
                    </div>
                  </div>
                </div>
              </div>

              {/* Tarjeta de Preferencias (Vinculada a la suscripción) */}
              <div style={{ backgroundColor: '#ffffff', padding: '30px', borderRadius: '12px', boxShadow: '0 8px 25px rgba(0,0,0,0.04)', border: '1px solid #eee' }}>
                <div style={{ display: 'flex', alignItems: 'center', gap: '10px', marginBottom: '10px' }}>
                  <Zap size={20} color="#161311" />
                  <h4 style={{ fontWeight: '700', fontSize: '1.1rem' }}>Modo Analista Avanzado</h4>
                </div>
                <p style={{ color: '#666', fontSize: '0.9rem', marginBottom: '15px' }}>
                  Gracias a tu suscripción <strong>Enterprise Tier</strong>, tienes desbloqueado el acceso a los datos crudos de clustering (K-Means/DBSCAN) en el Dashboard.
                </p>
                <span style={{ display: 'inline-block', backgroundColor: '#fff5e6', color: '#efa748', padding: '5px 10px', borderRadius: '20px', fontSize: '0.8rem', fontWeight: 'bold' }}>Característica Desbloqueada</span>
              </div>

            </div>

          </div>
        </div>
      </main>

    </div>
  );
}