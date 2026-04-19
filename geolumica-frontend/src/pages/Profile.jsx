import React, { useState, useEffect } from 'react';
import NavBar from '../components/layout/NavBar';
import { 
  User, Mail, Briefcase, Map, Bookmark, 
  Shield, LogOut, ChevronRight, Zap, Building, Lock, FileText, ArrowRight, Check
} from 'lucide-react';
import mapaColorido from '../assets/mapa-colorido.png';
import logo from '../assets/logo.png'; 
import { useTranslation } from 'react-i18next'; 

export default function Profile({ currentView, setView, isAuthenticated, setIsAuthenticated }) {
  const { t } = useTranslation();
  const [isLogin, setIsLogin] = useState(true);
  
  // Estado del usuario con campos ampliados
  const [userData, setUserData] = useState({ name: '', email: '', company: '', role: '' });

  // Estados para el modo edición
  const [isEditing, setIsEditing] = useState(false);
  const [editName, setEditName] = useState('');
  const [editRole, setEditRole] = useState('');

  useEffect(() => {
    const storedUser = localStorage.getItem('geolumica_user');
    if (storedUser) {
      setUserData(JSON.parse(storedUser));
    }
  }, [isAuthenticated]); 

  // --- LÓGICA DE AUTENTICACIÓN ---
  const handleAuth = (e) => {
    e.preventDefault(); 
    const email = e.target.email.value;
    const password = e.target.password.value;

    if (!isLogin) {
      const name = e.target.name.value;
      const company = e.target.company.value; // Capturamos la empresa
      
      // Creamos el nuevo usuario con un rol por defecto
      const newUser = { name, email, password, company, role: 'Analista Junior' };
      localStorage.setItem('geolumica_user', JSON.stringify(newUser));
      setIsAuthenticated(true);
      setView('dashboard');
    } else {
      const storedUserString = localStorage.getItem('geolumica_user');
      if (!storedUserString) {
        alert(t('profile.auth.noAccountAlert'));
        return;
      }
      const storedUser = JSON.parse(storedUserString);
      if (storedUser.email === email && storedUser.password === password) {
        setIsAuthenticated(true);
        setView('dashboard');
      } else {
        alert(t('profile.auth.wrongAuthAlert'));
      }
    }
  };

  // --- LÓGICA PARA EDITAR EL PERFIL ---
  const handleEditClick = () => {
    setEditName(userData.name);
    setEditRole(userData.role || 'Analista Junior');
    setIsEditing(true);
  };

  const handleSaveProfile = () => {
    const updatedUser = { ...userData, name: editName, role: editRole };
    setUserData(updatedUser);
    localStorage.setItem('geolumica_user', JSON.stringify(updatedUser)); // Guardamos los cambios
    setIsEditing(false); // Cerramos el modo edición
  };

  const handleLogout = () => {
    setIsAuthenticated(false);
    setView('home'); 
  };

  const handleChangePassword = () => {
    const storedUserString = localStorage.getItem('geolumica_user');
    if (storedUserString) {
      const storedUser = JSON.parse(storedUserString);
      const newPassword = prompt(t('profile.auth.newPasswordPrompt'));
      
      if (newPassword && newPassword.trim() !== '') {
        storedUser.password = newPassword;
        localStorage.setItem('geolumica_user', JSON.stringify(storedUser));
        alert(t('profile.auth.passwordSuccess'));
      }
    }
  };

  // Comprobamos si el usuario tiene acceso avanzado (Si su empresa contiene 'geolumica')
  const isAdvancedUser = userData.company && userData.company.toLowerCase().includes('geolumica');


  // --- VISTA 1: FORMULARIO DE INICIO DE SESIÓN / REGISTRO ---
  if (!isAuthenticated) {
    return (
      <div style={{ backgroundColor: '#fcfcfc', minHeight: '100vh', color: '#161311', fontFamily: '"Segoe UI", Roboto, Helvetica, Arial, sans-serif' }}>
        <NavBar currentView={currentView} setView={setView} />
        
        <main style={{ display: 'flex', justifyContent: 'center', alignItems: 'center', minHeight: 'calc(100vh - 75px)', padding: '40px 20px', backgroundImage: `radial-gradient(circle at top right, #fff5e6 0%, #fcfcfc 40%)` }}>
          <div style={{ backgroundColor: '#ffffff', width: '100%', maxWidth: '420px', padding: '40px', borderRadius: '16px', boxShadow: '0 15px 40px rgba(0,0,0,0.08)', border: '1px solid #eee' }}>
            
            <div style={{ textAlign: 'center', marginBottom: '35px' }}>
              <img tabIndex="0" src={logo} alt="GeoLúmica Logo" style={{ height: '60px', objectFit: 'contain', marginBottom: '20px' }} />
              <h2 tabIndex="0" style={{ fontSize: '1.8rem', fontWeight: '900', color: '#161311', marginBottom: '10px', letterSpacing: '-0.5px' }}>
                {isLogin ? t('profile.auth.welcomeBack') : t('profile.auth.join')}
              </h2>
              <p tabIndex="0" style={{ color: '#666', fontSize: '0.95rem' }}>
                {isLogin ? t('profile.auth.loginDesc') : t('profile.auth.registerDesc')}
              </p>
            </div>

            <form onSubmit={handleAuth} style={{ display: 'flex', flexDirection: 'column', gap: '20px' }}>
              
              {!isLogin && (
                <>
                  <div>
                    <label tabIndex="0" style={{ display: 'block', fontSize: '0.85rem', fontWeight: '700', marginBottom: '8px', color: '#161311' }}>{t('profile.auth.fullName')}</label>
                    <div style={{ position: 'relative' }}>
                      <div style={{ position: 'absolute', left: '15px', top: '50%', transform: 'translateY(-50%)', color: '#aaa' }}><User size={18} /></div>
                      <input name="name" type="text" placeholder={t('profile.auth.namePlaceholder')} required style={{ width: '100%', padding: '12px 15px 12px 45px', borderRadius: '8px', border: '1px solid #ddd', fontSize: '0.95rem', fontFamily: 'inherit', boxSizing: 'border-box', outlineColor: '#efa748' }} />
                    </div>
                  </div>

                  {/* NUEVO CAMPO DE EMPRESA */}
                  <div>
                    <label tabIndex="0" style={{ display: 'block', fontSize: '0.85rem', fontWeight: '700', marginBottom: '8px', color: '#161311' }}>Empresa / Organización</label>
                    <div style={{ position: 'relative' }}>
                      <div style={{ position: 'absolute', left: '15px', top: '50%', transform: 'translateY(-50%)', color: '#aaa' }}><Building size={18} /></div>
                      <input name="company" type="text" placeholder="Ej. GeoLúmica, INE, Universidad..." required style={{ width: '100%', padding: '12px 15px 12px 45px', borderRadius: '8px', border: '1px solid #ddd', fontSize: '0.95rem', fontFamily: 'inherit', boxSizing: 'border-box', outlineColor: '#efa748' }} />
                    </div>
                  </div>
                </>
              )}

              <div>
                <label tabIndex="0" style={{ display: 'block', fontSize: '0.85rem', fontWeight: '700', marginBottom: '8px', color: '#161311' }}>{t('profile.auth.email')}</label>
                <div style={{ position: 'relative' }}>
                  <div style={{ position: 'absolute', left: '15px', top: '50%', transform: 'translateY(-50%)', color: '#aaa' }}><Mail size={18} /></div>
                  <input name="email" type="email" placeholder="tu@empresa.com" required style={{ width: '100%', padding: '12px 15px 12px 45px', borderRadius: '8px', border: '1px solid #ddd', fontSize: '0.95rem', fontFamily: 'inherit', boxSizing: 'border-box', outlineColor: '#efa748' }} />
                </div>
              </div>

              <div>
                <label tabIndex="0" style={{ display: 'block', fontSize: '0.85rem', fontWeight: '700', marginBottom: '8px', color: '#161311' }}>{t('profile.auth.password')}</label>
                <div style={{ position: 'relative' }}>
                  <div style={{ position: 'absolute', left: '15px', top: '50%', transform: 'translateY(-50%)', color: '#aaa' }}><Lock size={18} /></div>
                  <input name="password" type="password" placeholder="••••••••" required style={{ width: '100%', padding: '12px 15px 12px 45px', borderRadius: '8px', border: '1px solid #ddd', fontSize: '0.95rem', fontFamily: 'inherit', boxSizing: 'border-box', outlineColor: '#efa748' }} />
                </div>
              </div>

              <button type="submit" style={{ width: '100%', backgroundColor: '#efa748', color: '#161311', border: 'none', padding: '14px', borderRadius: '8px', fontSize: '1rem', fontWeight: 'bold', cursor: 'pointer', marginTop: '10px', display: 'flex', justifyContent: 'center', alignItems: 'center', gap: '10px', transition: 'background-color 0.2s', boxShadow: '0 4px 15px rgba(239, 167, 72, 0.3)' }} onMouseOver={(e) => e.currentTarget.style.backgroundColor = '#e09635'} onMouseOut={(e) => e.currentTarget.style.backgroundColor = '#efa748'}>
                {isLogin ? t('profile.auth.loginBtn') : t('profile.auth.registerBtn')} <ArrowRight size={18} />
              </button>

            </form>

            <div style={{ textAlign: 'center', marginTop: '30px', borderTop: '1px solid #eee', paddingTop: '20px' }}>
              <p style={{ fontSize: '0.9rem', color: '#666' }}>
                {isLogin ? t('profile.auth.noAccount') : t('profile.auth.hasAccount')}
                <button onClick={() => setIsLogin(!isLogin)} style={{ background: 'transparent', border: 'none', color: '#efa748', fontWeight: 'bold', cursor: 'pointer', marginLeft: '5px', fontSize: '0.9rem', padding: 0 }}>
                  {isLogin ? t('profile.auth.registerHere') : t('profile.auth.loginHere')}
                </button>
              </p>
            </div>

          </div>
        </main>
      </div>
    );
  }

  // --- VISTA 2: PERFIL DEL USUARIO LOGEADO ---
  return (
    <div style={{ backgroundColor: '#fcfcfc', minHeight: '100vh', color: '#161311', fontFamily: '"Segoe UI", Roboto, Helvetica, Arial, sans-serif' }}>
      
      <NavBar currentView={currentView} setView={setView} />

      <main style={{ paddingBottom: '100px' }}>
        
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

        <div style={{ maxWidth: '1100px', margin: '0 auto', padding: '0 30px' }}>
          
          <div style={{ display: 'flex', alignItems: 'flex-end', marginTop: '-75px', marginBottom: '40px', gap: '30px' }}>
            
            <div tabIndex="0" aria-label="Avatar del usuario" style={{ backgroundColor: '#ffffff', border: '6px solid #fcfcfc', height: '150px', width: '150px', borderRadius: '50%', display: 'flex', justifyContent: 'center', alignItems: 'center', boxShadow: '0 10px 25px rgba(0,0,0,0.08)', position: 'relative', zIndex: 10 }}>
              <User size={65} color="#ccc" />
            </div>

            {/* SECCIÓN DE EDICIÓN DE NOMBRE Y ROL */}
            <div style={{ paddingBottom: '15px', position: 'relative', zIndex: 11, flex: 1 }}>
              {isEditing ? (
                <div style={{ display: 'flex', flexDirection: 'column', gap: '8px' }}>
                  <input 
                    value={editName} 
                    onChange={(e) => setEditName(e.target.value)} 
                    style={{ fontSize: '1.8rem', fontWeight: 'bold', padding: '5px 10px', borderRadius: '6px', border: '2px solid #efa748', outline: 'none' }} 
                  />
                  <input 
                    value={editRole} 
                    onChange={(e) => setEditRole(e.target.value)} 
                    style={{ fontSize: '0.9rem', padding: '5px 10px', borderRadius: '6px', border: '1px solid #ccc', textTransform: 'uppercase', outline: 'none' }} 
                  />
                </div>
              ) : (
                <>
                  <h1 tabIndex="0" style={{ fontSize: '2rem', fontWeight: '900', letterSpacing: '-1px', lineHeight: '1', marginBottom: '8px', color: '#161311' }}>
                    {userData.name}
                  </h1>
                  <p tabIndex="0" style={{ color: '#efa748', textTransform: 'uppercase', letterSpacing: '1.5px', fontSize: '0.8rem', fontWeight: '700' }}>
                    {userData.role || t('profile.role')}
                  </p>
                </>
              )}
            </div>

            <div style={{ marginLeft: 'auto', paddingBottom: '15px', display: 'flex', gap: '15px', position: 'relative', zIndex: 11 }}>
              {isEditing ? (
                <button onClick={handleSaveProfile} style={{ backgroundColor: '#10b981', color: '#fff', border: 'none', padding: '10px 20px', borderRadius: '8px', fontWeight: '600', cursor: 'pointer', display: 'flex', alignItems: 'center', gap: '8px', transition: 'all 0.2s' }}>
                  <Check size={18} /> Guardar
                </button>
              ) : (
                <button onClick={handleEditClick} style={{ backgroundColor: '#161311', color: '#fff', border: 'none', padding: '10px 20px', borderRadius: '8px', fontWeight: '600', cursor: 'pointer', display: 'flex', alignItems: 'center', gap: '8px', transition: 'all 0.2s' }} onMouseOver={(e) => e.currentTarget.style.backgroundColor = '#efa748'} onMouseOut={(e) => e.currentTarget.style.backgroundColor = '#161311'}>
                  <User size={18} /> {t('profile.editProfile')}
                </button>
              )}
            </div>
          </div>

          <div style={{ display: 'grid', gridTemplateColumns: '1fr 2fr', gap: '40px', alignItems: 'start' }}>
            
            <div style={{ display: 'flex', flexDirection: 'column', gap: '30px' }}>
              
              <div style={{ backgroundColor: '#ffffff', padding: '30px', borderRadius: '12px', boxShadow: '0 8px 25px rgba(0,0,0,0.04)', border: '1px solid #eee' }}>
                <h3 tabIndex="0" style={{ fontSize: '1.2rem', fontWeight: '700', marginBottom: '25px', borderBottom: '2px solid #f5f5f5', paddingBottom: '10px' }}>{t('profile.accountInfo.title')}</h3>
                
                <div style={{ display: 'flex', flexDirection: 'column', gap: '20px' }}>
                  <div tabIndex="0" style={{ display: 'flex', alignItems: 'center', gap: '15px' }}>
                    <div style={{ backgroundColor: '#fff5e6', padding: '10px', borderRadius: '8px' }}><Mail size={20} color="#efa748" /></div>
                    <div>
                      <p style={{ fontSize: '0.8rem', color: '#888', textTransform: 'uppercase', letterSpacing: '1px', marginBottom: '2px' }}>{t('profile.accountInfo.emailLabel')}</p>
                      <p style={{ fontWeight: '600', color: '#333' }}>{userData.email}</p>
                    </div>
                  </div>
                  
                  <div tabIndex="0" style={{ display: 'flex', alignItems: 'center', gap: '15px' }}>
                    <div style={{ backgroundColor: '#fdf4ec', padding: '10px', borderRadius: '8px' }}><Building size={20} color="#96551f" /></div>
                    <div>
                      <p style={{ fontSize: '0.8rem', color: '#888', textTransform: 'uppercase', letterSpacing: '1px', marginBottom: '2px' }}>{t('profile.accountInfo.companyLabel')}</p>
                      {/* Aquí mostramos la empresa real del usuario */}
                      <p style={{ fontWeight: '600', color: '#333' }}>{userData.company || 'Desconocida'}</p>
                    </div>
                  </div>
                </div>
              </div>

              <div tabIndex="0" style={{ backgroundColor: '#f8fbff', padding: '30px', borderRadius: '12px', boxShadow: '0 8px 25px rgba(0,0,0,0.04)', border: '1px solid #e3f2fd' }}>
                <h3 style={{ fontSize: '1.2rem', fontWeight: '700', marginBottom: '15px', color: '#161311' }}>{t('profile.subscription.title')}</h3>
                
                <div style={{ display: 'flex', alignItems: 'center', gap: '10px', marginBottom: '15px' }}>
                  <Shield size={24} color="#1976d2" />
                  <span style={{ fontWeight: '800', fontSize: '1.1rem', color: '#1976d2' }}>{t('profile.subscription.tier')}</span>
                </div>
                <p style={{ color: '#555', fontSize: '0.9rem', lineHeight: '1.5' }}>
                  {t('profile.subscription.desc')}
                </p>
              </div>

              <div style={{ backgroundColor: '#ffffff', padding: '30px', borderRadius: '12px', boxShadow: '0 8px 25px rgba(0,0,0,0.04)', border: '1px solid #eee' }}>
                <h3 tabIndex="0" style={{ fontSize: '1.2rem', fontWeight: '700', marginBottom: '20px' }}>{t('profile.security.title')}</h3>
                <button 
                  onClick={handleChangePassword} 
                  style={{ width: '100%', backgroundColor: '#fff', border: '1px solid #ccc', padding: '12px', borderRadius: '8px', fontWeight: '600', cursor: 'pointer', display: 'flex', alignItems: 'center', justifyContent: 'center', gap: '10px', transition: 'all 0.2s', color: '#333' }} 
                  onMouseOver={(e) => e.currentTarget.style.backgroundColor = '#f5f5f5'} 
                  onMouseOut={(e) => e.currentTarget.style.backgroundColor = '#fff'}
                >
                  <Lock size={18} /> {t('profile.security.changePassword')}
                </button>
              </div>

              <button 
                onClick={handleLogout} 
                style={{ backgroundColor: '#fff0f0', color: '#d32f2f', border: '1px solid #ffcdd2', padding: '15px', borderRadius: '8px', fontWeight: '600', cursor: 'pointer', display: 'flex', alignItems: 'center', justifyContent: 'center', gap: '10px', transition: 'all 0.2s' }} 
                onMouseOver={(e) => e.currentTarget.style.backgroundColor = '#ffebee'} 
                onMouseOut={(e) => e.currentTarget.style.backgroundColor = '#fff0f0'}
              >
                <LogOut size={18} /> {t('profile.logout')}
              </button>

            </div>

            <div style={{ display: 'flex', flexDirection: 'column', gap: '30px' }}>
              
              <div style={{ backgroundColor: '#ffffff', padding: '40px', borderRadius: '12px', boxShadow: '0 8px 25px rgba(0,0,0,0.04)', border: '1px solid #eee' }}>
                <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '30px' }}>
                  <h3 tabIndex="0" style={{ fontSize: '1.4rem', fontWeight: '800', color: '#161311' }}>{t('profile.savedRegions.title')}</h3>
                  <button style={{ backgroundColor: 'transparent', border: 'none', color: '#efa748', fontWeight: 'bold', cursor: 'pointer' }}>{t('profile.savedRegions.add')}</button>
                </div>
                
                <div style={{ display: 'flex', flexDirection: 'column', gap: '15px' }}>
                  <div tabIndex="0" style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', padding: '15px 20px', backgroundColor: '#f9f9f9', borderRadius: '8px', borderLeft: '4px solid #efa748', transition: 'background-color 0.2s', cursor: 'pointer' }} onMouseOver={(e) => e.currentTarget.style.backgroundColor = '#f0f0f0'} onMouseOut={(e) => e.currentTarget.style.backgroundColor = '#f9f9f9'}>
                    <div style={{ display: 'flex', alignItems: 'center', gap: '15px' }}>
                      <Bookmark size={20} color="#efa748" />
                      <span style={{ fontWeight: '600', fontSize: '1.05rem', color: '#333' }}>{t('profile.savedRegions.region1')}</span>
                    </div>
                    <ChevronRight size={20} color="#ccc" />
                  </div>

                  <div tabIndex="0" style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', padding: '15px 20px', backgroundColor: '#f9f9f9', borderRadius: '8px', borderLeft: '4px solid #96551f', transition: 'background-color 0.2s', cursor: 'pointer' }} onMouseOver={(e) => e.currentTarget.style.backgroundColor = '#f0f0f0'} onMouseOut={(e) => e.currentTarget.style.backgroundColor = '#f9f9f9'}>
                    <div style={{ display: 'flex', alignItems: 'center', gap: '15px' }}>
                      <Bookmark size={20} color="#96551f" />
                      <span style={{ fontWeight: '600', fontSize: '1.05rem', color: '#333' }}>{t('profile.savedRegions.region2')}</span>
                    </div>
                    <ChevronRight size={20} color="#ccc" />
                  </div>

                  <div tabIndex="0" style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', padding: '15px 20px', backgroundColor: '#f9f9f9', borderRadius: '8px', borderLeft: '4px solid #161311', transition: 'background-color 0.2s', cursor: 'pointer' }} onMouseOver={(e) => e.currentTarget.style.backgroundColor = '#f0f0f0'} onMouseOut={(e) => e.currentTarget.style.backgroundColor = '#f9f9f9'}>
                    <div style={{ display: 'flex', alignItems: 'center', gap: '15px' }}>
                      <Bookmark size={20} color="#161311" />
                      <span style={{ fontWeight: '600', fontSize: '1.05rem', color: '#333' }}>{t('profile.savedRegions.region3')}</span>
                    </div>
                    <ChevronRight size={20} color="#ccc" />
                  </div>
                </div>
              </div>

              <div style={{ backgroundColor: '#ffffff', padding: '40px', borderRadius: '12px', boxShadow: '0 8px 25px rgba(0,0,0,0.04)', border: '1px solid #eee' }}>
                <h3 tabIndex="0" style={{ fontSize: '1.4rem', fontWeight: '800', marginBottom: '30px', color: '#161311' }}>{t('profile.recentActivity.title')}</h3>
                
                <div style={{ display: 'flex', flexDirection: 'column', gap: '25px', position: 'relative' }}>
                  <div style={{ position: 'absolute', left: '19px', top: '10px', bottom: '10px', width: '2px', backgroundColor: '#eee', zIndex: 1 }}></div>

                  <div tabIndex="0" style={{ display: 'flex', gap: '20px', position: 'relative', zIndex: 2 }}>
                    <div style={{ backgroundColor: '#fff', border: '3px solid #efa748', width: '40px', height: '40px', borderRadius: '50%', display: 'flex', justifyContent: 'center', alignItems: 'center', flexShrink: 0 }}>
                      <FileText size={16} color="#efa748" />
                    </div>
                    <div style={{ paddingTop: '5px' }}>
                      <p style={{ fontWeight: '700', color: '#161311', marginBottom: '5px', fontSize: '1.05rem' }}>{t('profile.recentActivity.act1Title')}</p>
                      <p style={{ color: '#666', fontSize: '0.95rem', lineHeight: '1.5' }}>{t('profile.recentActivity.act1Desc')}</p>
                      <p style={{ color: '#aaa', fontSize: '0.8rem', marginTop: '8px', fontWeight: '600' }}>{t('profile.recentActivity.act1Time')}</p>
                    </div>
                  </div>

                  <div tabIndex="0" style={{ display: 'flex', gap: '20px', position: 'relative', zIndex: 2 }}>
                    <div style={{ backgroundColor: '#fff', border: '3px solid #161311', width: '40px', height: '40px', borderRadius: '50%', display: 'flex', justifyContent: 'center', alignItems: 'center', flexShrink: 0 }}>
                      <Bookmark size={16} color="#161311" />
                    </div>
                    <div style={{ paddingTop: '5px' }}>
                      <p style={{ fontWeight: '700', color: '#161311', marginBottom: '5px', fontSize: '1.05rem' }}>{t('profile.recentActivity.act2Title')}</p>
                      <p style={{ color: '#666', fontSize: '0.95rem', lineHeight: '1.5' }}>{t('profile.recentActivity.act2Desc')}</p>
                      <p style={{ color: '#aaa', fontSize: '0.8rem', marginTop: '8px', fontWeight: '600' }}>{t('profile.recentActivity.act2Time')}</p>
                    </div>
                  </div>

                  <div tabIndex="0" style={{ display: 'flex', gap: '20px', position: 'relative', zIndex: 2 }}>
                    <div style={{ backgroundColor: '#fff', border: '3px solid #ccc', width: '40px', height: '40px', borderRadius: '50%', display: 'flex', justifyContent: 'center', alignItems: 'center', flexShrink: 0 }}>
                      <Map size={16} color="#888" />
                    </div>
                    <div style={{ paddingTop: '5px' }}>
                      <p style={{ fontWeight: '700', color: '#161311', marginBottom: '5px', fontSize: '1.05rem' }}>{t('profile.recentActivity.act3Title')}</p>
                      <p style={{ color: '#666', fontSize: '0.95rem', lineHeight: '1.5' }}>{t('profile.recentActivity.act3Desc')}</p>
                      <p style={{ color: '#aaa', fontSize: '0.8rem', marginTop: '8px', fontWeight: '600' }}>{t('profile.recentActivity.act3Time')}</p>
                    </div>
                  </div>
                </div>
              </div>

              {/* MODO ANALISTA AVANZADO - CON LÓGICA DE PAYWALL */}
              <div tabIndex="0" style={{ backgroundColor: '#ffffff', padding: '30px', borderRadius: '12px', boxShadow: '0 8px 25px rgba(0,0,0,0.04)', border: '1px solid #eee', position: 'relative', overflow: 'hidden' }}>
                
                {/* Overlay de Bloqueo si NO es de GeoLúmica */}
                {!isAdvancedUser && (
                  <div style={{ position: 'absolute', top: 0, left: 0, right: 0, bottom: 0, backgroundColor: 'rgba(255,255,255,0.85)', zIndex: 5, display: 'flex', flexDirection: 'column', justifyContent: 'center', alignItems: 'center', backdropFilter: 'blur(3px)' }}>
                      <Lock size={35} color="#666" style={{ marginBottom: '10px' }} />
                      <p style={{ fontWeight: 'bold', color: '#161311', fontSize: '1.1rem' }}>Función Bloqueada</p>
                      <p style={{ fontSize: '0.8rem', color: '#666', marginTop: '5px' }}>Requiere cuenta Corporativa GeoLúmica</p>
                  </div>
                )}

                {/* Contenido (se difumina si está bloqueado) */}
                <div style={{ filter: !isAdvancedUser ? 'blur(3px)' : 'none', transition: 'filter 0.3s' }}>
                  <div style={{ display: 'flex', alignItems: 'center', gap: '10px', marginBottom: '10px' }}>
                    <Zap size={20} color="#161311" />
                    <h4 style={{ fontWeight: '700', fontSize: '1.1rem' }}>{t('profile.advancedMode.title')}</h4>
                  </div>
                  <p style={{ color: '#666', fontSize: '0.9rem', marginBottom: '15px' }}>
                    {t('profile.advancedMode.desc1')}<strong>{t('profile.advancedMode.desc2')}</strong>{t('profile.advancedMode.desc3')}
                  </p>
                  <span style={{ display: 'inline-block', backgroundColor: '#fff5e6', color: '#efa748', padding: '5px 10px', borderRadius: '20px', fontSize: '0.8rem', fontWeight: 'bold' }}>{t('profile.advancedMode.badge')}</span>
                </div>
              </div>

            </div>

          </div>
        </div>
      </main>

    </div>
  );
}