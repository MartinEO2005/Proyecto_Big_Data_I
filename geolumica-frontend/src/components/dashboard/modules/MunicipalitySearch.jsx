// components/dashboard/modules/MunicipalitySearch.jsx
import React, { useState, useEffect, useRef } from 'react';
import { Search, MapPin, X, Loader2 } from 'lucide-react';

export default function MunicipalitySearch({ onSelect }) {
  const [searchTerm, setSearchTerm] = useState('');
  const [searchResults, setSearchResults] = useState([]);
  const [isOpen, setIsOpen] = useState(false);
  const [isSearching, setIsSearching] = useState(false);
  const wrapperRef = useRef(null);

  // Cerrar el desplegable si haces clic fuera
  useEffect(() => {
    function handleClickOutside(event) {
      if (wrapperRef.current && !wrapperRef.current.contains(event.target)) {
        setIsOpen(false);
      }
    }
    document.addEventListener("mousedown", handleClickOutside);
    return () => document.removeEventListener("mousedown", handleClickOutside);
  }, [wrapperRef]);

  const handleSearch = async (e) => {
    const query = e.target.value;
    setSearchTerm(query);
    
    if (query.length > 2) {
      setIsSearching(true);
      try {
        // Sincronizado con el parámetro 'q' de la API
        const res = await fetch(`http://127.0.0.1:8000/search?q=${encodeURIComponent(query)}`);
        const data = await res.json();
        
        // Mapeamos LAU_ID a muni_key para mantener compatibilidad con el resto del front
        const procesados = (data.resultados || []).map(m => ({
          ...m,
          muni_key: m.LAU_ID 
        }));
        
        setSearchResults(procesados);
        setIsOpen(true);
      } catch (err) {
        console.error("Error en búsqueda GeoLúmica:", err);
      } finally {
        setIsSearching(false);
      }
    } else {
      setSearchResults([]);
      setIsOpen(false);
    }
  };

  const clearSearch = () => {
    setSearchTerm('');
    setSearchResults([]);
    setIsOpen(false);
  };

  return (
    <div className="relative w-full max-w-md" ref={wrapperRef}>
      <div className="relative group">
        <div className="absolute inset-y-0 left-0 pl-4 flex items-center pointer-events-none">
          {isSearching ? (
            <Loader2 size={18} className="text-indigo-500 animate-spin" />
          ) : (
            <Search size={18} className="text-slate-400 group-focus-within:text-indigo-500 transition-colors" />
          )}
        </div>
        <input
          type="text"
          className="w-full bg-white border-2 border-slate-100 py-3 pl-11 pr-12 rounded-2xl text-sm font-medium focus:outline-none focus:border-indigo-500 focus:ring-4 focus:ring-indigo-500/10 transition-all shadow-sm"
          placeholder="Buscar municipio (ej: Madrid,San Seb...)"
          value={searchTerm}
          onChange={handleSearch}
          onFocus={() => searchTerm.length > 2 && setIsOpen(true)}
        />
        {searchTerm && (
          <button 
            onClick={clearSearch}
            className="absolute inset-y-0 right-0 pr-4 flex items-center text-slate-400 hover:text-slate-600"
          >
            <X size={18} />
          </button>
        )}
      </div>
      
      {/* DESPLEGABLE DE RESULTADOS */}
      {isOpen && searchResults.length > 0 && (
        <div className="absolute top-full left-0 right-0 mt-2 bg-white/95 backdrop-blur-md border border-slate-200 rounded-2xl shadow-2xl z-[9999] max-h-64 overflow-y-auto animate-in fade-in slide-in-from-top-2 duration-200">
          <div className="p-2">
            {searchResults.map((m) => (
              <button 
                key={m.muni_key} // Clave única LAU_ID blindada
                onClick={() => {
                  onSelect(m); 
                  setSearchTerm(m.muni_display); 
                  setIsOpen(false); 
                }}
                className="w-full text-left px-4 py-3 hover:bg-indigo-50 rounded-xl flex items-center gap-3 transition-all group"
              >
                <div className="p-2 bg-slate-100 rounded-lg group-hover:bg-indigo-100 transition-colors">
                  <MapPin size={14} className="text-indigo-600" />
                </div>
                <div className="flex flex-col">
                  <span className="text-sm font-bold text-slate-700 group-hover:text-indigo-700 transition-colors">
                    {m.muni_display}
                  </span>
                  <span className="text-[10px] text-slate-400 font-medium tracking-tight">
                    ID: {m.muni_key}
                  </span>
                </div>
              </button>
            ))}
          </div>
        </div>
      )}
    </div>
  );
}