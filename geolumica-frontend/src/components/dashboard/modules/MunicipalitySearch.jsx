import React, { useState } from 'react';
import { Search } from 'lucide-react';

export default function MunicipalitySearch({ onSelect }) {
  const [searchTerm, setSearchTerm] = useState('');
  const [searchResults, setSearchResults] = useState([]);

  const handleSearch = async (e) => {
    const query = e.target.value;
    setSearchTerm(query);
    if (query.length > 2) {
      try {
        const res = await fetch(`http://127.0.0.1:8000/search?query=${query}`);
        const data = await res.json();
        setSearchResults(data.resultados || []);
      } catch (err) { console.error(err); }
    } else { setSearchResults([]); }
  };

  return (
    <div className="relative w-full md:w-80">
      <div className="relative">
        <Search className="absolute left-3 top-2.5 text-slate-400" size={18} />
        <input 
          type="text" placeholder="Buscar municipio..." 
          value={searchTerm} onChange={handleSearch}
          className="w-full pl-10 pr-4 py-2 border border-slate-200 rounded-xl focus:ring-2 focus:ring-indigo-500 bg-white shadow-sm outline-none"
        />
      </div>
      {searchResults.length > 0 && (
        <div className="absolute top-full mt-2 w-full bg-white border border-slate-100 rounded-xl shadow-lg z-50 max-h-60 overflow-y-auto">
          {searchResults.map((m) => (
            <button 
              key={m.muni_key} 
              onClick={() => { onSelect(m); setSearchTerm(''); setSearchResults([]); }}
              className="w-full text-left px-4 py-3 hover:bg-indigo-50 text-sm text-slate-700 font-medium border-b border-slate-50 last:border-0"
            >
              {m.muni_display}
            </button>
          ))}
        </div>
      )}
    </div>
  );
}