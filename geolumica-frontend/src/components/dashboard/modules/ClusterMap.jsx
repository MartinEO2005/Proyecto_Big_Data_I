// components/dashboard/modules/ClusterMap.jsx
import React, { useEffect, useState } from 'react';
import { MapContainer, TileLayer, GeoJSON, useMap } from 'react-leaflet';
import 'leaflet/dist/leaflet.css';
import L from 'leaflet';

// Componente invisible que "vuela" a los límites exactos del polígono
function PolygonFlyer({ feature }) {
  const map = useMap();
  useEffect(() => {
    if (feature) {
      const layer = L.geoJSON(feature);
      const bounds = layer.getBounds();
      if (bounds.isValid()) {
        // Hace zoom encuadrando la forma exacta del municipio
        map.flyToBounds(bounds, { duration: 1.5, padding: [30, 30] });
      }
    }
  }, [feature, map]);
  return null;
}

export default function ClusterMap({ municipio, perfil, color }) {
  const [geoData, setGeoData] = useState(null);

  // Cargamos el GeoJSON entero una sola vez al abrir el Dashboard
  useEffect(() => {
    fetch('/municipios_es.geojson')
      .then(res => res.json())
      .then(data => setGeoData(data))
      .catch(err => console.error("Error cargando GeoJSON:", err));
  }, []);

  // Buscamos la geometría exacta (polígono) cruzando el LAU_ID (igual que en tu pipeline.py)
  const activeFeature = geoData?.features.find(
    f => f.properties.LAU_ID === municipio.lau_id
  );

  const markerColor = color || '#f59e0b';

  return (
    <div className="w-full h-full rounded-[1.5rem] overflow-hidden relative shadow-inner bg-[#f8fafc]">
      <MapContainer 
        center={[40.4168, -3.7038]} 
        zoom={6} 
        style={{ height: '100%', width: '100%' }}
        zoomControl={false}
      >
        <TileLayer
          url="https://{s}.basemaps.cartocdn.com/light_all/{z}/{x}/{y}{r}.png"
          attribution='&copy; CARTO'
        />
        
        {/* Si encontramos el polígono, volamos hacia él */}
        {activeFeature && <PolygonFlyer feature={activeFeature} />}

        {/* Pintamos el polígono con el color del Cluster */}
        {activeFeature && (
          <GeoJSON
            key={municipio.lau_id} // Obliga a redibujar si cambias de municipio
            data={activeFeature}
            style={{
              fillColor: markerColor,
              weight: 2,
              opacity: 1,
              color: '#1e293b', // Borde oscuro
              fillOpacity: 0.85
            }}
            onEachFeature={(feature, layer) => {
              layer.bindPopup(`
                <div class="text-center p-1">
                  <h3 class="font-black text-xs uppercase text-slate-800">${municipio.nombre}</h3>
                  <span class="text-[9px] font-bold text-slate-500">${perfil}</span>
                </div>
              `);
            }}
          />
        )}
      </MapContainer>
      
      {/* Leyenda Flotante */}
      <div className="absolute bottom-4 left-4 z-[400] bg-white/95 backdrop-blur-sm p-3 rounded-xl border border-slate-200 shadow-lg pointer-events-none">
        <p className="text-[8px] font-black uppercase tracking-widest text-slate-500 mb-1">Clasificación K-Means</p>
        <div className="flex items-center gap-2">
          <div className="w-3 h-3 rounded-sm border border-slate-300" style={{ backgroundColor: markerColor }}></div>
          <span className="text-[10px] font-bold text-slate-800 truncate max-w-[150px]">{perfil || "Segmentando..."}</span>
        </div>
      </div>
    </div>
  );
}