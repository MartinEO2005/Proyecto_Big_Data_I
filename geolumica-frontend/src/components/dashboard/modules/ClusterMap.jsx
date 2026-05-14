// components/dashboard/modules/ClusterMap.jsx
import React, { useEffect, useState, useMemo } from 'react';
import { MapContainer, TileLayer, GeoJSON, useMap } from 'react-leaflet';
import 'leaflet/dist/leaflet.css';
import L from 'leaflet';

// Paleta exacta de tu pipeline_clustering.py
const COLORES_GEOLUMICA = {
  '1 - Despoblación Grave (Riesgo Crítico)': '#d73027',
  '2 - Pérdida Moderada (Rural en Retroceso)': '#f46d43',
  '3 - Estancamiento Rural (Declive Suave)': '#fdae61',
  '4 - Población Estable (Núcleos Tradicionales)': '#fee090',
  '5 - Fuerte Crecimiento (Zonas de Expansión)': '#abd9e9',
  '6 - Grandes Ciudades (Motores Regionales)': '#74add1',
  '7 - Enormes Centros Logísticos (Efecto Amazon)': '#4575b4'
};

function MapController({ activeId }) {
  const map = useMap();
  useEffect(() => {
    if (!activeId) return;
    // Buscamos la capa del municipio seleccionado para hacer zoom
    map.eachLayer((layer) => {
      if (layer.feature && layer.feature.properties.LAU_ID === activeId) {
        map.flyToBounds(layer.getBounds(), { duration: 1.5, padding: [50, 50] });
      }
    });
  }, [activeId, map]);
  return null;
}

export default function ClusterMap({ municipioActual, allClusters }) {
  const [geoData, setGeoData] = useState(null);

  useEffect(() => {
    fetch('/municipios_es.geojson')
      .then(res => res.json())
      .then(data => setGeoData(data));
  }, []);

  // Función de estilo que aplica el ML al mapa
  const style = (feature) => {
    const lauId = feature.properties.LAU_ID;
    const infoCluster = allClusters[lauId]; // Buscamos el resultado del K-Means
    
    return {
      fillColor: infoCluster ? COLORES_GEOLUMICA[infoCluster.perfil] : '#f1f5f9',
      weight: lauId === municipioActual.lau_id ? 2 : 0.2,
      opacity: 1,
      color: lauId === municipioActual.lau_id ? '#1e293b' : '#cbd5e1',
      fillOpacity: 0.8
    };
  };

  return (
    <div className="w-full h-full rounded-[1.5rem] overflow-hidden relative bg-slate-100">
      <MapContainer 
        center={[40.4168, -3.7038]} 
        zoom={6} 
        preferCanvas={true} // CRÍTICO: Para renderizar 8000 polígonos con fluidez
        style={{ height: '100%', width: '100%' }}
        zoomControl={false}
      >
        <TileLayer url="https://{s}.basemaps.cartocdn.com/light_all/{z}/{x}/{y}{r}.png" />
        
        <MapController activeId={municipioActual.lau_id} />

        {geoData && (
          <GeoJSON 
            data={geoData} 
            style={style}
            onEachFeature={(feature, layer) => {
              layer.on('click', () => {
                // Aquí podrías disparar la selección desde el mapa
              });
            }}
          />
        )}
      </MapContainer>

      {/* Leyenda Táctica */}
      <div className="absolute bottom-4 right-4 z-[400] bg-white/90 p-3 rounded-2xl border border-slate-200 shadow-xl max-w-[200px]">
        <p className="text-[8px] font-black uppercase text-slate-400 mb-2">Estratigrafía Territorial</p>
        {Object.entries(COLORES_GEOLUMICA).map(([name, color]) => (
          <div key={name} className="flex items-center gap-2 mb-1">
            <div className="w-2 h-2 rounded-full" style={{ backgroundColor: color }}></div>
            <span className="text-[7px] font-bold text-slate-600 leading-none">{name}</span>
          </div>
        ))}
      </div>
    </div>
  );
}