# Proyecto_Open_Data_I

NeoLumina - Proyecto Big Data I
===============================

Estructura del proyecto:

neo_lumina/
├─ main.py            -> Orquestador principal. Llama a todos los módulos y genera CSV temáticos.
├─ config.py          -> Variables de configuración (AOI, fechas, credenciales, rutas de salida).
├─ catalog.py         -> Consulta el catálogo Copernicus OData y convierte los resultados en DataFrame.
├─ Demografia.py      -> Descarga y procesa datos de población por provincias.
├─ osm.py             -> Descarga estaciones ferroviarias usando Overpass API.
├─ viirs.py           -> Genera plantilla CSV para night-lights VIIRS (NOAA/EOG).
├─ storage.py         -> Funciones auxiliares para guardar CSV en carpetas temáticas.
├─ utils.py           -> Funciones de utilidad general para el proyecto (opcional).
└─ outputs/
   └─ data/
      ├─ satelital/       -> CSV de productos satelitales (Sentinel-1, Sentinel-2)
      ├─ transporte/      -> CSV de estaciones ferroviarias
      └─ luz_nocturna/    -> CSV plantilla para VIIRS DNB

Flujo del proyecto:

1. main.py ejecuta todos los módulos.
2. catalog.py consulta metadatos Sentinel y devuelve DataFrames.
3. osm.py descarga estaciones de tren dentro del AOI.
4. viirs.py genera plantilla para night-lights.
5. storage.py y main.py guardan todos los CSV en la carpeta correspondiente.
6. Los CSV se usarán más adelante para análisis, ML y visualización.

Proyecto_Open_Data_I/         <-- Raíz del Proyecto
├── Dockerfile.limpieza       <-- Aquí se define el entorno
├── Dockerfile.descarga       <-- Aquí se define la extracción
├── requirements.txt          <-- Librerías unificadas (Pandas, etc.)
├── data/                     <-- Almacén de datos (Volumen)
└── Proyecto/                 <-- Tu código fuente
    ├── etl/
    └── extraction/

Notas:

docker build -t proyecto_big_data .

docker run --rm `-v "$env:USERPROFILE.config\earthengine:/root/.config/earthengine" `

docker build -f Dockerfile.limpieza -t pipeline-limpieza .

docker run --name ejecutor-limpieza -v "${PWD}/data:/app/data" pipeline-limpieza
