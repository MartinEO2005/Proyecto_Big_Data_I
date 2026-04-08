# 🌌 GeoLumica: Integración de Open Data e Inteligencia Satelital

**GeoLumica** es un proyecto diseñado para analizar la correlación entre la infraestructura de transporte, la densidad poblacional y la actividad económica, utilizando como indicador clave la luminosidad nocturna. 

He desarrollado este sistema como un pipeline de datos automatizado que integra fuentes diversas (Copernicus, Google Earth Engine, INE y OpenStreetMap) en un modelo de datos relacional para facilitar su análisis avanzado.

---

## ⚙️ Configuración Previa (Crítico)

Para que el proyecto funcione correctamente, he identificado los siguientes puntos que debes configurar en tu entorno local:

### 1. Credenciales y APIs
* **Copernicus (CDSE):** Es necesario estar registrado en el [Copernicus Data Space](https://dataspace.copernicus.eu/). Debes añadir tus credenciales en el archivo `.env`.
* **Google Earth Engine (GEE):** 1. Requiere una cuenta con acceso a [GEE](https://earthengine.google.com/).
    2. Debes ejecutar `earthengine authenticate` en tu terminal local para generar el token de acceso.
    3. **Cambio de Proyecto:** He definido el nombre del proyecto en `config.py`. Debes cambiar el `project_name` por el ID de tu proyecto activo en Google Cloud Console.

### 2. Base de Datos (Estructura de Datos)
He diseñado el sistema para que sea flexible según la necesidad de cómputo:
* **SQLite (Por defecto):** El pipeline genera un archivo `.db` automáticamente tras la etapa de limpieza.
* **MySQL (Producción/Copo de Nieve):** Si deseas el esquema de estrella completo, he dejado comentadas las secciones de `mysql_server` y `carga_mysql` en el `docker-compose.yml`. Solo tienes que activarlas para habilitar las relaciones de integridad.

---

## 🚀 Ejecución con Docker

He automatizado el flujo de trabajo en etapas secuenciales para garantizar la limpieza de los datos.

```bash
# Construir y levantar el pipeline completo
docker-compose up --build


 ## 🔥 Ejecución del pipeline con Spark + HDFS

Además del flujo ETL general, el proyecto incorpora una capa de procesamiento distribuido con **Apache Spark** y almacenamiento en **HDFS**.

### ¿Qué hace esta parte?
- Los datos fuente se cargan en **HDFS** dentro de `/data/raw`
- Los scripts de limpieza Spark leen desde **HDFS**
- Los resultados finales se guardan en **HDFS** dentro de `/data/clean`

### Servicios implicados
- **namenode**: gestiona el sistema de archivos HDFS
- **datanode**: almacena físicamente los datos en HDFS
- **hdfs_init**: carga automáticamente los datasets necesarios en HDFS
- **limpieza**: ejecuta los scripts Spark del proyecto

### Arranque de HDFS
```bash
docker compose up -d namenode datanode