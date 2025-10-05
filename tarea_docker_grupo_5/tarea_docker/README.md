README - Tarea Docker Grupo 5

1) Descripción
Este proyecto usa Docker para ejecutar un pipeline ETL que descarga datos meteorológicos de Villaviciosa de Odón desde la API de Open-Meteo y los guarda en una base de datos MariaDB.  
Hay dos contenedores: 
- db → base de datos MariaDB.
- etl → script de Python (mainScript.py) que obtiene y carga los datos.

2) Requisitos
- Tener Docker Desktop instalado y en ejecución (que muestre “Docker Desktop is running”).
- Haber descomprimido el proyecto en:
  C:\Users\<TuUsuario>\Desktop\tarea_docker_grupo_5\tarea_docker

3) Ejecución paso a paso
1. Abrí PowerShell y situate en la carpeta del proyecto:
   cd "C:\Users\<TuUsuario>\Desktop\tarea_docker_grupo_5\tarea_docker"

2. Verificá que Docker funciona:
   docker version
   (Si da error, abrí Docker Desktop.)

3. Construí las imágenes:
   docker compose build --no-cache
   (Crea las imágenes Docker desde los Dockerfile.)

4. Iniciá los contenedores:
   docker compose up
   (Arranca la base de datos y ejecuta el script ETL.)

5. Observá los logs:
   Verás algo similar a:
   Rango: 2025-09-28 -> 2025-10-04
   Datos descargados:
   date | temperature_max | temperature_min | precipitation_sum | weather_code
   ...
   Inserción / actualización completada.

6. Comprobá los datos en la base:
   docker ps  (para ver el nombre del contenedor db)
   docker exec -it <nombre_contenedor_db> mysql -uweatheruser -pweatherpass -e "USE weatherdb; SELECT * FROM info_meteorologica;"

7. Cuando termines, apagá los contenedores:
   docker compose down
   (O usa docker compose down -v para borrar también los datos.)

4) Posibles errores comunes
- “no configuration file provided”: no estás en la carpeta correcta. Asegurate de estar en la que tiene docker-compose.yml.
- “COPY etl.py not found”: el script se llama mainScript.py, editá el Dockerfile para usar ese nombre o renombrá el archivo.
- “Docker Daemon not running”: abrí Docker Desktop y esperá que diga “Running”.

5) Resultado esperado
El contenedor ETL mostrará una tabla con los datos meteorológicos y los guardará en MariaDB dentro de la tabla `info_meteorologica`.
