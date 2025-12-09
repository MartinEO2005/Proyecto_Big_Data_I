# ----------------------------------------------------
# Dockerfile (Ubicado DENTRO de Proyecto/etl/)
# ----------------------------------------------------
FROM python:3.12-slim
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# WORKDIR sigue siendo la base del contenedor
WORKDIR /app

# 1. INSTALACIÓN DE DEPENDENCIAS
# Copiamos requirements.txt (que está al lado del Dockerfile)
COPY requirements.txt . 
RUN pip install --no-cache-dir -r requirements.txt

# 2. COPIA DEL CÓDIGO (¡El cambio crucial!)
# Necesitamos copiar todo el proyecto desde la raíz (..) a /app
# Pero como estamos copiando desde el contexto del 'etl/',
# haremos un COPY específico, asumiendo que el build se lanza desde la raíz.
# Simplificamos la copia para que solo traiga lo necesario y el código.

# Copiamos el contenido de 'etl' y lo ponemos en la raíz del contenedor /app
COPY . /app/etl/ 

# Copiamos otros datos importantes (si es necesario)
# Si tus CSVs de entrada están en 'data/', copiamos toda la carpeta.
COPY ../../data /app/data/ 

# Corregimos el WORKDIR para que el CMD pueda ejecutar el script
WORKDIR /app/etl

# 3. COMANDO DE INICIO (CMD)
# Ahora, como estamos en el WORKDIR /app/etl, solo necesitamos el nombre del archivo.
CMD ["python", "limpiezaGeneral.py"]