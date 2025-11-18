# Imagen base de Python
FROM python:3.12-slim

# Evitar que Python guarde __pycache__ innecesarios
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# Crear directorio de trabajo
WORKDIR /app

# Copiar dependencias
COPY requirements.txt /app/

# Instalar dependencias
RUN pip install --no-cache-dir -r requirements.txt

# Copiar todo el proyecto
COPY . /app/

# Comando por defecto (ajústalo según tu script principal)
CMD ["python", "Proyecto/main.py"]
