# Use an official Python base image
FROM python:3.12-slim

# Prevent creation of .pyc files and enable unbuffered stdout/stderr
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# Install system packages required by common heavy Python libs (GDAL, GEOS, PROJ, build tools)
RUN apt-get update && \
    DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
      build-essential gcc g++ git curl ca-certificates \
      gdal-bin libgdal-dev libproj-dev libgeos-dev \
      libpq-dev libssl-dev libffi-dev libxml2-dev libxslt1-dev \
      && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /app

# Copy only dependency files first to leverage Docker cache
COPY requirements.txt /app/requirements.txt
COPY constraints.txt /app/constraints.txt

# Install Python dependencies using a constraints file to pin heavy packages
RUN pip install --upgrade pip setuptools wheel && \
    pip install --no-cache-dir -r /app/requirements.txt -c /app/constraints.txt

# Copy project files
COPY . /app/

# Ensure data output directory exists
RUN mkdir -p /app/data

# Default command runs your main orchestrator
CMD ["python", "Proyecto/main.py"]
