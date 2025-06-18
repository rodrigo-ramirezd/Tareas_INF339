FROM python:3.8-slim

# Instalar dependencias del sistema primero (más estables)
RUN apt-get update && apt-get install -y \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Instalar dependencias de Python con pip (usa --no-cache-dir y versiones fijas)
RUN pip install --no-cache-dir \
    apache-beam==2.50.0 \
    apache-airflow==2.6.3 \
    confluent-kafka==2.1.1 \
    avro-python3==1.10.2

WORKDIR /workspace