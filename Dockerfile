# Use an official Python runtime as the base image
FROM python:3.11-slim

# Set metadata
LABEL maintainer="MQTT Publisher Tool"
LABEL description="High-performance MQTT publisher with spread distribution and per-endpoint rate limiting"
LABEL version="1.0.0"

# Set environment variables
ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1

# Set the working directory
WORKDIR /app

# Create non-root user for security
RUN groupadd -r mqttpub && useradd -r -g mqttpub -u 1001 mqttpub

# Copy requirements file first for better caching
COPY requirements.txt .

# Install dependencies
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# Copy application files
COPY mqtt_publisher_rate_limited.py .
COPY *.py ./

# Create logs directory and set permissions
RUN mkdir -p /app/logs && \
    chown -R mqttpub:mqttpub /app

# Switch to non-root user
USER mqttpub

# Set default environment variables for configuration
ENV MQTT_BROKER="mqtt://localhost:1883"
ENV MQTT_USERNAME="admin"
ENV MQTT_PASSWORD="admin"
ENV ENDPOINTS="2000"
ENV ENDPOINT_INTERVAL="17.65"
ENV CHECK_INTERVAL="5.0"
ENV SPREAD_INTERVAL="30.0"
ENV START_DELAY="0"
ENV QOS="1"
ENV LOG_LEVEL="INFO"

CMD ["python", "mqtt_publisher_rate_limited.py"]
