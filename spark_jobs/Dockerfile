FROM bitnami/spark:3.2.0

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY spark_als_training.py .
COPY spark_streaming_recommendations.py .

# Commande par défaut, peut être surchargée par docker-compose
CMD ["/bin/bash"]
