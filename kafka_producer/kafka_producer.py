import time
import pandas as pd
from kafka import KafkaProducer
import json
import os

# Configuration Kafka
KAFKA_BROKER = 'kafka:29092'
KAFKA_TOPIC = 'reviews'

producer = KafkaProducer(bootstrap_servers=KAFKA_BROKER,
                         value_serializer=lambda v: json.dumps(v).encode('utf-8'))

# Chemin vers le dataset
DATASET_PATH = '/opt/spark/data/amazon_reviews_clean.csv'

def produce_messages():
    try:
        df = pd.read_csv(DATASET_PATH)
        print(f"Colonnes trouvées: {df.columns.tolist()}")
        df = df[['UserId', 'ProductId', 'Score', 'Time']]
        print(f"Début de la production de {len(df)} messages sur le topic {KAFKA_TOPIC}...")
        for index, row in df.iterrows():
            message = {
                'UserId': str(row['UserId']),
                'ProductId': str(row['ProductId']),
                'Score': float(row['Score']),
                'Time': int(row['Time'])
            }
            producer.send(KAFKA_TOPIC, value=message)
            if index % 1000 == 0:
                print(f"  {index} messages envoyés...")
            time.sleep(0.01)
        print(f"Terminé ! {len(df)} messages envoyés.")
    except Exception as e:
        print(f"Erreur: {e}")
    finally:
        producer.flush()
        producer.close()

if __name__ == "__main__":
    time.sleep(5)
    produce_messages()