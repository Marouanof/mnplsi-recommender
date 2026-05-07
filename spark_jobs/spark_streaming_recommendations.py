from kafka import KafkaConsumer
import json
import joblib
import pandas as pd

# ========== CHARGER LE MODÈLE ==========
print("Chargement du modèle...")
user_factors = joblib.load("/opt/spark/work-dir/user_factors.joblib")
item_factors = joblib.load("/opt/spark/work-dir/item_factors.joblib")
print("✅ Modèle chargé")

# ========== CONFIG KAFKA CONSUMER ==========
consumer = KafkaConsumer(
    'reviews',
    bootstrap_servers='kafka:29092',
    auto_offset_reset='earliest',
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

print("🟢 En attente des messages... (Ctrl+C pour arrêter)")
print("Lance le producteur dans un AUTRE terminal !")
print("")

# ========== BOUCLE DE CONSOMMATION ==========
for message in consumer:
    data = message.value
    print(f"📩 Reçu : UserId={data['UserId']}, ProductId={data['ProductId']}, Score={data['Score']}")
    
    # Simuler une recommandation
    user_id = data['UserId']
    product_id = data['ProductId']
    
    # Chercher des produits similaires
    if product_id in item_factors['id'].values[:5]:
        print(f"   ➜ Recommandation : Vous avez noté {product_id} ({data['Score']}/5). Produits similaires disponibles !")