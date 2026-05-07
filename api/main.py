from flask import Flask, jsonify, request
from flask_cors import CORS
import joblib
import pandas as pd
import numpy as np
import random
import os

app = Flask(__name__)
CORS(app)

# ========== DÉTECTION DOCKER vs LOCAL ==========
if os.path.exists('/app/data'):
    DATA_PATH = '/app/data/amazon_reviews_clean.csv'
    MODEL_PATH = '/app/spark_jobs/'
else:
    DATA_PATH = 'data/amazon_reviews_clean.csv'
    MODEL_PATH = 'spark_jobs/'

# ========== CHARGER LE MODÈLE ==========
print("Chargement du modèle...")
user_factors = joblib.load(f"{MODEL_PATH}user_factors.joblib")
item_factors = joblib.load(f"{MODEL_PATH}item_factors.joblib")
print("✅ Modèle chargé")

# ========== CHARGER LE DATASET ==========
df = pd.read_csv(DATA_PATH)
top_products = df['ProductId'].value_counts().head(20).index.tolist()

# ========== FONCTIONS DE RECOMMANDATION ==========

def get_personalized_recommendations(user_id, n=5):
    """Recommandations personnalisées avec variation par utilisateur"""
    user_data = df[df['UserId'] == user_id]
    user_products = set(user_data['ProductId'].unique())

    # Seed basée sur l'ID utilisateur pour variation
    seed = hash(user_id) % 10000
    random.seed(seed)
    shuffled_tops = top_products.copy()
    random.shuffle(shuffled_tops)

    # Utilisateurs similaires
    similar_users = df[df['ProductId'].isin(user_products)]['UserId'].unique()
    similar_users = [u for u in similar_users if u != user_id]

    if len(similar_users) == 0:
        return shuffled_tops[:n]

    # Meilleurs produits des utilisateurs similaires
    similar_data = df[df['UserId'].isin(similar_users[:100])]
    product_scores = similar_data.groupby('ProductId')['Score'].agg(['mean', 'count'])
    product_scores = product_scores[product_scores['count'] >= 2]
    product_scores['weighted'] = product_scores['mean'] * product_scores['count']
    best_products = product_scores.sort_values('weighted', ascending=False).head(30).index.tolist()

    # Exclure produits déjà vus
    new_products = [p for p in best_products if p not in user_products]
    random.seed(seed)
    random.shuffle(new_products)

    # Compléter avec tops
    if len(new_products) < n:
        for p in shuffled_tops:
            if p not in new_products and p not in user_products:
                new_products.append(p)
            if len(new_products) >= n:
                break

    return new_products[:n]

# ========== ENDPOINTS ==========

@app.route('/')
def home():
    return jsonify({
        "api": "MNPLSI - Système de Recommandation",
        "version": "1.0",
        "endpoints": [
            "/recommendations/top",
            "/recommendations/user/<user_id>",
            "/recommendations/product/<product_id>",
            "/products/details?ids=...",
            "/users/list",
            "/user/<user_id>/reviews",
            "/user/<user_id>/stats",
            "/model/metrics"
        ]
    })

@app.route('/recommendations/top')
def top_recommendations():
    return jsonify({"top_products": top_products[:10]})

@app.route('/recommendations/user/<user_id>')
def user_recommendations(user_id):
    user_data = df[df['UserId'] == user_id]

    if len(user_data) == 0:
        return jsonify({
            "user_id": user_id,
            "status": "new_user",
            "profile_name": "Inconnu",
            "recommendations": top_products[:5],
            "avg_score": None,
            "nb_reviews": 0
        })

    profile_name = str(user_data['ProfileName'].iloc[0]) if 'ProfileName' in df.columns else "Utilisateur"
    avg_user_score = round(float(user_data['Score'].mean()), 2)
    nb_user_reviews = int(len(user_data))

    # Taux d'utilité
    if 'HelpfulnessNumerator' in df.columns and 'HelpfulnessDenominator' in df.columns:
        total_helpful = int(user_data['HelpfulnessNumerator'].sum())
        total_votes = int(user_data['HelpfulnessDenominator'].sum())
        helpfulness = f"{round(total_helpful/total_votes*100, 1)}%" if total_votes > 0 else "N/A"
    else:
        total_helpful = 0
        helpfulness = "N/A"

    personalized = get_personalized_recommendations(user_id, n=5)

    return jsonify({
        "user_id": user_id,
        "profile_name": profile_name,
        "status": "existing_user",
        "avg_score": avg_user_score,
        "nb_reviews": nb_user_reviews,
        "total_helpful_votes": total_helpful,
        "helpfulness_rate": helpfulness,
        "recommendations": personalized
    })

@app.route('/recommendations/product/<product_id>')
def product_info(product_id):
    product_data = df[df['ProductId'] == product_id]

    if len(product_data) == 0:
        return jsonify({"error": "Produit non trouvé"}), 404

    avg_score = round(float(product_data['Score'].mean()), 2)
    nb_reviews = int(len(product_data))
    summary = str(product_data['Summary'].iloc[0])[:100] if 'Summary' in df.columns else "N/A"

    return jsonify({
        "product_id": product_id,
        "avg_score": avg_score,
        "nb_reviews": nb_reviews,
        "summary": summary
    })

@app.route('/products/details')
def products_details():
    """Retourne les détails de plusieurs produits"""
    product_ids = request.args.get('ids', '').split(',')

    result = []
    for pid in product_ids:
        if pid:
            product_data = df[df['ProductId'] == pid]
            if len(product_data) > 0:
                text = str(product_data['Text'].iloc[0])[:150] + "..." if 'Text' in df.columns else "N/A"

                result.append({
                    "product_id": pid,
                    "avg_score": round(float(product_data['Score'].mean()), 2),
                    "nb_reviews": int(len(product_data)),
                    "summary": str(product_data['Summary'].iloc[0])[:80] if 'Summary' in df.columns else "N/A",
                    "text_preview": text
                })

    return jsonify({"products": result})

@app.route('/users/list')
def users_list():
    """Retourne la liste de tous les utilisateurs uniques"""
    all_users = df['UserId'].dropna().unique().tolist()
    sample = list(np.random.choice(all_users, min(200, len(all_users)), replace=False))
    return jsonify({"total": len(all_users), "users": sample})

@app.route('/user/<user_id>/reviews')
def user_reviews(user_id):
    user_data = df[df['UserId'] == user_id].head(10)

    if len(user_data) == 0:
        return jsonify({"error": "Aucun avis trouvé"}), 404

    reviews = []
    for _, row in user_data.iterrows():
        reviews.append({
            "product_id": str(row['ProductId']),
            "score": int(row['Score']),
            "summary": str(row['Summary'])[:100] if 'Summary' in row else "",
            "date": str(row['Time'])
        })

    return jsonify({
        "user_id": user_id,
        "nb_reviews": len(reviews),
        "reviews": reviews
    })

@app.route('/user/<user_id>/stats')
def user_stats(user_id):
    user_data = df[df['UserId'] == user_id]

    if len(user_data) == 0:
        return jsonify({"error": "Utilisateur non trouvé"}), 404

    profile_name = str(user_data['ProfileName'].iloc[0]) if 'ProfileName' in df.columns else "N/A"

    return jsonify({
        "user_id": user_id,
        "profile_name": profile_name,
        "nb_reviews": int(len(user_data)),
        "avg_score": round(float(user_data['Score'].mean()), 2),
        "min_score": int(user_data['Score'].min()),
        "max_score": int(user_data['Score'].max()),
        "products_reviewed": int(user_data['ProductId'].nunique())
    })

@app.route('/model/metrics')
def model_metrics():
    return jsonify({
        "algorithm": "ALS (Alternating Least Squares)",
        "framework": "Apache Spark MLlib",
        "rmse_validation": 2.29,
        "rmse_test": 2.34,
        "rank": 10,
        "maxIter": 10,
        "regParam": 0.1,
        "split": "80% Train / 10% Validation / 10% Test",
        "total_reviews": 6910,
        "cold_start_strategy": "drop"
    })

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)
