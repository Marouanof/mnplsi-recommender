# MNPLSI — Moteur de recommandation temps réel

> Système de recommandation de produits basé sur Apache Kafka, Spark MLlib (ALS) et Airflow. Projet pédagogique Master LSI 2025-2026.

## 📋 Table des matières

- [Vue d'ensemble](#vue-densemble)
- [Architecture](#architecture)
- [Démarrage rapide](#démarrage-rapide)
- [Structure du projet](#structure-du-projet)
- [Composants](#composants)
- [Endpoints API](#endpoints-api)
- [Configuration](#configuration)
- [Dépannage](#dépannage)
- [À ignorer avant GitHub](#à-ignorer-avant-github)

## Vue d'ensemble

MNPLSI implémente un **pipeline complet de recommandation en temps réel** :

1. **Ingestion** : Kafka consomme des avis utilisateurs en continu
2. **Traitement** : Spark nettoie et transforme les données
3. **Modélisation** : ALS (Alternating Least Squares) entraîne un modèle collaboratif
4. **Orchestration** : Airflow coordonne l'exécution des jobs
5. **Exposition** : API REST + interface web affichent les recommandations

**Technologie clé** : Recommandations basées sur la factorisation matricielle (user-item interactions).

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                     KAFKA (Event Streaming)                     │
│ ┌─────────────────────────────────────────────────────────────┐ │
│ │ Topic: reviews | Messages: {UserId, ProductId, Score, Time} │ │
│ └─────────────────────────────────────────────────────────────┘ │
└────────────────────────────────┬────────────────────────────────┘
                                 │
        ┌────────────────────────┼────────────────────────┐
        │                        │                        │
        ▼                        ▼                        ▼
   ┌─────────────┐      ┌─────────────────┐      ┌──────────────┐
   │   Kafka     │      │  Spark Batch    │      │   Spark      │
   │  Producer   │      │   (Training)    │      │  Streaming   │
   │  (Python)   │      │   (ALS Model)   │      │  (Real-time) │
   └─────────────┘      └─────────────────┘      └──────────────┘
        │                      │                        │
        └──────────────────────┼────────────────────────┘
                               │
                   ┌───────────▼────────────┐
                   │   Model Storage       │
                   │ (user_factors.joblib  │
                   │  item_factors.joblib) │
                   └───────────────────────┘
                               │
                   ┌───────────▼────────────┐
                   │   Flask/FastAPI       │
                   │   (API REST)          │
                   │ Port: 5000            │
                   └───────────────────────┘
                               │
                   ┌───────────▼────────────┐
                   │   Frontend (HTML/CSS) │
                   │   (Webapp)            │
                   │ Port: 80 (Docker)     │
                   └───────────────────────┘

┌─────────────────────────────────────────────────────────────────┐
│                 Airflow (Orchestration)                         │
│ DAGs:                                                           │
│ • dag_ingestion.py   → Déclenchement producteur Kafka          │
│ • dag_training.py    → Lancement job Spark ALS                 │
│ • dag_streaming.py   → Streaming recommandations               │
│ UI: http://localhost:8081 (Airflow Webserver)                 │
└─────────────────────────────────────────────────────────────────┘
```

### Flux de données

```
Dataset (Amazon Reviews)
    ↓
Kafka Producer → Kafka Topic (reviews)
    ↓
Spark Streaming Consumer
    ├─ Nettoyage (filtrage, déduplication)
    ├─ Indexation (StringIndexer pour user/item)
    └─ Construction matrice user-item
    ↓
ALS Model Training (80% train, 10% val, 10% test)
    ├─ rank=10, maxIter=10, regParam=0.01
    └─ Évaluation RMSE
    ↓
Model → Factors (user_factors, item_factors)
    ↓
API Endpoint (/recommendations/user/{user_id})
    ↓
Frontend Web UI
```

## Démarrage rapide

### Prérequis

- Docker & Docker Compose 20.10+
- Python 3.9+ (si exécution locale)
- Java 11+ (Spark)

### Installation & Lancement

```bash
# 1. Cloner le repository
git clone https://github.com/<user>/mnplsi_recommender.git
cd mnplsi_recommender

# 2. Placer le dataset
# Télécharger amazon_fine_food_reviews.csv et placer dans data/
# OU configurer le producteur pour auto-télécharger

# 3. Lancer l'infrastructure Docker
docker-compose up --build

# Services lancés:
# • Zookeeper (2181)
# • Kafka (9092)
# • Spark Master (8080, 7077)
# • Spark Worker
# • PostgreSQL (5432) - Airflow DB
# • Airflow Webserver (8081)
# • Airflow Scheduler
# • API (5000)
# • Webapp (80)
```

### Accès aux services

| Service           | URL/Port                    | Accès                        |
| :---------------- | :-------------------------- | :--------------------------- |
| Frontend (Webapp) | http://localhost            | UI démonstration             |
| API REST          | http://localhost:5000       | Docs: /docs (FastAPI)        |
| Airflow UI        | http://localhost:8081       | DAGs et exécutions           |
| Spark Master      | http://localhost:8080       | État cluster Spark           |
| Kafka Broker      | localhost:9092              | Interne (Docker)             |
| PostgreSQL        | localhost:5432              | Airflow metadata             |

## Structure du projet

```
mnplsi_recommender/
│
├── README.md                          # Ce fichier
├── docker-compose.yml                 # Orchestration Docker
├── .gitignore                         # Fichiers à ignorer
│
├── data/
│   ├── amazon_reviews_clean.csv       # Dataset principal
│   └── checkpoint/                    # Spark streaming checkpoints
│
├── kafka_producer/
│   ├── Dockerfile
│   ├── kafka_producer.py              # Script Python producteur
│   └── requirements.txt
│
├── spark_jobs/
│   ├── Dockerfile
│   ├── prepare_data.py                # Nettoyage initial
│   ├── spark_als_training.py          # Entraînement ALS
│   ├── spark_streaming_recommendations.py  # Streaming temps réel
│   ├── user_factors.joblib            # Facteurs utilisateur (modèle)
│   ├── item_factors.joblib            # Facteurs produit (modèle)
│   └── requirements.txt
│
├── api/
│   ├── Dockerfile
│   ├── main.py                        # API Flask
│   ├── requirements.txt
│   └── __pycache__/                   # À ignorer
│
├── webapp/
│   ├── Dockerfile                     # Serveur nginx pour static
│   ├── index.html                     # Interface principale
│   ├── style.css                      # Styles professionnels
│   ├── script.js                      # Logique frontend
│   └── .browserslistrc
│
├── airflow_dags/
│   ├── __init__.py
│   ├── dag_ingestion.py               # DAG ingestion Kafka
│   ├── dag_training.py                # DAG entraînement modèle
│   ├── dag_streaming.py               # DAG streaming temps réel
│   └── __pycache__/                   # À ignorer
│
├── airflow/
│   ├── Dockerfile                     # Image Airflow personnalisée
│   └── requirements.txt
│
└── venv/                              # À ignorer

```

## Composants

### 1. Kafka Producer (`kafka_producer/`)

**Rôle** : Lit le dataset et produit des événements d'avis utilisateurs.

```python
# Structure du message Kafka
{
  "UserId": "A3SGXH7AUHU8GW",
  "ProductId": "B00L9EPY8E",
  "Score": 5,
  "Time": 1370131200
}
```

**Configuration** :
- Topic : `reviews`
- Broker : `kafka:29092` (Docker)
- Débit : 100 messages/sec (configurable)

### 2. Spark Jobs (`spark_jobs/`)

#### `prepare_data.py`
Nettoyage initial du dataset :
- Suppression des doublons
- Filtrage des produits avec < 5 avis
- Normalisation des colonnes

#### `spark_als_training.py`
Entraînement du modèle ALS :
- **Input** : données nettoyées (CSV)
- **Modèle** : ALS (factorisation matricielle)
- **Hyperparamètres** : rank=10, maxIter=10, regParam=0.01
- **Splitting** : 80% train, 10% validation, 10% test
- **Output** : user_factors.joblib, item_factors.joblib
- **Métrique** : RMSE

#### `spark_streaming_recommendations.py`
Consommation Kafka et recommandations en temps réel :
- Lit les avis du topic Kafka
- Applique le modèle ALS
- Génère scores de recommandation par utilisateur

### 3. API REST (`api/`)

**Framework** : Flask avec CORS

#### Endpoints principaux

```http
GET /recommendations/user/{user_id}
```
Retourne les recommandations personnalisées.

**Response** :
```json
{
  "user_id": "A3SGXH7AUHU8GW",
  "profile_name": "Amazon Customer",
  "status": "existing_user",
  "avg_score": 4.5,
  "nb_reviews": 24,
  "recommendations": ["B001ECQ4FW", "B002DUEIQQ", "B003O475PU"]
}
```

---

```http
GET /recommendations/top
```
Retourne les 20 produits les plus populaires.

**Response** :
```json
{
  "top_products": ["B001ECQ4FW", "B002DUEIQQ", "B003O475PU", ...]
}
```

---

```http
GET /products/details?ids=B001ECQ4FW,B002DUEIQQ
```
Retourne les métadonnées produits (note moyenne, nombre d'avis, etc.).

**Response** :
```json
{
  "products": [
    {
      "product_id": "B001ECQ4FW",
      "avg_score": 4.7,
      "nb_reviews": 256,
      "summary": "Great product!"
    }
  ]
}
```

---

```http
GET /users/list
```
Retourne la liste des IDs utilisateurs disponibles.

### 4. Frontend Web (`webapp/`)

Interface responsive pour tester les recommandations :

- **Recherche** : Saisir ID utilisateur
- **Affichage** : Métriques modèle + résultats recommandations
- **Top produits** : Visualisation produits populaires
- **Responsive** : Mobile, tablet, desktop

**Tech Stack** :
- HTML5 semantic
- CSS3 (variables CSS, flexbox, grid, animations)
- Vanilla JavaScript (fetch API)

### 5. Airflow (`airflow_dags/`)

Orchestration des pipelines :

#### `dag_ingestion.py`
Déclenche le producteur Kafka sur une planification.

#### `dag_training.py`
Lance régulièrement l'entraînement du modèle ALS.

#### `dag_streaming.py`
Gère le streaming temps réel des recommandations.

**UI** : http://localhost:8081 (user: admin, password: admin)

## Configuration

### Variables d'environnement

Éditer les fichiers `.env` ou `docker-compose.yml` :

```env
# Kafka
KAFKA_BROKER=kafka:29092
KAFKA_TOPIC=reviews

# Spark
SPARK_MASTER_HOST=spark-master
SPARK_MASTER_PORT=7077

# Airflow
AIRFLOW__CORE__SQL_ALCHEMY_CONN=postgresql+psycopg2://airflow:airflow@postgres/airflow
AIRFLOW__CORE__EXECUTOR=LocalExecutor

# API
FLASK_ENV=production
API_PORT=5000
```

### Tuning des hyperparamètres ALS

Éditer `spark_jobs/spark_als_training.py` :

```python
als = ALS(
    maxIter=20,           # Augmenter pour mieux converger
    regParam=0.01,        # Régularisation
    rank=50,              # Dimension facteurs latents
    userCol="userId",
    itemCol="itemId",
    ratingCol="rating"
)
```

## Dépannage

### Erreur : "Kafka Broker not reachable"

```bash
# Vérifier que Kafka est bien lancé
docker ps | grep kafka

# Vérifier les logs
docker logs kafka

# Redémarrer
docker restart kafka
```

### Erreur : "Model not found"

```bash
# Vérifier que les modèles existent
docker exec spark-worker ls -la /opt/spark/work-dir/
# ou
ls -la spark_jobs/*.joblib

# Relancer l'entraînement
docker exec spark-master spark-submit spark_jobs/spark_als_training.py
```

### API ne répond pas

```bash
# Vérifier les logs de l'API
docker logs api

# Redémarrer
docker restart api

# Tester la connexion
curl http://localhost:5000/recommendations/top
```

### Airflow : DAG ne s'exécute pas

1. Accéder à http://localhost:8081
2. Activer le DAG (toggle)
3. Vérifier les logs dans l'interface
4. Redémarrer scheduler : `docker restart airflow-scheduler`

## À ignorer avant GitHub

**Créer un `.gitignore` à la racine** :

```gitignore
# Virtual environments
venv/
env/
ENV/
.venv

# Données brutes (larges fichiers)
data/*.csv
data/checkpoint/
*.joblib
*.pkl
*.parquet

# Cache & temporary
__pycache__/
*.pyc
*.pyo
*.pyd
.Python
*.egg-info/
.pytest_cache/
.coverage
htmlcov/

# IDE
.vscode/
.idea/
*.swp
*.swo
*~
.DS_Store

# Docker
.dockerignore
docker-compose.override.yml

# Logs
*.log
logs/
airflow/logs/
*.txt

# OS
.DS_Store
Thumbs.db

# Sensitive data
.env
.env.local
*.secrets
credentials.json
config.local.yml

# Build artifacts
dist/
build/
*.egg

# Node modules (si frontend avec npm)
node_modules/
npm-debug.log

# Spark
spark-warehouse/
metastore_db/

# Airflow
airflow/airflow.cfg
airflow/airflow.db

# Compiled Python
*.cpython-*.pyc

# IDE settings
.vscode/settings.json
.idea/workspace.xml
```

**Commandes avant commit** :

```bash
# Vérifier les fichiers à ignorer
git status

# Ajouter .gitignore
git add .gitignore

# Nettoyer les fichiers déjà trackés par erreur
git rm --cached data/*.csv
git rm --cached venv/ -r
git commit -m "Add .gitignore and clean tracked artifacts"
```

### Fichiers critiques à IGNORER absolument

| Fichier/Dossier             | Raison                              |
| :-------------------------- | :---------------------------------- |
| `data/*.csv`                | Trop volumineux (> 500 MB)          |
| `data/checkpoint/`          | Checkpoints Spark (temporaires)     |
| `*.joblib`, `*.pkl`         | Modèles sérialisés (> 50 MB)       |
| `venv/`, `env/`             | Environments Python (recréer avec pip) |
| `__pycache__/`, `.pyc`      | Cache Python compilé                |
| `.env`, `credentials.json`  | Secrets (DB passwords, API keys)    |
| `airflow/logs/`             | Logs Airflow (énormes)              |
| `.vscode/`, `.idea/`        | Config IDE personnelle              |

## Contribution

1. Fork le repository
2. Créer une branche feature : `git checkout -b feature/mon-feature`
3. Commit : `git commit -am 'Add feature'`
4. Push : `git push origin feature/mon-feature`
5. Pull Request

## Licence

Projet sous le cadre du module Big Data.

---
