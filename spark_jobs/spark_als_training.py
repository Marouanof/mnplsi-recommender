import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count
from pyspark.ml.recommendation import ALS
from pyspark.ml.feature import StringIndexer
from pyspark.ml.evaluation import RegressionEvaluator
import joblib

# ========== INIT SPARK ==========
spark = SparkSession.builder \
    .appName("ALSModelTraining") \
    .config("spark.driver.memory", "4g") \
    .config("spark.sql.adaptive.enabled", "false") \
    .config("spark.sql.streaming.checkpointLocation", "spark_jobs/checkpoint") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# ========== LECTURE DU DATASET ==========
print("Lecture du dataset...")
from pyspark.sql.types import StructType, StructField, StringType, FloatType, IntegerType

# Définir le schéma explicitement
schema = StructType([
    StructField("UserId", StringType(), True),
    StructField("ProductId", StringType(), True),
    StructField("ProfileName", StringType(), True),
    StructField("HelpfulnessNumerator", IntegerType(), True),
    StructField("HelpfulnessDenominator", IntegerType(), True),
    StructField("Score", FloatType(), True),
    StructField("Time", IntegerType(), True),
    StructField("Summary", StringType(), True)
])

df = spark.read \
    .format("csv") \
    .option("header", "true") \
    .schema(schema) \
    .load("/opt/spark/data/amazon_reviews_clean.csv")

df = df.select(
    col("UserId").cast("string"),
    col("ProductId").cast("string"),
    col("Score").cast("float")
).na.drop()

# ========== FILTRAGE ==========
print("Filtrage utilisateurs/produits actifs...")
user_counts = df.groupBy("UserId").agg(count("Score").alias("user_count"))
active_users = user_counts.filter(col("user_count") >= 3).select("UserId")

item_counts = df.groupBy("ProductId").agg(count("Score").alias("item_count"))
active_items = item_counts.filter(col("item_count") >= 3).select("ProductId")

df_filtered = df.join(active_users, on="UserId", how="inner") \
                .join(active_items, on="ProductId", how="inner")

print(f"Après filtrage : {df_filtered.count()}")

# ========== INDEXATION ==========
print("Indexation...")
user_indexer = StringIndexer(inputCol="UserId", outputCol="user_idx")
item_indexer = StringIndexer(inputCol="ProductId", outputCol="item_idx")

df_indexed = user_indexer.fit(df_filtered).transform(df_filtered)
df_indexed = item_indexer.fit(df_indexed).transform(df_indexed)

df_final = df_indexed.select(
    col("user_idx").cast("int"),
    col("item_idx").cast("int"),
    col("Score").cast("float")
)

# ========== SPLIT 80/10/10 ==========
train, val, test = df_final.randomSplit([0.8, 0.1, 0.1], seed=42)
print(f"Train: {train.count()}, Val: {val.count()}, Test: {test.count()}")

# ========== ALS ==========
print("Entraînement ALS...")
als = ALS(
    maxIter=10,
    regParam=0.1,
    rank=10,
    userCol="user_idx",
    itemCol="item_idx",
    ratingCol="Score",
    coldStartStrategy="drop"
)
model = als.fit(train)

# ========== ÉVALUATION ==========
evaluator = RegressionEvaluator(metricName="rmse", labelCol="Score", predictionCol="prediction")

# Validation 10%
predictions_val = model.transform(val)
rmse_val = evaluator.evaluate(predictions_val)
print(f"✅ RMSE (validation 10%) = {rmse_val:.4f}")

# Test 10% - Données non vues
predictions_test = model.transform(test)
rmse_test = evaluator.evaluate(predictions_test)
print(f"✅ RMSE (test 10% - données non vues) = {rmse_test:.4f}")

# ========== SAUVEGARDE ==========
user_factors = model.userFactors.toPandas()
item_factors = model.itemFactors.toPandas()

joblib.dump(user_factors, "/opt/spark/work-dir/user_factors.joblib")
joblib.dump(item_factors, "/opt/spark/work-dir/item_factors.joblib")
print("✅ Modèle sauvegardé (user_factors.joblib, item_factors.joblib)")

spark.stop()

