import pandas as pd

# Lire le CSV
df = pd.read_csv('/opt/spark/data/amazon_reviews_small.csv')

# Garder toutes les colonnes utiles
columns_to_keep = ['UserId', 'ProductId', 'ProfileName', 'HelpfulnessNumerator', 
                   'HelpfulnessDenominator', 'Score', 'Time', 'Summary']
df_clean = df[columns_to_keep].copy()

# Nettoyer Score
df_clean['Score'] = pd.to_numeric(df_clean['Score'], errors='coerce')
df_clean = df_clean.dropna(subset=['Score'])
df_clean = df_clean[(df_clean['Score'] >= 1) & (df_clean['Score'] <= 5)]

# Sauvegarder
df_clean.to_csv('/opt/spark/data/amazon_reviews_clean.csv', index=False)
print(f"Lignes avant: {len(df)}, après: {len(df_clean)}")
print(f"Colonnes: {df_clean.columns.tolist()}")