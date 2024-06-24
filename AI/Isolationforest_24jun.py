import pandas as pd
import numpy as np
from sklearn.ensemble import IsolationForest

# Load the generated data from CSV
df = pd.read_csv('data.csv')

# Select numeric columns for anomaly detection
numeric_features = df.select_dtypes(include=[np.number]).columns
X = df[numeric_features]

# Initialize Isolation Forest model
model = IsolationForest(contamination=0.01, random_state=42)  # Adjust contamination based on data characteristics

# Fit the model and predict anomalies
model.fit(X)
df['anomaly_score'] = model.decision_function(X)
df['anomaly'] = model.predict(X)

# Save the results with anomaly scores to a new CSV file
df.to_csv('results.csv', index=False)

print("Anomaly detection performed and results saved to 'results.csv'.")
