from sklearn.ensemble import IsolationForest
import numpy as np

# Selecting numeric features for anomaly detection (adjust based on your data)
numeric_features = df.select_dtypes(include=[np.number]).columns

# Prepare data for anomaly detection
X = df[numeric_features]

# Train the Isolation Forest model
model = IsolationForest(contamination=0.01)  # Adjust contamination based on your data
model.fit(X)

# Predict outliers/anomalies
df['anomaly_score'] = model.decision_function(X)
df['anomaly'] = model.predict(X)

# Display anomalies
anomalies = df[df['anomaly'] == -1]
print("Anomalies:")
print(anomalies)
