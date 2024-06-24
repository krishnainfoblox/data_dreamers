import pandas as pd
import numpy as np

# Generate 10,000 records of normal data
np.random.seed(42)  # Setting seed for reproducibility

# Generating normal data
data = {
    'feature1': np.random.normal(50, 10, 10000),
    'feature2': np.random.normal(30, 5, 10000),
    'feature3': np.random.normal(100, 20, 10000)
}

# Introducing some anomalies
n_anomalies = 100
anomalies = {
    'feature1': np.random.uniform(0, 100, n_anomalies),
    'feature2': np.random.uniform(0, 60, n_anomalies),
    'feature3': np.random.uniform(50, 150, n_anomalies)
}

# Combine normal data and anomalies into a DataFrame
df_normal = pd.DataFrame(data)
df_anomalies = pd.DataFrame(anomalies)
df = pd.concat([df_normal, df_anomalies], ignore_index=True)

# Save the generated data to a CSV file
df.to_csv('data.csv', index=False)

print("Sample data with anomalies generated and saved to 'data.csv'.")
