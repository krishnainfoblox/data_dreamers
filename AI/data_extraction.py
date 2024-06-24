import psycopg2
import pandas as pd

# Redshift connection parameters
redshift_host = 'localhost'
redshift_db = 'ib-dl-it'
redshift_user = 'kkrishna'
redshift_password = 'zY1O6ajaSJnffCYxe8m8'
redshift_port = 54391

# Establishing the connection
conn = psycopg2.connect(
    dbname=redshift_db,
    user=redshift_user,
    password=redshift_password,
    host=redshift_host,
    port=redshift_port
)

# Query to extract data
query = "SELECT * FROM edw_asis.vw_obj_account limit 10000"

# Load data into a DataFrame
df = pd.read_sql(query, conn)

# Close the connection
conn.close()

# Save the dataframe to CSV for later use in Azure ML Studio
df.to_csv('data.csv', index=False)
