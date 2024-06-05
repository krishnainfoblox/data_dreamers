# WORKING SPARK CODE
# CODER - KRISHNA

# Import necessary libraries
from pyspark.sql import SparkSession
import pandas as pd
from datetime import datetime

# List of JAR files required for Redshift
redjar_files = "/Users/kkrishna/pycharmprojects/jars/httpcore-4.4.16.jar," \
               "/Users/kkrishna/pycharmprojects/jars/commons-logging-1.2.jar," \
               "/Users/kkrishna/pycharmprojects/jars/jackson-core-2.16.0.jar," \
               "/Users/kkrishna/pycharmprojects/jars/jackson-dataformat-cbor-2.16.0.jar," \
               "/Users/kkrishna/pycharmprojects/jars/aws-java-sdk-redshift-1.12.577.jar," \
               "/Users/kkrishna/pycharmprojects/jars/aws-java-sdk-redshiftserverless-1.12.577.jar," \
               "/Users/kkrishna/pycharmprojects/jars/aws-java-sdk-core-1.12.577.jar," \
               "/Users/kkrishna/pycharmprojects/jars/commons-codec-1.15.jar," \
               "/Users/kkrishna/pycharmprojects/jars/joda-time-2.8.1.jar," \
               "/Users/kkrishna/pycharmprojects/jars/redshift-jdbc42-2.1.0.26.jar," \
               "/Users/kkrishna/pycharmprojects/jars/jackson-databind-2.16.0.jar," \
               "/Users/kkrishna/pycharmprojects/jars/jackson-annotations-2.16.0.jar," \
               "/Users/kkrishna/pycharmprojects/jars/jsoup-1.16.1.jar," \
               "/Users/kkrishna/pycharmprojects/jars/httpclient-4.5.14.jar," \
               "/Users/kkrishna/pycharmprojects/jars/aws-java-sdk-sts-1.12.577.jar"

# Initialize Spark for Redshift
sparkrs = SparkSession.builder \
    .appName("RedshiftJDBC") \
    .config("spark.jars", redjar_files) \
    .getOrCreate()

# AWS Redshift JDBC URL and properties
jdbc_url = "jdbc:redshift://localhost:54391/ib-dl-it"
properties = {
    "user": "kkrishna",
    "password": "*********",
    "driver": "com.amazon.redshift.jdbc.Driver"
}



# Define functions
def check_nulls(schema_name):
    spark = SparkSession.builder.getOrCreate()

    # Get list of all tables
    query = f"SELECT table_name FROM information_schema.tables WHERE table_schema = '{schema_name}'"
    tables_df = spark.read.jdbc(url=jdbc_url, table=f"({query}) AS tmp", properties=properties)
    table_list = [row['table_name'] for row in tables_df.collect()]

    null_counts = []

    # Iterate over all tables and their columns
    for table in table_list:
        # Get list of all columns for the table
        column_query = f"SELECT column_name FROM information_schema.columns WHERE table_schema = '{schema_name}' AND table_name = '{table}'"
        columns_df = spark.read.jdbc(url=jdbc_url, table=f"({column_query}) AS tmp", properties=properties)
        column_list = [row['column_name'] for row in columns_df.collect()]

        for column in column_list:
            # Count nulls in each column
            null_count_query = f'SELECT COUNT(*) as null_count FROM "{schema_name}"."{table}" WHERE "{column}" IS NULL'
            null_count_df = spark.read.jdbc(url=jdbc_url, table=f"({null_count_query}) AS tmp", properties=properties)
            null_count = null_count_df.first()['null_count']
            null_counts.append((table, column, null_count))

    # Convert list to DataFrame
    null_counts_df = pd.DataFrame(null_counts, columns=["table_name", "column_name", "number_of_nulls"])

    return null_counts_df

def check_nulls_table(schema_name, table_name, column_name):
    null_count_query = f'SELECT COUNT(*) as null_count FROM "{schema_name}"."{table_name}" WHERE "{column_name}" IS NULL'
    null_count_df = sparkrs.read.jdbc(url=jdbc_url, table=f"({null_count_query}) AS tmp", properties=properties)
    null_count = null_count_df.first()['null_count']

    null_check = "FAIL" if null_count > 0 else "PASS"
    run_date = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    df = pd.DataFrame({
        "schema_name": [schema_name],
        "table_name": [table_name],
        "column_name": [column_name],
        "null_check": [null_check],
        "number_of_nulls": [null_count],
        "run_date": [run_date]
    })

    return df

def find_duplicates(schema_name, table_name, column_name):
    query = f"SELECT {column_name}, COUNT(*) c FROM {schema_name}.{table_name} GROUP BY {column_name} HAVING c > 1"
    df = sparkrs.read.jdbc(url=jdbc_url, table=f"({query}) AS tmp", properties=properties)

    if df.count() == 0:
        duplicate_check = 'PASS'
        number_of_duplicates = 0
    else:
        duplicate_check = 'FAIL'
        number_of_duplicates = df.groupBy().sum('c').collect()[0][0]

    results = pd.DataFrame({
        'schema_name': [schema_name],
        'column_name': [column_name],
        'duplicate_check': [duplicate_check],
        'number_of_duplicates': [number_of_duplicates],
        'run_date': [datetime.now()]
    })

    return results

def full_data_validation(excel_file):
    spark = SparkSession.builder.getOrCreate()

    df1 = pd.read_excel(excel_file, sheet_name='Sheet1')
    df2 = pd.read_excel(excel_file, sheet_name='Sheet2')

    all_results = []

    for index, row in df1.iterrows():
        schema_name1 = row['schema_name']
        table_name1 = row['table_name']
        column_names1 = row['column_name'].split(',')

        for index, row in df2.iterrows():
            schema_name2 = row['schema_name']
            table_name2 = row['table_name']
            column_names2 = row['column_name'].split(',')

            query1 = f"SELECT {', '.join(column_names1)} FROM {schema_name1}.{table_name1}"
            query2 = f"SELECT {', '.join(column_names2)} FROM {schema_name2}.{table_name2}"

            df1_spark = spark.read.jdbc(url=jdbc_url, table=f"({query1}) AS tmp", properties=properties)
            df2_spark = spark.read.jdbc(url=jdbc_url, table=f"({query2}) AS tmp", properties=properties)

            result1 = df1_spark.subtract(df2_spark)
            result2 = df2_spark.subtract(df1_spark)

            if result1.count() == 0:
                a_b = 'PASS'
                a_b_count_minus = 0
            else:
                a_b = 'FAIL'
                a_b_count_minus = result1.count()

            if result2.count() == 0:
                b_a = 'PASS'
                b_a_count_minus = 0
            else:
                b_a = 'FAIL'
                b_a_count_minus = result2.count()

            results = pd.DataFrame({
                'schema_name1': [schema_name1],
                'schema_name2': [schema_name2],
                'table_name1': [table_name1],
                'table_name2': [table_name2],
                'a_b': [a_b],
                'b_a': [b_a],
                'a_b_count_minus': [a_b_count_minus],
                'b_a_count_minus': [b_a_count_minus],
                'run_date': [datetime.now()]
            })

            all_results.append(results)

    return pd.concat(all_results)
