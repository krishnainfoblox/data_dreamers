"""
# WORKING SPARK CODE
# CODER - KRISHNA
# pip install pyspark
# pip install pandas
# pip install openpyxl
# pip install datetime


"""
# Import any other necessary libraries

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
    "password": "*****",
    "driver": "com.amazon.redshift.jdbc.Driver"
}


# Define functions
def aws_check_nulls(schema_name):
    spark = SparkSession.builder.getOrCreate()

    # Get list of all tables
    query = f"SELECT table_name FROM information_schema.tables WHERE table_schema = '{schema_name}'"
    tables_df = sparkrs.read.jdbc(url=jdbc_url, table=f"({query}) AS tmp", properties=properties)
    table_list = [row['table_name'] for row in tables_df.collect()]

    null_counts = []

    # Iterate over all tables and their columns
    for table in table_list:
        # Get list of all columns for the table
        column_query = f"SELECT column_name FROM information_schema.columns WHERE table_schema = '{schema_name}' AND table_name = '{table}'"
        columns_df = sparkrs.read.jdbc(url=jdbc_url, table=f"({column_query}) AS tmp", properties=properties)
        column_list = [row['column_name'] for row in columns_df.collect()]

        for column in column_list:
            # Count nulls in each column
            null_count_query = f'SELECT COUNT(*) as null_count FROM "{schema_name}"."{table}" WHERE "{column}" IS NULL'
            null_count_df = sparkrs.read.jdbc(url=jdbc_url, table=f"({null_count_query}) AS tmp", properties=properties)
            null_count = null_count_df.first()['null_count']
            null_counts.append((table, column, null_count))

    # Convert list to DataFrame
    null_counts_df = pd.DataFrame(null_counts, columns=["table_name", "column_name", "number_of_nulls"])

    return null_counts_df


def aws_check_nulls_table(schema_name, table_name, column_name):
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


def aws_find_duplicates(schema_name, table_name, column_name):
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


################################################################
# FILE VALIDATION


#/Users/kkrishna/Library/CloudStorage/OneDrive-InfobloxInc/hackthon-2024/Project/code-hackathon-2024/result/aws_full_data_validation_results.xlsx



def get_columns(schema_name, table_name):
    column_query = f"SELECT column_name FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = '{schema_name}' AND TABLE_NAME = '{table_name}'"
    columns_df = sparkrs.read.jdbc(url=jdbc_url, table=f"({column_query}) AS tmp", properties=properties)
    return columns_df.rdd.flatMap(lambda x: x).collect()


def aws_full_data_validation():
    # Define file path for the single result file
    excel_file = "/Users/kkrishna/Library/CloudStorage/OneDrive-InfobloxInc/hackthon-2024/Project/code-hackathon-2024/_raw/aws_validationcheck.xlsx"
    result_file_path = "/Users/kkrishna/Library/CloudStorage/OneDrive-InfobloxInc/hackthon-2024/Project/code-hackathon-2024/result/aws_full_data_validation_results.xlsx"

    # Null Check from Excel
    def null_check_from_excel(df1):
        df_results = pd.DataFrame()

        for index, row in df1.iterrows():
            schema_name = row['schema_name']
            table_name = row['table_name']
            columns = get_columns(schema_name, table_name)

            for column_name in columns:
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
                df_results = pd.concat([df_results, df])

        return df_results

    # Duplicate Check from Excel
    def find_duplicates(df1):
        results = pd.DataFrame()

        for index, row in df1.iterrows():
            schema_name = row['schema_name']
            table_name = row['table_name']
            key_column = row['key_column']
            query = f"SELECT {key_column}, COUNT(*) c FROM {schema_name}.{table_name} GROUP BY {key_column} HAVING c > 1"
            df = sparkrs.read.jdbc(url=jdbc_url, table=f"({query}) AS tmp", properties=properties)

            if df.count() == 0:
                duplicate_check = 'PASS'
                number_of_duplicates = 0
            else:
                duplicate_check = 'FAIL'
                number_of_duplicates = df.groupBy().sum('c').collect()[0][0]

            result = pd.DataFrame({
                'schema_name': [schema_name],
                'key_column': [key_column],
                'duplicate_check': [duplicate_check],
                'number_of_duplicates': [number_of_duplicates],
                'run_date': [datetime.now().strftime('%Y-%m-%d %H:%M:%S')]
            })
            results = pd.concat([results, result])

        return results

    # Data Validation from Excel
    def data_validation(df):
        results = pd.DataFrame()

        for index, row in df.iterrows():
            s_schema_name = row['s_schema_name']
            s_tb_name = row['s_tb_name']
            s_col_name = row['s_col_name'].split(',')
            t_schema_name = row['t_schema_name']
            t_tb_name = row['t_tb_name']
            t_col_name = row['t_col_name'].split(',')
            source = f"{s_schema_name}.{s_tb_name}"
            target = f"{t_schema_name}.{t_tb_name}"
            query1 = f"SELECT {', '.join(s_col_name)} FROM {source}"
            query2 = f"SELECT {', '.join(t_col_name)} FROM {target}"
            df1_spark = sparkrs.read.jdbc(url=jdbc_url, table=f"({query1}) AS tmp", properties=properties)
            df2_spark = sparkrs.read.jdbc(url=jdbc_url, table=f"({query2}) AS tmp", properties=properties)
            result1 = df1_spark.subtract(df2_spark)
            result2 = df2_spark.subtract(df1_spark)

            a_b = 'PASS' if result1.count() == 0 else 'FAIL'
            b_a = 'PASS' if result2.count() == 0 else 'FAIL'
            a_b_count_minus = 0 if a_b == 'PASS' else result1.count()
            b_a_count_minus = 0 if b_a == 'PASS' else result2.count()
            diff_col = []

            if a_b_count_minus != 0 or b_a_count_minus != 0:
                for col in s_col_name:
                    min_result1 = df1_spark.select(col).subtract(df2_spark.select(col))
                    min_result2 = df2_spark.select(col).subtract(df1_spark.select(col))
                    if min_result1.count() > 0 or min_result2.count() > 0:
                        diff_col.append(col)

            diff_columns = ', '.join(diff_col)
            result = pd.DataFrame({
                'source': [source],
                'target': [target],
                'a-b': [a_b_count_minus],
                'a-b-result': [a_b],
                'b-a': [b_a_count_minus],
                'b-a-result': [b_a],
                'diff_columns': [diff_columns],
                'run_date': [datetime.now().strftime('%Y-%m-%d %H:%M:%S')]
            })
            results = pd.concat([results, result])

        return results

    # Read each sheet into a DataFrame
    df_null_check = pd.read_excel(excel_file, sheet_name='Sheet1')
    df_duplicate_check = pd.read_excel(excel_file, sheet_name='Sheet2')
    df_data_validation = pd.read_excel(excel_file, sheet_name='Sheet3')

    # Perform the validations
    null_check_results = null_check_from_excel(df_null_check)
    duplicate_check_results = find_duplicates(df_duplicate_check)
    data_validation_results = data_validation(df_data_validation)

    # Write the results to a single Excel file with different sheets
    with pd.ExcelWriter(result_file_path, engine='openpyxl') as writer:
        null_check_results.to_excel(writer, sheet_name='Null Check Results', index=False)
        duplicate_check_results.to_excel(writer, sheet_name='Duplicate Check Results', index=False)
        data_validation_results.to_excel(writer, sheet_name='Data Validation Results', index=False)

    return('success')