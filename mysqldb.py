"""
# WORKING SPARK CODE
# CODER - KRISHNA

# pip install mysql-connector-python

"""

import mysql.connector
import pandas as pd
from datetime import datetime

# MySQL connection configuration
mysql_config = {
    "host": "localhost",
    "port": 3306,
    "user": "root",
    "password": "rootroot",
    "database": "org"
}


# Define functions
def mysql_check_nulls(schema_name):
    conn = mysql.connector.connect(**mysql_config)
    cursor = conn.cursor()

    # Get list of all tables
    query = f"SELECT table_name FROM information_schema.tables WHERE table_schema = '{schema_name}'"
    cursor.execute(query)
    table_list = [row[0] for row in cursor.fetchall()]

    null_counts = []

    # Iterate over all tables and their columns
    for table in table_list:
        # Get list of all columns for the table
        column_query = f"SELECT column_name FROM information_schema.columns WHERE table_schema = '{schema_name}' AND table_name = '{table}'"
        cursor.execute(column_query)
        column_list = [row[0] for row in cursor.fetchall()]

        for column in column_list:
            # Count nulls in each column
            null_count_query = f"SELECT COUNT(*) as null_count FROM `{schema_name}`.`{table}` WHERE `{column}` IS NULL"
            cursor.execute(null_count_query)
            null_count = cursor.fetchone()[0]
            null_counts.append((table, column, null_count))

    cursor.close()
    conn.close()

    # Convert list to DataFrame
    null_counts_df = pd.DataFrame(null_counts, columns=["table_name", "column_name", "number_of_nulls"])

    return null_counts_df


def mysql_check_nulls_table(schema_name, table_name, column_name):
    conn = mysql.connector.connect(**mysql_config)
    cursor = conn.cursor()

    null_count_query = f"SELECT COUNT(*) as null_count FROM `{schema_name}`.`{table_name}` WHERE `{column_name}` IS NULL"
    cursor.execute(null_count_query)
    null_count = cursor.fetchone()[0]

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

    cursor.close()
    conn.close()

    return df


def mysql_find_duplicates(schema_name, table_name, column_name):
    conn = mysql.connector.connect(**mysql_config)
    cursor = conn.cursor()

    query = f"SELECT {column_name}, COUNT(*) c FROM {schema_name}.{table_name} GROUP BY {column_name} HAVING c > 1"
    cursor.execute(query)
    result = cursor.fetchall()

    if len(result) == 0:
        duplicate_check = 'PASS'
        number_of_duplicates = 0
    else:
        duplicate_check = 'FAIL'
        number_of_duplicates = sum(row[1] for row in result)

    results = pd.DataFrame({
        'schema_name': [schema_name],
        'column_name': [column_name],
        'duplicate_check': [duplicate_check],
        'number_of_duplicates': [number_of_duplicates],
        'run_date': [datetime.now()]
    })

    cursor.close()
    conn.close()

    return results


# Function to get columns from a table
def get_columns(schema_name, table_name):
    conn = mysql.connector.connect(**mysql_config)
    cursor = conn.cursor()
    column_query = f"SELECT column_name FROM information_schema.columns WHERE table_schema = '{schema_name}' AND table_name = '{table_name}'"
    cursor.execute(column_query)
    columns = [row[0] for row in cursor.fetchall()]
    cursor.close()
    conn.close()
    return columns


# Function for null check
def null_check_from_excel(df1):
    df_results = pd.DataFrame()

    for index, row in df1.iterrows():
        schema_name = row['schema_name']
        table_name = row['table_name']
        columns = get_columns(schema_name, table_name)

        for column_name in columns:
            conn = mysql.connector.connect(**mysql_config)
            cursor = conn.cursor()
            null_count_query = f'SELECT COUNT(*) as null_count FROM `{schema_name}`.`{table_name}` WHERE `{column_name}` IS NULL'
            cursor.execute(null_count_query)
            null_count = cursor.fetchone()[0]
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
            cursor.close()
            conn.close()

    return df_results


# Function for duplicate check
def find_duplicates(df1):
    results = pd.DataFrame()

    for index, row in df1.iterrows():
        schema_name = row['schema_name']
        table_name = row['table_name']
        key_column = row['key_column']
        conn = mysql.connector.connect(**mysql_config)
        cursor = conn.cursor()
        query = f"SELECT `{key_column}`, COUNT(*) c FROM `{schema_name}`.`{table_name}` GROUP BY `{key_column}` HAVING c > 1"
        cursor.execute(query)
        duplicates = cursor.fetchall()

        if len(duplicates) == 0:
            duplicate_check = 'PASS'
            number_of_duplicates = 0
        else:
            duplicate_check = 'FAIL'
            number_of_duplicates = sum(row[1] for row in duplicates)

        result = pd.DataFrame({
            'schema_name': [schema_name],
            'key_column': [key_column],
            'duplicate_check': [duplicate_check],
            'number_of_duplicates': [number_of_duplicates],
            'run_date': [datetime.now().strftime('%Y-%m-%d %H:%M:%S')]
        })
        results = pd.concat([results, result])
        cursor.close()
        conn.close()

    return results


# Function for data validation
def data_validation(df):
    results = pd.DataFrame()

    for index, row in df.iterrows():
        s_schema_name = row['s_schema_name']
        s_tb_name = row['s_tb_name']
        s_col_name = row['s_col_name'].split(',')
        t_schema_name = row['t_schema_name']
        t_tb_name = row['t_tb_name']
        t_col_name = row['t_col_name'].split(',')
        source = f"`{s_schema_name}`.`{s_tb_name}`"
        target = f"`{t_schema_name}`.`{t_tb_name}`"

        conn = mysql.connector.connect(**mysql_config)
        cursor = conn.cursor()

        query1 = f"SELECT {', '.join([f'`{col}`' for col in s_col_name])} FROM {source}"
        query2 = f"SELECT {', '.join([f'`{col}`' for col in t_col_name])} FROM {target}"

        cursor.execute(query1)
        df1 = pd.DataFrame(cursor.fetchall(), columns=s_col_name)
        cursor.execute(query2)
        df2 = pd.DataFrame(cursor.fetchall(), columns=t_col_name)

        result1_diff = df1[~df1.isin(df2).all(axis=1)]
        result2_diff = df2[~df2.isin(df1).all(axis=1)]

        a_b_count_minus = len(result1_diff)
        b_a_count_minus = len(result2_diff)

        a_b = 'PASS' if a_b_count_minus == 0 else 'FAIL'
        b_a = 'PASS' if b_a_count_minus == 0 else 'FAIL'

        diff_columns = ', '.join(result1_diff.columns.tolist())

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

        cursor.close()
        conn.close()

    return results


def mysql_full_data_validation():
    excel_file = "/Users/kkrishna/Library/CloudStorage/OneDrive-InfobloxInc/hackthon-2024/Project/data_dreamer_24-jun/data_dreamers/_raw/mysql_validationcheck.xlsx"
    result_file_path = "/Users/kkrishna/Library/CloudStorage/OneDrive-InfobloxInc/hackthon-2024/Project/data_dreamer_24-jun/data_dreamers/result/mysql_full_data_validation_results.xlsx"

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

    return 'success'


