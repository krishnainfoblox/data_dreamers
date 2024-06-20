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


def mysql_null_validation_table(schema_name, table_name):
    conn = mysql.connector.connect(**mysql_config)
    cursor = conn.cursor()

    try:
        # Get list of columns for the specified table
        column_query = f"SELECT column_name FROM information_schema.columns WHERE table_schema = '{schema_name}' AND table_name = '{table_name}'"
        cursor.execute(column_query)
        columns = [row[0] for row in cursor.fetchall()]

        null_counts = []

        # Iterate over each column and count nulls
        for column_name in columns:
            null_count_query = f"SELECT COUNT(*) as null_count FROM `{schema_name}`.`{table_name}` WHERE `{column_name}` IS NULL"
            cursor.execute(null_count_query)
            null_count = cursor.fetchone()[0]
            null_check = "FAIL" if null_count > 0 else "PASS"
            run_date = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

            # Append result to null_counts list
            null_counts.append({
                "schema_name": schema_name,
                "table_name": table_name,
                "column_name": column_name,
                "null_check": null_check,
                "number_of_nulls": null_count,
                "run_date": run_date
            })

        # Close cursor and connection
        cursor.close()
        conn.close()

        # Convert list of dictionaries to DataFrame
        null_counts_df = pd.DataFrame(null_counts)

        return null_counts_df

    except mysql.connector.Error as err:
        print(f"Error: {err}")
        return pd.DataFrame()


def mysql_data_validation(s_schema_name, s_tb_name, s_col_name, t_schema_name, t_tb_name, t_col_name):
    conn = mysql.connector.connect(**mysql_config)
    cursor = conn.cursor()

    try:
        # Construct queries for source and target tables
        query1 = f"SELECT {', '.join(s_col_name)} FROM {s_schema_name}.{s_tb_name}"
        query2 = f"SELECT {', '.join(t_col_name)} FROM {t_schema_name}.{t_tb_name}"

        # Execute queries
        cursor.execute(query1)
        result1 = cursor.fetchall()
        cursor.execute(query2)
        result2 = cursor.fetchall()

        # Convert results to DataFrames
        df1 = pd.DataFrame(result1, columns=s_col_name)
        df2 = pd.DataFrame(result2, columns=t_col_name)

        # Perform data validation
        result1_diff = df1[~df1.isin(df2)].dropna()
        result2_diff = df2[~df2.isin(df1)].dropna()

        a_b_count_minus = len(result1_diff)
        b_a_count_minus = len(result2_diff)

        a_b = 'PASS' if a_b_count_minus == 0 else 'FAIL'
        b_a = 'PASS' if b_a_count_minus == 0 else 'FAIL'

        diff_columns = ', '.join(result1_diff.columns.tolist())

        # Prepare result DataFrame
        results = pd.DataFrame({
            'source': [f"{s_schema_name}.{s_tb_name}"],
            'target': [f"{t_schema_name}.{t_tb_name}"],
            'a-b': [a_b_count_minus],
            'a-b-result': [a_b],
            'b-a': [b_a_count_minus],
            'b-a-result': [b_a],
            'diff_columns': [diff_columns],
            'run_date': [datetime.now().strftime('%Y-%m-%d %H:%M:%S')]
        })

        return results

    except mysql.connector.Error as err:
        print(f"Error: {err}")
        return pd.DataFrame()

    finally:
        # Close cursor and connection
        cursor.close()
        conn.close()


def mysql_full_data_validation():
    excel_file = "/Users/kkrishna/Library/CloudStorage/OneDrive-InfobloxInc/hackthon-2024/Project/code-hackathon-2024/_raw/mysql_validationcheck.xlsx"
    result_file_path = "/Users/kkrishna/Library/CloudStorage/OneDrive-InfobloxInc/hackthon-2024/Project/code-hackathon-2024/result/mysql_full_data_validation_results.xlsx"

    # Read Excel file containing sheets for null validation, primary key check, and data validation
    df = pd.read_excel(excel_file, sheet_name=None)

    results = []

    # Null Validation
    if 'Sheet1' in df:
        null_df = df['Sheet1']
        for index, row in null_df.iterrows():
            schema_name = row['schema_name']
            table_name = row['table_name']

            result = mysql_null_validation_table(schema_name, table_name)
            results.append(result)

    # Primary Key Check
    if 'Sheet2' in df:
        duplicate_df = df['Sheet2']
        for index, row in duplicate_df.iterrows():
            schema_name = row['schema_name']
            table_name = row['table_name']
            key_column = row['key_column']

            duplicates = mysql_find_duplicates(schema_name, table_name, key_column)

            results.append(result)

    # Data Validation
    if 'Sheet3' in df:
        data_validation_df = df['Sheet3']
        for index, row in data_validation_df.iterrows():
            s_schema_name = row['s_schema_name']
            s_tb_name = row['s_tb_name']
            s_col_name = row['s_col_name'].split(',')
            t_schema_name = row['t_schema_name']
            t_tb_name = row['t_tb_name']
            t_col_name = row['t_col_name'].split(',')

            a_b, a_b_count_minus, b_a_count_minus, diff_columns = mysql_data_validation(
                s_schema_name, s_tb_name, s_col_name, t_schema_name, t_tb_name, t_col_name
            )

            result = {
                'Validation Type': 'Data Validation',
                'Source': f"{s_schema_name}.{s_tb_name}",
                'Target': f"{t_schema_name}.{t_tb_name}",
                'A - B Count': a_b_count_minus,
                'A - B Result': a_b,
                'B - A Count': b_a_count_minus,
                'Diff Columns': diff_columns,
                'Run Date': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            }
            results.append(result)

    # Convert results to DataFrame
    results_df = pd.DataFrame(results)

    # Write results to an Excel file with multiple sheets
    result_file_path = "/path/to/save/mysql_full_data_validation_results.xlsx"
    with pd.ExcelWriter(result_file_path, engine='xlsxwriter') as writer:
        for validation_type, group in results_df.groupby('Validation Type'):
            group.to_excel(writer, sheet_name=validation_type, index=False)

    return ('success')


mysql_full_data_validation()
