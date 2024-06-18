'''

pip install mysql-connector-python


'''

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
def check_nulls(schema_name):
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


def check_nulls_table(schema_name, table_name, column_name):
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


def find_duplicates(schema_name, table_name, column_name):
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


def full_data_validation(excel_file):
    conn = mysql.connector.connect(**mysql_config)
    cursor = conn.cursor()

    df1 = pd.read_excel(excel_file, sheet_name='Sheet1')
    df2 = pd.read_excel(excel_file, sheet_name='Sheet2')

    all_results = []

    for index, row1 in df1.iterrows():
        schema_name1 = row1['schema_name']
        table_name1 = row1['table_name']
        column_names1 = row1['column_name'].split(',')

        for index, row2 in df2.iterrows():
            schema_name2 = row2['schema_name']
            table_name2 = row2['table_name']
            column_names2 = row2['column_name'].split(',')

            query1 = f"SELECT {', '.join(column_names1)} FROM {schema_name1}.{table_name1}"
            query2 = f"SELECT {', '.join(column_names2)} FROM {schema_name2}.{table_name2}"

            df1_mysql = pd.read_sql_query(query1, conn)
            df2_mysql = pd.read_sql_query(query2, conn)

            result1 = df1_mysql.merge(df2_mysql, how='left', indicator=True)
            result2 = df2_mysql.merge(df1_mysql, how='left', indicator=True)

            a_b = 'PASS' if result1['_merge'].eq('left_only').all() else 'FAIL'
            a_b_count_minus = result1['_merge'].eq('left_only').sum()

            b_a = 'PASS' if result2['_merge'].eq('left_only').all() else 'FAIL'
            b_a_count_minus = result2['_merge'].eq('left_only').sum()

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

    cursor.close()
    conn.close()

    return pd.concat(all_results)
