from flask import Flask, render_template, request
import pandas as pd
import time
from awsrs import aws_check_nulls, aws_check_nulls_table, aws_find_duplicates, aws_full_data_validation
from mysqldb import mysql_check_nulls, mysql_check_nulls_table, mysql_find_duplicates, mysql_full_data_validation

app = Flask(__name__)


@app.route('/')
def home():
    return render_template('index.html')


@app.route('/001_AWS_null-validation-full-schema', methods=['GET', 'POST'])
def aws_null_validation_full_schema_view():
    result = None
    if request.method == 'POST':
        schema_name = request.form['schema']
        start_time = time.time()
        data = aws_check_nulls(schema_name)
        result = data.to_html(classes='table table-striped')
        elapsed_time = time.time() - start_time
    else:
        elapsed_time = None
    return render_template('001_AWS_null_validation_full_schema.html', result=result, elapsed_time=elapsed_time)


@app.route('/002_AWS_null-validation-specific-table', methods=['GET', 'POST'])
def aws_null_validation_specific_table_view():
    result = None
    if request.method == 'POST':
        schema_name = request.form['schema']
        table_name = request.form['table']
        column_name = request.form['column']
        start_time = time.time()
        data = aws_check_nulls_table(schema_name, table_name, column_name)
        result = data.to_html(classes='table table-striped')
        elapsed_time = time.time() - start_time
    else:
        elapsed_time = None
    return render_template('002_AWS_null_validation_specific_table.html', result=result, elapsed_time=elapsed_time)


@app.route('/003_AWS_primary-key-check', methods=['GET', 'POST'])
def aws_duplicate_validation_view():
    result = None
    if request.method == 'POST':
        schema_name = request.form['schema']
        table_name = request.form['table']
        column_name = request.form['column']
        start_time = time.time()
        data = aws_find_duplicates(schema_name, table_name, column_name)
        result = data.to_html(classes='table table-striped')
        elapsed_time = time.time() - start_time
    else:
        elapsed_time = None
    return render_template('003_AWS_duplicate_validation.html', result=result, elapsed_time=elapsed_time)


@app.route('/004_AWS_full-data-validation', methods=['GET', 'POST'])
def aws_full_data_validation_view():
    result = None
    if request.method == 'POST':
        excel_file = request.files['excel_file']
        start_time = time.time()
        data = aws_full_data_validation(excel_file)
        result = data.to_html(classes='table table-striped')
        elapsed_time = time.time() - start_time
    else:
        elapsed_time = None
    return render_template('004_AWS_full_data_validation.html', result=result, elapsed_time=elapsed_time)


@app.route('/001_MYSQL_null-validation-full-schema', methods=['GET', 'POST'])
def mysql_null_validation_full_schema_view():
    result = None
    if request.method == 'POST':
        schema_name = request.form['schema']
        start_time = time.time()
        data = mysql_check_nulls(schema_name)
        result = data.to_html(classes='table table-striped')
        elapsed_time = time.time() - start_time
    else:
        elapsed_time = None
    return render_template('001_MySQL_null_validation_full_schema.html', result=result, elapsed_time=elapsed_time)


@app.route('/002_MYSQL_null-validation-specific-table', methods=['GET', 'POST'])
def mysql_null_validation_specific_table_view():
    result = None
    if request.method == 'POST':
        schema_name = request.form['schema']
        table_name = request.form['table']
        column_name = request.form['column']
        start_time = time.time()
        data = mysql_check_nulls_table(schema_name, table_name, column_name)
        result = data.to_html(classes='table table-striped')
        elapsed_time = time.time() - start_time
    else:
        elapsed_time = None
    return render_template('002_MySQL_null_validation_specific_table.html', result=result, elapsed_time=elapsed_time)


@app.route('/003_MYSQL_primary-key-check', methods=['GET', 'POST'])
def mysql_duplicate_validation_view():
    result = None
    if request.method == 'POST':
        schema_name = request.form['schema']
        table_name = request.form['table']
        column_name = request.form['column']
        start_time = time.time()
        data = mysql_find_duplicates(schema_name, table_name, column_name)
        result = data.to_html(classes='table table-striped')
        elapsed_time = time.time() - start_time
    else:
        elapsed_time = None
    return render_template('003_MySQL_duplicate_validation.html', result=result, elapsed_time=elapsed_time)


@app.route('/004_MYSQL_full-data-validation', methods=['GET', 'POST'])
def mysql_full_data_validation_view():
    result = None
    if request.method == 'POST':
        excel_file = request.files['excel_file']
        start_time = time.time()
        data = mysql_full_data_validation(excel_file)
        result = data.to_html(classes='table table-striped')
        elapsed_time = time.time() - start_time
    else:
        elapsed_time = None
    return render_template('004_MySQL_full_data_validation.html', result=result, elapsed_time=elapsed_time)


if __name__ == '__main__':
    app.run(debug=True)
