"""
CODER - KRISHNA
INFOBLOX 2024 - HACKATHON/HACK FEST

-----------------------------------
---- LIBRARIES TO USE ----
-----------------------------------
# pip install pyspark pandas openpyxl datetime psycopg2-binary flask XlsxWriter xlrd mysql-connector-python

# brew install node

OPEN CHROME in debugging mode -
/Applications/Google\ Chrome.app/Contents/MacOS/Google\ Chrome --remote-debugging-port=9222


"""
import subprocess

from flask import Flask, render_template, request, redirect, url_for, flash, jsonify
import pandas as pd
import time
import os
import subprocess
from awsrs import aws_check_nulls, aws_check_nulls_table, aws_find_duplicates, aws_full_data_validation
from mysqldb import mysql_check_nulls, mysql_check_nulls_table, mysql_find_duplicates, mysql_full_data_validation
from filevalidation import read_csv, read_json, read_parquet, comparedf

app = Flask(__name__)

UPLOAD_FOLDER = 'uploads'
os.makedirs(UPLOAD_FOLDER, exist_ok=True)
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER


@app.route('/')
def home():
    return render_template('index.html')


@app.route('/aws')
def aws_home():
    return render_template('aws.html')


@app.route('/mysql')
def mysql_home():
    return render_template('mysql.html')


frontend_path = '/Users/kkrishna/Library/CloudStorage/OneDrive-InfobloxInc/hackthon-2024/Project/data_dreamers/frontend'
backend_path = '/Users/kkrishna/Library/CloudStorage/OneDrive-InfobloxInc/hackthon-2024/Project/data_dreamers/backend'


@app.route('/bi-performance-testing')
def bi_performance():
    return redirect('http://localhost:5173/')


@app.route('/bi-Validation')
def download_crosstab():
    return render_template('bi_download_crosstab.html')


@app.route('/file')
def file_home():
    return render_template('validate_files.html')


@app.route('/001_AWS_null-validation-full-schema', methods=['GET', 'POST'])
def aws_null_validation_full_schema_view():
    result = None
    elapsed_time = None
    if request.method == 'POST':
        schema_name = request.form['schema']
        start_time = time.time()
        data = aws_check_nulls(schema_name)
        result = data.to_html(classes='table table-striped')
        elapsed_time = time.time() - start_time
    return render_template('001_AWS_null_validation_full_schema.html', result=result, elapsed_time=elapsed_time)


@app.route('/002_AWS_null-validation-specific-table', methods=['GET', 'POST'])
def aws_null_validation_specific_table_view():
    result = None
    elapsed_time = None
    if request.method == 'POST':
        schema_name = request.form['schema']
        table_name = request.form['table']
        column_name = request.form['column']
        start_time = time.time()
        data = aws_check_nulls_table(schema_name, table_name, column_name)
        result = data.to_html(classes='table table-striped')
        elapsed_time = time.time() - start_time
    return render_template('002_AWS_null_validation_specific_table.html', result=result, elapsed_time=elapsed_time)


@app.route('/003_AWS_primary-key-check', methods=['GET', 'POST'])
def aws_duplicate_validation_view():
    result = None
    elapsed_time = None
    if request.method == 'POST':
        schema_name = request.form['schema']
        table_name = request.form['table']
        column_name = request.form['column']
        start_time = time.time()
        data = aws_find_duplicates(schema_name, table_name, column_name)
        result = data.to_html(classes='table table-striped')
        elapsed_time = time.time() - start_time
    return render_template('003_AWS_duplicate_validation.html', result=result, elapsed_time=elapsed_time)


@app.route('/004_AWS_full-data-validation', methods=['GET', 'POST'])
def aws_full_data():
    result = None
    elapsed_time = None
    if request.method == 'POST':
        start_time = time.time()
        aws_full_data_validation()
        elapsed_time = time.time() - start_time
    return render_template('004_AWS_full_data_validation.html', result=result, elapsed_time=elapsed_time)


@app.route('/001_MYSQL_null-validation-full-schema', methods=['GET', 'POST'])
def mysql_null_validation_full_schema_view():
    result = None
    elapsed_time = None
    if request.method == 'POST':
        schema_name = request.form['schema']
        start_time = time.time()
        data = mysql_check_nulls(schema_name)
        result = data.to_html(classes='table table-striped')
        elapsed_time = time.time() - start_time
    return render_template('001_MySQL_null_validation_full_schema.html', result=result, elapsed_time=elapsed_time)


@app.route('/002_MYSQL_null-validation-specific-table', methods=['GET', 'POST'])
def mysql_null_validation_specific_table_view():
    result = None
    elapsed_time = None
    if request.method == 'POST':
        schema_name = request.form['schema']
        table_name = request.form['table']
        column_name = request.form['column']
        start_time = time.time()
        data = mysql_check_nulls_table(schema_name, table_name, column_name)
        result = data.to_html(classes='table table-striped')
        elapsed_time = time.time() - start_time
    return render_template('002_MySQL_null_validation_specific_table.html', result=result, elapsed_time=elapsed_time)


@app.route('/003_MYSQL_primary-key-check', methods=['GET', 'POST'])
def mysql_duplicate_validation_view():
    result = None
    elapsed_time = None
    if request.method == 'POST':
        schema_name = request.form['schema']
        table_name = request.form['table']
        column_name = request.form['column']
        start_time = time.time()
        data = mysql_find_duplicates(schema_name, table_name, column_name)
        result = data.to_html(classes='table table-striped')
        elapsed_time = time.time() - start_time
    return render_template('003_MySQL_duplicate_validation.html', result=result, elapsed_time=elapsed_time)


@app.route('/004_MYSQL_full-data-validation', methods=['GET', 'POST'])
def mysql_full_data_validation_view():
    result = None
    elapsed_time = None
    if request.method == 'POST':
        start_time = time.time()
        mysql_full_data_validation()
        elapsed_time = time.time() - start_time
    return render_template('004_MySQL_full_data_validation.html', result=result, elapsed_time=elapsed_time)


@app.route('/file-validation', methods=['GET', 'POST'])
def file_validation_route():
    if request.method == 'POST':
        # Check if the POST request has the file part
        if 'file1' not in request.files or 'file2' not in request.files:
            return "No file part in the request"

        file1 = request.files['file1']
        file2 = request.files['file2']
        file1_type = request.form['file1_type']
        file2_type = request.form['file2_type']
        primary_key = request.form['primary_key']

        # If user does not select file, browser also submits an empty part without filename
        if file1.filename == '' or file2.filename == '':
            return "No selected file"

        file1_path = os.path.join(app.config['UPLOAD_FOLDER'], file1.filename)
        file2_path = os.path.join(app.config['UPLOAD_FOLDER'], file2.filename)

        file1.save(file1_path)
        file2.save(file2_path)

        # Trigger the PySpark script
        subprocess.run(['python3', 'filevalidation.py', file1_path, file1_type, file2_path, file2_type, primary_key])

        return "Validation completed. Check the results."

    # If method is GET, render the form
    return render_template('validate_files.html')


@app.route('/file-to-database-validation')
def file_vs_db():
    return render_template('file_to_database_validation.html')


if __name__ == '__main__':
    app.run(debug=True)
