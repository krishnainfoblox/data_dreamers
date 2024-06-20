"""
CODER - KRISHNA
INFOBLOX 2024 - HACKATHON/HACK FEST

-----------------------------------
---- LIBRARIES TO USE ----
-----------------------------------
# pip install pyspark
# pip install pandas
# pip install openpyxl
# pip install datetime
# pip install pandas openpyxl psycopg2-binary
# pip install flask
# pip install
# pip install XlsxWriter
# pip install pandas openpyxl xlrd
# pip install mysql-connector-python

"""

from flask import Flask, render_template, request, redirect, url_for, flash, jsonify
import pandas as pd
import time
import os
from awsrs import aws_check_nulls, aws_check_nulls_table, aws_find_duplicates, aws_full_data_validation
from mysqldb import mysql_check_nulls, mysql_check_nulls_table, mysql_find_duplicates, mysql_full_data_validation
from filevalidation import read_csv, read_json, read_parquet, comparedf

app = Flask(__name__)
app.config['UPLOAD_FOLDER'] = 'uploads'


@app.route('/')
def home():
    return render_template('index.html')


@app.route('/aws')
def aws_home():
    return render_template('aws.html')


@app.route('/mysql')
def mysql_home():
    return render_template('mysql.html')


@app.route('/file')
def file_home():
    return render_template('file_validation.html')


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
        excel_file = request.files['excel_file']
        start_time = time.time()
        data = mysql_full_data_validation(excel_file)
        result = data.to_html(classes='table table-striped')
        elapsed_time = time.time() - start_time
    return render_template('004_MySQL_full_data_validation.html', result=result, elapsed_time=elapsed_time)


@app.route('/file_001_csv_csv', methods=['GET', 'POST'])
def file_csv_csv_view():
    result = None
    elapsed_time = None

    if request.method == 'POST':
        if 'file1' not in request.files or 'file2' not in request.files:
            flash('No file part')
            return redirect(request.url)

        file1 = request.files['file1']
        file2 = request.files['file2']
        primary_key = request.form['primary_key']

        if file1.filename == '' or file2.filename == '':
            flash('No selected file')
            return redirect(request.url)

        file1_path = os.path.join(app.config['UPLOAD_FOLDER'], file1.filename)
        file2_path = os.path.join(app.config['UPLOAD_FOLDER'], file2.filename)

        file1.save(file1_path)
        file2.save(file2_path)

        start_time = time.time()
        df1 = read_csv(file1_path)
        df2 = read_csv(file2_path)
        result = comparedf(df1, df2, primary_key)
        elapsed_time = time.time() - start_time

    return render_template('file_001_csv_csv.html', result=result, elapsed_time=elapsed_time)


@app.route('/file_002_csv_json', methods=['GET', 'POST'])
def file_csv_json_view():
    result = None
    elapsed_time = None
    if request.method == 'POST':
        file1 = request.files['file1']
        file2 = request.files['file2']
        primary_key = request.form['primary_key']

        if file1 and file2:
            file1_path = os.path.join(app.config['UPLOAD_FOLDER'], file1.filename)
            file2_path = os.path.join(app.config['UPLOAD_FOLDER'], file2.filename)
            file1.save(file1_path)
            file2.save(file2_path)

            srcdf = read_csv(file1_path)
            tgtdf = read_json(file2_path)

            result_df = comparedf(srcdf, tgtdf, primary_key)
            result = result_df.to_html(classes='table table-striped')

            os.remove(file1_path)
            os.remove(file2_path)

            elapsed_time = time.time() - start_time

    return render_template('file_002_csv_json.html', result=result, elapsed_time=elapsed_time)


@app.route('/file_003_csv_parquet', methods=['GET', 'POST'])
def file_csv_parquet_view():
    result = None
    elapsed_time = None
    if request.method == 'POST':
        file1 = request.files['file1']
        file2 = request.files['file2']
        primary_key = request.form['primary_key']

        if file1 and file2:
            file1_path = os.path.join(app.config['UPLOAD_FOLDER'], file1.filename)
            file2_path = os.path.join(app.config['UPLOAD_FOLDER'], file2.filename)
            file1.save(file1_path)
            file2.save(file2_path)

            srcdf = read_csv(file1_path)
            tgtdf = read_parquet(file2_path)

            result_df = comparedf(srcdf, tgtdf, primary_key)
            result = result_df.to_html(classes='table table-striped')

            os.remove(file1_path)
            os.remove(file2_path)

            elapsed_time = time.time() - start_time

    return render_template('file_003_csv_parquet.html', result=result, elapsed_time=elapsed_time)


@app.route('/file_004_json_json', methods=['GET', 'POST'])
def file_json_json_view():
    result = None
    elapsed_time = None
    if request.method == 'POST':
        file1 = request.files['file1']
        file2 = request.files['file2']
        primary_key = request.form['primary_key']

        if file1 and file2:
            file1_path = os.path.join(app.config['UPLOAD_FOLDER'], file1.filename)
            file2_path = os.path.join(app.config['UPLOAD_FOLDER'], file2.filename)
            file1.save(file1_path)
            file2.save(file2_path)

            srcdf = read_json(file1_path)
            tgtdf = read_json(file2_path)

            result_df = comparedf(srcdf, tgtdf, primary_key)
            result = result_df.to_html(classes='table table-striped')

            os.remove(file1_path)
            os.remove(file2_path)

            elapsed_time = time.time() - start_time

    return render_template('file_004_json_json.html', result=result, elapsed_time=elapsed_time)


@app.route('/file_005_json_parquet', methods=['GET', 'POST'])
def file_json_parquet_view():
    result = None
    elapsed_time = None
    if request.method == 'POST':
        file1 = request.files['file1']
        file2 = request.files['file2']
        primary_key = request.form['primary_key']

        if file1 and file2:
            file1_path = os.path.join(app.config['UPLOAD_FOLDER'], file1.filename)
            file2_path = os.path.join(app.config['UPLOAD_FOLDER'], file2.filename)
            file1.save(file1_path)
            file2.save(file2_path)

            srcdf = read_json(file1_path)
            tgtdf = read_parquet(file2_path)

            result_df = comparedf(srcdf, tgtdf, primary_key)
            result = result_df.to_html(classes='table table-striped')

            os.remove(file1_path)
            os.remove(file2_path)

            elapsed_time = time.time() - start_time

    return render_template('file_005_json_parquet.html', result=result, elapsed_time=elapsed_time)


@app.route('/file_006_parquet_parquet', methods=['GET', 'POST'])
def file_parquet_parquet_view():
    result = None
    elapsed_time = None
    if request.method == 'POST':
        file1 = request.files['file1']
        file2 = request.files['file2']
        primary_key = request.form['primary_key']

        if file1 and file2:
            file1_path = os.path.join(app.config['UPLOAD_FOLDER'], file1.filename)
            file2_path = os.path.join(app.config['UPLOAD_FOLDER'], file2.filename)
            file1.save(file1_path)
            file2.save(file2_path)

            srcdf = read_parquet(file1_path)
            tgtdf = read_parquet(file2_path)

            result_df = comparedf(srcdf, tgtdf, primary_key)
            result = result_df.to_html(classes='table table-striped')

            os.remove(file1_path)
            os.remove(file2_path)

            elapsed_time = time.time() - start_time

    return render_template('file_006_parquet_parquet.html', result=result, elapsed_time=elapsed_time)


if __name__ == '__main__':
    app.run(debug=True)
