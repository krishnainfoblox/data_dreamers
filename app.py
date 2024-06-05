from flask import Flask, render_template, request
import pandas as pd
import time
from main import check_nulls, check_nulls_table, find_duplicates, full_data_validation

app = Flask(__name__)

@app.route('/')
def home():
    return render_template('index.html')

@app.route('/null-validation-full-schema', methods=['GET', 'POST'])
def null_validation_full_schema_view():
    result = None
    if request.method == 'POST':
        schema_name = request.form['schema']
        start_time = time.time()
        data = check_nulls(schema_name)
        result = data.to_html(classes='table table-striped')
        elapsed_time = time.time() - start_time
    else:
        elapsed_time = None
    return render_template('null_validation_full_schema.html', result=result, elapsed_time=elapsed_time)

@app.route('/null-validation-specific-table', methods=['GET', 'POST'])
def null_validation_specific_table_view():
    result = None
    if request.method == 'POST':
        schema_name = request.form['schema']
        table_name = request.form['table']
        column_name = request.form['column']
        start_time = time.time()
        data = check_nulls_table(schema_name, table_name, column_name)
        result = data.to_html(classes='table table-striped')
        elapsed_time = time.time() - start_time
    else:
        elapsed_time = None
    return render_template('null_validation_specific_table.html', result=result, elapsed_time=elapsed_time)

@app.route('/primary-key-check', methods=['GET', 'POST'])
def duplicate_validation_view():
    result = None
    if request.method == 'POST':
        schema_name = request.form['schema']
        table_name = request.form['table']
        column_name = request.form['column']
        start_time = time.time()
        data = find_duplicates(schema_name, table_name, column_name)
        result = data.to_html(classes='table table-striped')
        elapsed_time = time.time() - start_time
    else:
        elapsed_time = None
    return render_template('duplicate_validation.html', result=result, elapsed_time=elapsed_time)

@app.route('/full-data-validation', methods=['GET', 'POST'])
def full_data_validation_view():
    result = None
    if request.method == 'POST':
        excel_file = request.files['excel_file']
        start_time = time.time()
        data = full_data_validation(excel_file)
        result = data.to_html(classes='table table-striped')
        elapsed_time = time.time() - start_time
    else:
        elapsed_time = None
    return render_template('full_data_validation.html', result=result, elapsed_time=elapsed_time)

if __name__ == '__main__':
    app.run(debug=True)
