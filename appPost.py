from flask import Flask, request, jsonify
import csv
import psycopg2
from psycopg2 import sql
import io

app = Flask(__name__)

# PostgreSQL database connection
conn = psycopg2.connect(
    dbname="Globant",
    user="postgres",
    password="postgres",
    host="localhost"
)
cursor = conn.cursor()

# Endpoint to receive historical data from CSV files and upload to the new DB
@app.route('/upload', methods=['POST'])
def upload_csv():
    # Debugging code to print keys in request.files
    print(request.files.keys())
    # Check if the request contains files
    if 'file_departments' not in request.files or 'file_jobs' not in request.files or 'file_employees' not in request.files:
        return jsonify({'error': 'No file part'})

    # Check if file is not empty
    file_departments = request.files['file_departments']
    if file_departments.filename == '':
        return jsonify({'error': 'No se ha seleccionado un archivo para departamentos'})

    file_jobs = request.files['file_jobs']
    if file_jobs.filename == '':
        return jsonify({'error': 'No se ha seleccionado un archivo para trabajos'})

    file_employees = request.files['file_employees']
    if file_employees.filename == '':
        return jsonify({'error': 'No se ha seleccionado un archivo para empleados'})

    # Read the CSV file
    text_stream_departments = io.TextIOWrapper(file_departments.stream, encoding='utf-8')
    csv_data_departments = csv.reader(text_stream_departments)

    text_stream_jobs = io.TextIOWrapper(file_jobs.stream, encoding='utf-8')
    csv_data_jobs = csv.reader(text_stream_jobs)

    text_stream_employees = io.TextIOWrapper(file_employees.stream, encoding='utf-8')
    csv_data_employees = csv.reader(text_stream_employees)

    def record_exists(table_name, primary_key, values):
        query = sql.SQL("SELECT EXISTS (SELECT 1 FROM {} WHERE {} = %s)").format(
            sql.Identifier(table_name),
            sql.Identifier(primary_key)
        )
        cursor.execute(query, (values,))
        return cursor.fetchone()[0]
  

    # Iterate through each row in the CSV and insert into the corresponding table
    for row in csv_data_departments:
        if not record_exists('departments', 'department_id', row[0]):
        # Insert into departments table
            cursor.execute(sql.SQL("INSERT INTO departments (department_id, department_name) VALUES (%s, %s)"), (row[0], row[1]))
    for row in csv_data_jobs: 
        if not record_exists('jobs', 'job_id', row[0]):   
        # Insert into jobs table
            cursor.execute(sql.SQL("INSERT INTO jobs (job_id, job_title, min_salary, max_salary) VALUES (%s, %s, %s, %s)"), (row[0], row[1], row[2], row[3]))
    for row in csv_data_employees: 
        if not record_exists('employees', 'employee_id', row[0]):   
        # Insert into employees table
            cursor.execute(sql.SQL("INSERT INTO employees (employee_id, first_name, last_name, email, phone_number, hire_date, job_id, salary, manager_id, department_id) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"), (row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[9]))

    conn.commit()

    return jsonify({'message': 'CSV data uploaded successfully'})


if __name__ == '__main__':
    app.run(debug=True)







