from datetime import datetime
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
import logging

# Task 1 & 2: We use PostgresOperator for direct SQL (Cleaner)
# Task 3: We use PythonOperator + Hook for processing/logging results

def read_data_callable():
    hook = PostgresHook(postgres_conn_id='postgres_default')
    # get_records returns a list of tuples
    records = hook.get_records("SELECT * FROM employees;")
    
    if not records:
        logging.info("No records found.")
        return

    for row in records:
        # row[0]=id, row[1]=name, row[2]=dept, row[3]=created_date
        logging.info(f"ID: {row[0]} | Name: {row[1]} | Dept: {row[2]} | Created: {row[3]}")

with DAG(
    dag_id="psql_final_solution",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["postgres", "debugged"]
) as dag:

    # 1. Create Table with proper DEFAULT
    create_table = PostgresOperator(
        task_id="create_table",
        postgres_conn_id="postgres_default",
        sql="""
            CREATE TABLE IF NOT EXISTS employees (
                id SERIAL PRIMARY KEY,
                name TEXT NOT NULL,
                department TEXT,
                created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """
    )

    # 2. Insert Data (Not providing created_date so the DEFAULT kicks in)
    insert_data = PostgresOperator(
        task_id="insert_data",
        postgres_conn_id="postgres_default",
        sql="""
            INSERT INTO employees (name, department)
            VALUES ('Somnath', 'Data Engineering');
        """
    )

    # 3. Read and Log Data
    read_data = PythonOperator(
        task_id="read_data",
        python_callable=read_data_callable
    )

    create_table >> insert_data >> read_data