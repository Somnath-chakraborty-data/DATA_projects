from datetime import datetime
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import logging

# We use logging.info to see output in the Airflow UI Task Logs
logger = logging.getLogger(__name__)

def create_table_func():
    hook = PostgresHook(postgres_conn_id='postgres_default')
    hook.run("""
        CREATE TABLE IF NOT EXISTS employees (
            id SERIAL PRIMARY KEY,
            name TEXT,
            department TEXT,
            created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """)
    logger.info("Table 'employees' is ready.")

def insert_data_func():
    hook = PostgresHook(postgres_conn_id='postgres_default')
    hook.run("""
        INSERT INTO employees (name, department)
        VALUES ('Somnath', 'Data Engineering');
    """)
    logger.info("Data inserted successfully.")

def read_data_func():
    hook = PostgresHook(postgres_conn_id='postgres_default')
    # Fetching all columns including the auto-generated timestamp
    records = hook.get_records("SELECT id, name, department, created_date FROM employees;")

    logger.info("--- FETCHING EMPLOYEE RECORDS ---")
    for row in records:
        # row[3] is the created_date timestamp
        logger.info(f"ID: {row[0]} | Name: {row[1]} | Dept: {row[2]} | TS: {row[3]}")
    logger.info("--- END OF RECORDS ---")

with DAG(
    dag_id="psql_hook_with_timestamp",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["postgres", "hook_test"]
) as dag:

    task1 = PythonOperator(
        task_id="create_table",
        python_callable=create_table_func
    )

    task2 = PythonOperator(
        task_id="insert_data",
        python_callable=insert_data_func
    )

    task3 = PythonOperator(
        task_id="read_data",
        python_callable=read_data_func
    )

    task1 >> task2 >> task3