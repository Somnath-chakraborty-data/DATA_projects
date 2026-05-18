from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime
from pymongo import MongoClient
import json

# MongoDB connection details
MONGO_URI = "mongodb://localhost:27017/"
DB_NAME = "airflow_db"
COLLECTION = "employees"

# Define the DAG
dag = DAG(
    dag_id="MongoAPI_dag",
    start_date=datetime(2026, 1, 1),
    schedule='0 0 * * *',   # run daily at midnight
    catchup=False
)

def fetch_api(**kwargs):
    # Use Airflow macros for dynamic date
    execution_date = kwargs['ds']   # DAG run date (YYYY-MM-DD)

    # Dummy API response
    dummy_data = {
        "employees": [
            {"id": 1, "name": "Alice", "role": "Engineer"},
            {"id": 2, "name": "Bob", "role": "Manager"},
            {"id": 3, "name": "Charlie", "role": "Analyst"}
        ]
    }

    print("Dummy API Response:", dummy_data)

    # Save to MongoDB with date key
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]
    collection = db[COLLECTION]

    document = {
        "date": execution_date,
        "data": dummy_data
    }

    collection.insert_one(document)
    print(f"Dummy data saved to MongoDB for {execution_date}!")

# PythonOperator to run the function
pull_api_data = PythonOperator(
    task_id="pull_api_data",
    python_callable=fetch_api,
    dag=dag
)

