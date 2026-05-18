from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime
from pymongo import MongoClient
import requests
import json

# MongoDB connection details
MONGO_URI = "mongodb://localhost:27017/"
DB_NAME = "airflow_db"
COLLECTION = "employees"

# Define the DAG
dag = DAG(
    dag_id="API_demo_dag",
    start_date=datetime(2026, 1, 1),
    schedule='0 0 * * *',   # ✅ new style in Airflow 3.x
    catchup=False
)

def fetch_api(**kwargs):
    url = "http://localhost:8000/getAll"   # FastAPI endpoint
    payload = {
        "start_date": "2026-01-01",
        "end_date": "2026-12-31"
    }
    headers = {
        'accept': "application/json",
        'Content-Type': 'application/json'
    }

    # Call API
    response = requests.post(url, json=payload, headers=headers)
    data = response.json()
    print("API Response:", data)

    # Save to MongoDB
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]
    collection = db[COLLECTION]

    # Insert response (single doc or list)
    if isinstance(data, list):
        collection.insert_many(data)
    else:
        collection.insert_one(data)

    print("Data saved to MongoDB successfully!")

# PythonOperator to run the function
pull_api_data = PythonOperator(
    task_id="pull_api_data",
    python_callable=fetch_api,
    dag=dag
)
