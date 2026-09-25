"""
XCom — Cross Communication: como uma task manda valores para a próxima.

Aqui o XCom é usado na forma explícita (xcom_push / xcom_pull), para deixar
visível o que o decorator @task faz automaticamente em 02_dinamico_airflow.py.
"""
from __future__ import annotations

import json
from datetime import timedelta

import pendulum

from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator

# Dummy data
sales_data = [
    {'product': 'Laptop', 'quantity': 5, 'price': 1000},
    {'product': 'Mouse', 'quantity': 10, 'price': 50},
    {'product': 'Keyboard', 'quantity': 7, 'price': 100},
]


def extract_data(**kwargs):
    """Pushes sales data to XCom."""
    # A dag e a tarefa originadora vêm automaticamente
    kwargs['ti'].xcom_push(key='sales_data', value=sales_data)
    print("Sales data extracted.")


def process_data(**kwargs):
    """Processes sales data and stores results in XCom."""
    ti = kwargs['ti']
    data = ti.xcom_pull(task_ids='extract_data', key='sales_data')

    total_revenue = sum(item['quantity'] * item['price'] for item in data)
    best_seller = max(data, key=lambda x: x['quantity'])['product']

    result = {'total_revenue': total_revenue, 'best_seller': best_seller}
    ti.xcom_push(key='processed_data', value=result)
    print("Processed Data:", result)


def store_report(**kwargs):
    """Retrieves processed data from XCom, stores it, and pushes it for later use."""
    ti = kwargs['ti']
    processed_data = ti.xcom_pull(task_ids='process_data', key='processed_data')
    print("Storing Report:", json.dumps(processed_data, indent=2))

    # Push the processed data again so send_email_notification can access it
    ti.xcom_push(key='processed_data', value=processed_data)


def send_email_notification(**kwargs):
    """Retrieves stored report and simulates sending an email."""
    ti = kwargs['ti']
    report = ti.xcom_pull(task_ids='store_report', key='processed_data')
    print(
        f"Email Sent: Daily Sales Report\n"
        f"Total Revenue: ${report['total_revenue']}\n"
        f"Best Seller: {report['best_seller']}"
    )


with DAG(
    "03_cross",
    # These args will get passed on to each operator
    # You can override them on a per-task basis during operator initialization
    default_args={
        "owner": "Anower",
        "depends_on_past": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    description="A simple tutorial DAG",
    schedule=timedelta(days=1),
    start_date=pendulum.datetime(2025, 3, 4, tz="America/Sao_Paulo"),
    catchup=False,
    tags=["example"],
) as dag:
    extract = PythonOperator(task_id='extract_data', python_callable=extract_data)
    process = PythonOperator(task_id='process_data', python_callable=process_data)
    store = PythonOperator(task_id='store_report', python_callable=store_report)
    notify = PythonOperator(task_id='send_email_notification', python_callable=send_email_notification)

    # Define task dependencies
    extract >> process >> store >> notify
