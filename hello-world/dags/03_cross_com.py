from airflow import DAG
from airflow.operators.python import PythonOperator
import json

from datetime import datetime, timedelta


# Dummy data
sales_data = [
    {'product': 'Laptop', 'quantity': 5, 'price': 1000},
    {'product': 'Mouse', 'quantity': 10, 'price': 50},
    {'product': 'Keyboard', 'quantity': 7, 'price': 100},
]

with DAG(
    "03_cross",
    # These args will get passed on to each operator
    # You can override them on a per-task basis during operator initialization
    default_args={
        "depends_on_past": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
        # 'queue': 'bash_queue',
        # 'pool': 'backfill',
        # 'priority_weight': 10,
        # 'end_date': datetime(2016, 1, 1),
        # 'wait_for_downstream': False,
        # 'execution_timeout': timedelta(seconds=300),
        # 'on_failure_callback': some_function, # or list of functions
        # 'on_success_callback': some_other_function, # or list of functions
        # 'on_retry_callback': another_function, # or list of functions
        # 'sla_miss_callback': yet_another_function, # or list of functions
        # 'on_skipped_callback': another_function, #or list of functions
        # 'trigger_rule': 'all_success'
    },
    description="A simple tutorial DAG",
    schedule=timedelta(days=1),
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["example"],
) as dag:
    def extract_data(**kwargs):
        """Pushes sales data to XCom."""
        # A dag e a tarefa orginadora veem automaticamente
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
        print(f"Email Sent: Daily Sales Report\nTotal Revenue: ${report['total_revenue']}\nBest Seller: {report['best_seller']}")

    # Define Airflow DAG
    default_args = {'owner': 'Anower', 'start_date': datetime(2025, 3, 4)}

    extract_data = PythonOperator(task_id='extract_data', python_callable=extract_data, provide_context=True, dag=dag)
    process_data = PythonOperator(task_id='process_data', python_callable=process_data, provide_context=True, dag=dag)
    store_report = PythonOperator(task_id='store_report', python_callable=store_report, provide_context=True, dag=dag)
    send_email_notification = PythonOperator(task_id='send_email_notification', python_callable=send_email_notification, provide_context=True, dag=dag)

    # Define task dependencies
    extract_data >> process_data >> store_report >> send_email_notification