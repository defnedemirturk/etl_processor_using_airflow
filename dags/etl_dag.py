from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime, timedelta
from etlprocessor import ETLProcessor

def run_etl(**kwargs):
    """Execute the ETL pipeline from etlprocessor.py"""
    ETLProcessor().run()

with DAG(
    dag_id="etl_dag",
    default_args={
        'owner': 'airflow',
        'depends_on_past': False,
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description="Check for airflow_db existence and connect",
    schedule=timedelta(days=1),
    start_date=datetime(2025, 6, 27),
    catchup=False,
) as dag:

    run_etl_task = PythonOperator(
        task_id="run_etl",
        python_callable=run_etl
    )

    run_etl_task