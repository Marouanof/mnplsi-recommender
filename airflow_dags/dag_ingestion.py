from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id='kafka_ingestion_dag',
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=['kafka', 'ingestion'],
) as dag:
    start_kafka_producer = BashOperator(
        task_id='start_kafka_producer',
        bash_command='python /opt/spark/work-dir/kafka_producer.py',
    )