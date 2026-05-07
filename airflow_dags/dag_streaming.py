from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id='spark_streaming_dag',
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=['spark', 'streaming'],
) as dag:
    start_streaming = BashOperator(
        task_id='start_streaming',
        # timeout pour éviter de bloquer
        bash_command='timeout 60 python3 /opt/spark/work-dir/spark_streaming_recommendations.py || true',
    )