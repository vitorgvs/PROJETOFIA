from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import scripts.random_user_trusted as rut


def execucao_trusted():
    objeto_extracao = rut.RandomUserTrusted()
    objeto_extracao.RawToTrusted()

with DAG(
    dag_id='random_user_raw_to_trusted_incremental',
    start_date=datetime(2025, 12, 21),
    schedule_interval='@continuous',
    catchup=False,
    max_active_runs=1,
    tags=['minio', 'random_user', 'trusted']
) as dag:

    task_process = PythonOperator(
        task_id='random_user_raw_to_trusted',
        python_callable=execucao_trusted
    )