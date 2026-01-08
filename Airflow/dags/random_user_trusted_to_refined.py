from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import scripts.random_user_refined as rur

def execucao_refined():
    objeto_extracao = rur.RandomUserRefined()
    objeto_extracao.TrustedToRefined()

with DAG(
    dag_id='random_user_trusted_to_refined',
    start_date=datetime(2025, 12, 20),
    schedule_interval='@continuous',
    catchup=False,
    max_active_runs=1,
    tags=['postgres', 'refined','random_user']
) as dag:

    task_load = PythonOperator(
        task_id='random_user_trusted_to_refined',
        python_callable=execucao_refined
    )