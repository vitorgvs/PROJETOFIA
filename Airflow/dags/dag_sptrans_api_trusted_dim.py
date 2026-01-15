from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.sensors.external_task import ExternalTaskSensor
from datetime import datetime
from pendulum import datetime, duration 

# Definição dos argumentos padrão
default_args = {
    'owner': 'sptrans',
    'depends_on_past': False,
    'start_date': datetime(2026, 1, 1),
    'retries': 2,
    "retry_delay": duration(seconds=15), 
    "retry_exponential_backoff": True, 
    "max_retry_delay": duration(minutes=1)
}

with DAG(
    dag_id='dag_sptrans_api_trusted_dim',
    default_args=default_args,
    description='Pipeline da camada trusted 1 vez por semana',
    schedule_interval='@weekly', 
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["sptrans", "trusted", "prod","dim"],
) as dag:


    ingestion_raw = ExternalTaskSensor(
        task_id='sensor_camada_raw_fact',
        external_dag_id='dag_sptrans_api_raw_dim',
        external_task_id=None, 
        allowed_states=['success'],
        mode='reschedule',
        timeout=300,
        poke_interval=10
    )

    stop_trusted = BashOperator(
        task_id='stop_trusted',
        bash_command='python /opt/airflow/dags/scripts/raw_to_trusted_stops.py',
    )

    # Definindo a ordem
    ingestion_raw >> stop_trusted