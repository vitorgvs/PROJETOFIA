from airflow import DAG
from airflow.providers.papermill.operators.papermill import PapermillOperator
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
    dag_id='dag_sptrans_api_trusted_fact',
    default_args=default_args,
    description='Pipeline da camada trusted a cada 2 minutos',
    schedule_interval='*/2 * * * *', 
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["sptrans", "trusted", "prod","fact"],
) as dag:


    ingestion_raw = ExternalTaskSensor(
        task_id='sensor_camada_raw_fact',
        external_dag_id='dag_sptrans_api_raw_fact',
        external_task_id=None, 
        allowed_states=['success'],
        mode='reschedule',
        timeout=120,
        poke_interval=5
    )

    position_trusted = PapermillOperator(
        task_id='position_trusted',
        input_nb='dags/scripts/notebook/raw_to_trusted_position.ipynb',
        output_nb='dags/scripts/notebook/outputs/saida_{{ execution_date }}.ipynb',
        parameters={
            'data_execucao': '{{ ds }}',
            'hora_execucao': '{{ ts }}'
        }
    )
    previstion_stop_trusted = PapermillOperator(
        task_id='previstion_stop_trusted',
        input_nb='dags/scripts/notebook/raw_to_trusted_prevision_stop.ipynb',
        output_nb='dags/scripts/notebook/outputs/saida_{{ execution_date }}.ipynb',
        parameters={
            'data_execucao': '{{ ds }}',
            'hora_execucao': '{{ ts }}'
        }
    )

    # Definindo a ordem
    ingestion_raw >> position_trusted >> previstion_stop_trusted