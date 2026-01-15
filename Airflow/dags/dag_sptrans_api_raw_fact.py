from airflow import DAG
from airflow.operators.python import PythonOperator
from scripts.sptrans_api import ExtractorSpTransAPI, Loader_Minio
from datetime import datetime
from pendulum import datetime, duration 

default_args = {
    'owner': 'sptrans',
    'depends_on_past': False,
    'start_date': datetime(2026, 1, 1),
    'retries': 2,
    "retry_delay": duration(seconds=15), 
    "retry_exponential_backoff": True, 
    "max_retry_delay": duration(minutes=1)
}

def _load(ti, name_doc, partition):
    bucket = 'raw'
    dados_extraidos = ti.xcom_pull(task_ids=f"extract_{name_doc}")
    path = f"sptrans/{name_doc}"
    if not dados_extraidos:
        print("Nenhum dado foi extraído para carregar no MinIO")
        return
    loader = Loader_Minio()
    loader.LoaderMinio(dados_extraidos,bucket,path,name_doc,partition)

def task_extract_position():
    extractor = ExtractorSpTransAPI()
    extractor.Authenticate()
    return extractor.ExtractTotalPosition()

def task_extract_prevision_line_stop():
    extractor = ExtractorSpTransAPI()
    extractor.Authenticate()
    return  extractor.ExtractPrevisionLineStop()

def task_extract_prevision_stop():
    extractor = ExtractorSpTransAPI()
    extractor.Authenticate()
    return  extractor.ExtractPrevisionStop()

def task_load_position(ti):
    _load(ti,'position',True)

def task_load_prevision_line_stop(ti):
    _load(ti,'prevision_line_stop',True)

def task_load_prevision_stop(ti):
    _load(ti,'prevision_stop',True)

# Criando o DAG
with DAG(
    'dag_sptrans_api_raw_fact',
    default_args=default_args,
    description='Pipeline de ingestão de dados da SPTrans para a camada raw a cada 2 minutos',
    schedule_interval='*/2 * * * *',
    catchup=False,
    tags=["sptrans", "raw", "prod","fact"],
    max_active_runs = 1
) as dag:

    t1_extract_task = PythonOperator(
        task_id='extract_position',
        python_callable=task_extract_position
    )

    t2_extract_task = PythonOperator(
        task_id='extract_prevision_line_stop',
        python_callable=task_extract_prevision_line_stop
    )

    t3_extract_task = PythonOperator(
        task_id='extract_prevision_stop',
        python_callable=task_extract_prevision_stop
    )

    t1_load_task = PythonOperator(
        task_id='load_position',
        python_callable=task_load_position
    )

    t2_load_task = PythonOperator(
        task_id='load_prevision_line_stop',
        python_callable=task_load_prevision_line_stop
    )
    t3_load_task = PythonOperator(
        task_id='load_prevision_stop',
        python_callable=task_load_prevision_stop
    )

    t1_extract_task >> t1_load_task
    [t1_load_task] >> t2_extract_task >> t2_load_task
    [t2_load_task] >> t3_extract_task >> t3_load_task