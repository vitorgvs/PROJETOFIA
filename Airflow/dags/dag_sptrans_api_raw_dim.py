from airflow import DAG
from airflow.operators.python import PythonOperator
from scripts.sptrans_api import ExtractorSpTransAPI, Loader_Minio
from datetime import datetime

default_args = {
    'owner': 'sptrans',
    'depends_on_past': False,
    'start_date': datetime(2026, 1, 1),
    'retries': 1,
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

def task_extract_lines():
    extractor = ExtractorSpTransAPI()
    extractor.Authenticate()
    dados = extractor.ExtractTotalLine()
    return dados

def task_extract_enterprise():
    extractor = ExtractorSpTransAPI()
    extractor.Authenticate()
    dados = extractor.ExtractEnterprise()
    return dados

def task_extract_agency():
    extractor = ExtractorSpTransAPI()
    extractor.Authenticate()
    dados = extractor.ExtractAgency()
    return dados

def task_extract_calendar():
    extractor = ExtractorSpTransAPI()
    extractor.Authenticate()
    dados = extractor.ExtractCalendar()
    return dados

def task_extract_fare_attributes():
    extractor = ExtractorSpTransAPI()
    extractor.Authenticate()
    dados = extractor.ExtractFareAttributes()
    return dados

def task_extract_fare_rules():
    extractor = ExtractorSpTransAPI()
    extractor.Authenticate()
    dados = extractor.ExtractFareRule()
    return dados

def task_extract_frequencies():
    extractor = ExtractorSpTransAPI()
    extractor.Authenticate()
    dados = extractor.ExtractFrequencies()
    return dados

def task_extract_routes():
    extractor = ExtractorSpTransAPI()
    extractor.Authenticate()
    dados = extractor.ExtractRoutes()
    return dados

def task_extract_shapes():
    extractor = ExtractorSpTransAPI()
    extractor.Authenticate()
    dados = extractor.ExtractShapes()
    return dados

def task_extract_stop_times():
    extractor = ExtractorSpTransAPI()
    extractor.Authenticate()
    dados = extractor.ExtractStopTimes()
    return dados

def task_extract_stops():
    extractor = ExtractorSpTransAPI()
    extractor.Authenticate()
    dados = extractor.ExtractStops()
    return dados

def task_extract_trips():
    extractor = ExtractorSpTransAPI()
    extractor.Authenticate()
    dados = extractor.ExtractTrips()
    return dados

def task_load_lines(ti):
    _load(ti,'lines',False)

def task_load_enterprise(ti):
    _load(ti,'enterprise',False)

def task_load_agency(ti):
    _load(ti,'agency',False)

def task_load_calendar(ti):
    _load(ti,'calendar',False)

def task_load_fare_attributes(ti):
    _load(ti,'fare_attributes',False)

def task_load_fare_rules(ti):
    _load(ti,'fare_rules',False)

def task_load_frequencies(ti):
    _load(ti,'frequencies',False)

def task_load_routes(ti):
    _load(ti,'routes',False)

def task_load_shapes(ti):
    _load(ti,'shapes',False)

def task_load_stop_times(ti):
    _load(ti,'stop_times',False)

def task_load_stops(ti):
    _load(ti,'stops',False)

def task_load_trips(ti):
    _load(ti,'trips',False)

# Criando o DAG
with DAG(
    'dag_sptrans_api_raw_dim',
    default_args=default_args,
    description='Pipeline de ingestão de dados da SPTrans para a camada raw uma vez por semana',
    schedule_interval='@weekly',
    catchup=False,
    tags=["sptrans", "raw", "prod","dim"]
) as dag:

    t1_extract_task = PythonOperator(
        task_id='extract_lines',
        python_callable=task_extract_lines,
        trigger_rule='all_done',
        show_return_value_in_logs=False
    )

    t2_extract_task = PythonOperator(
        task_id='extract_enterprise',
        python_callable=task_extract_enterprise,
        trigger_rule='all_done',
        show_return_value_in_logs=False
    )
    t3_extract_task = PythonOperator(
        task_id='extract_agency',
        python_callable=task_extract_agency,
        trigger_rule='all_done',
        show_return_value_in_logs=False
    )
    t4_extract_task = PythonOperator(
        task_id='extract_calendar',
        python_callable=task_extract_calendar,
        trigger_rule='all_done',
        show_return_value_in_logs=False
    )
    t5_extract_task = PythonOperator(
        task_id='extract_fare_attributes',
        python_callable=task_extract_fare_attributes,
        trigger_rule='all_done',
        show_return_value_in_logs=False
    )
    t6_extract_task = PythonOperator(
        task_id='extract_fare_rules',
        python_callable=task_extract_fare_rules,
        trigger_rule='all_done',
        show_return_value_in_logs=False
    )
    t7_extract_task = PythonOperator(
        task_id='extract_frequencies',
        python_callable=task_extract_frequencies,
        trigger_rule='all_done',
        show_return_value_in_logs=False
    )
    t8_extract_task = PythonOperator(
        task_id='extract_routes',
        python_callable=task_extract_routes,
        trigger_rule='all_done',
        show_return_value_in_logs=False
    )
    t9_extract_task = PythonOperator(
        task_id='extract_stop_times',
        python_callable=task_extract_stop_times,
        trigger_rule='all_done',
        show_return_value_in_logs=False
    )
    t10_extract_task = PythonOperator(
        task_id='extract_stops',
        python_callable=task_extract_stops,
        trigger_rule='all_done',
        show_return_value_in_logs=False
    )
    t11_extract_task = PythonOperator(
        task_id='extract_trips',
        python_callable=task_extract_trips,
        trigger_rule='all_done',
        show_return_value_in_logs=False
    )
    

    t1_load_task = PythonOperator(
        task_id='load_lines',
        python_callable=task_load_lines,
        trigger_rule='all_done',
        show_return_value_in_logs=False
    )

    t2_load_task = PythonOperator(
        task_id='load_enterprise',
        python_callable=task_load_enterprise,
        trigger_rule='all_done',
        show_return_value_in_logs=False
    )
    t3_load_task = PythonOperator(
        task_id='load_agency',
        python_callable=task_load_agency,
        trigger_rule='all_done',
        show_return_value_in_logs=False
    )
    t4_load_task = PythonOperator(
        task_id='load_calendar',
        python_callable=task_load_calendar,
        trigger_rule='all_done',
        show_return_value_in_logs=False
    )
    t5_load_task = PythonOperator(
        task_id='load_fare_attributes',
        python_callable=task_load_fare_attributes,
        trigger_rule='all_done',
        show_return_value_in_logs=False
    )
    t6_load_task = PythonOperator(
        task_id='load_fare_rules',
        python_callable=task_load_fare_rules,
        trigger_rule='all_done',
        show_return_value_in_logs=False
    )
    t7_load_task = PythonOperator(
        task_id='load_frequencies',
        python_callable=task_load_frequencies,
        trigger_rule='all_done',
        show_return_value_in_logs=False
    )
    t8_load_task = PythonOperator(
        task_id='load_routes',
        python_callable=task_load_routes,
        trigger_rule='all_done',
        show_return_value_in_logs=False
    )

    t9_load_task = PythonOperator(
        task_id='load_stop_times',
        python_callable=task_load_stop_times,
        trigger_rule='all_done',
        show_return_value_in_logs=False
    )
    t10_load_task = PythonOperator(
        task_id='load_stops',
        python_callable=task_load_stops,
        trigger_rule='all_done',
        show_return_value_in_logs=False
    )
    t11_load_task = PythonOperator(
        task_id='load_trips',
        python_callable=task_load_trips,
        trigger_rule='all_done',
        show_return_value_in_logs=False
    )
    
    t1_extract_task >> t1_load_task 
    [t1_load_task] >> t2_extract_task >> t2_load_task    
    [t2_load_task] >> t3_extract_task >> t3_load_task
    [t3_load_task] >> t4_extract_task >> t4_load_task
    [t4_load_task] >> t5_extract_task >> t5_load_task
    [t5_load_task] >> t6_extract_task >> t6_load_task
    [t6_load_task] >> t7_extract_task >> t7_load_task
    [t7_load_task] >> t8_extract_task >> t8_load_task
    [t8_load_task] >> t9_extract_task >> t9_load_task
    [t9_load_task] >> t10_extract_task >> t10_load_task
    [t10_load_task] >> t11_extract_task >> t11_load_task