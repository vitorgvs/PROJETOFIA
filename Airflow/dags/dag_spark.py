# dags/ingestao_faker_dag.py
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import requests
from pyspark.sql import SparkSession
from datetime import date
import os
import pandas as pd

# ---------------------------
# Funções do pipeline
# ---------------------------

def load_items(**kwargs):
    qtde = kwargs.get("qtde", 10)
    items = []
    for _ in range(qtde):
        r = requests.get(
            "https://fakerapi.it/api/v1/persons?_quantity=1",
            timeout=30
        )
        r.raise_for_status()
        pessoa = r.json()["data"][0]
        address = pessoa.pop("address", {})
        items.append({**pessoa, **address})
    # salvar no XCom para usar na próxima task
    kwargs['ti'].xcom_push(key='items', value=items)

def escrever_delta(**kwargs):
    items = kwargs['ti'].xcom_pull(key='items', task_ids='load_items_task')
    df = pd.DataFrame(items)

    hoje = date.today()
    df["ano"] = hoje.year
    df["mes"] = hoje.month
    df["dia"] = hoje.day

    # Spark session com delta-spark
    spark = SparkSession.builder \
        .appName("IngestaoFakerDeltaSpark") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()

    sdf = spark.createDataFrame(df)

    # Endpoint do MinIO
    minio_endpoint = os.environ.get("MINIO_ENDPOINT")
    access_key = os.environ.get("MINIO_ACCESS_KEY")
    secret_key = os.environ.get("MINIO_SECRET_KEY")
    bucket = os.environ.get("MINIO_BUCKET", "raw")

    # Config S3 (MinIO)
    spark._jsc.hadoopConfiguration().set("fs.s3a.endpoint", minio_endpoint)
    spark._jsc.hadoopConfiguration().set("fs.s3a.access.key", access_key)
    spark._jsc.hadoopConfiguration().set("fs.s3a.secret.key", secret_key)
    spark._jsc.hadoopConfiguration().set("fs.s3a.path.style.access", "true")
    spark._jsc.hadoopConfiguration().set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

    # Caminho Delta
    delta_path = f"s3a://{bucket}/user"

    # Escreve Delta particionado
    sdf.write.format("delta") \
        .mode("append") \
        .partitionBy("ano", "mes", "dia") \
        .save(delta_path)

# ---------------------------
# DAG
# ---------------------------

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 12, 13),
    'retries': 1
}

with DAG(
    dag_id='ingestao_faker_delta_spark',
    default_args=default_args,
    schedule_interval=None,
    catchup=False
) as dag:

    load_items_task = PythonOperator(
        task_id='load_items_task',
        python_callable=load_items,
        op_kwargs={'qtde': 10},
        provide_context=True
    )

    escrever_delta_task = PythonOperator(
        task_id='escrever_delta_task',
        python_callable=escrever_delta,
        provide_context=True
    )

    load_items_task >> escrever_delta_task
