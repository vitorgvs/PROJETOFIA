
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from delta.tables import DeltaTable

from delta import *
import os
import pyspark
from datetime import datetime

# --- REMOVA A LÓGICA MANUAL DE PATH ---
# Vamos confiar que o Dockerfile configurou o ENV corretamente.
# Se precisar debuggar, apenas printe o que o sistema entregou:
print(f"--- JAVA_HOME DO SISTEMA: {os.environ.get('JAVA_HOME', 'Não definido')} ---")

# Evita conflitos de versão
os.environ.pop("SPARK_HOME", None)

# Se o JAVA_HOME estiver vazio (o que não deve acontecer se o Dockerfile rodou),
# aí sim lançamos erro, mas não tentamos adivinhar caminhos.
if not os.environ.get("JAVA_HOME"):
    raise RuntimeError("JAVA_HOME não veio configurado do Dockerfile!")

extra_packages = ["org.apache.hadoop:hadoop-aws:3.3.2"]

builder = (
    SparkSession.builder
    .appName("Trusted")
    .master("local[*]")
    #.master("spark://spark-master:7077")
    .config("spark.sql.extensions","io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog","org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.hadoop.fs.defaultFS", "file:///")  # Define local como padrão
    .config("spark.hadoop.fs.s3a.access.key", "projeto_final")
    .config("spark.hadoop.fs.s3a.secret.key", "projeto_final")
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
)

spark = configure_spark_with_delta_pip(builder, extra_packages=extra_packages).getOrCreate()
spark.conf.set("spark.sql.repl.eagerEval.enabled", True)










df_stops = (
    spark.read
    .option("multiLine", "true")
    .json("s3a://raw/sptrans/stops/")
)





df_stops.createOrReplaceTempView('stop')
df_stops_export = spark.sql(" SELECT A.*, current_date() dt_export from stop A")



delta_path = "s3a://trusted/sptrans/stops"
if DeltaTable.isDeltaTable(spark, delta_path):
    delta_table = DeltaTable.forPath(spark, delta_path)
else:
    df_stops_export.write.format("delta") \
        .mode("overwrite") \
        .partitionBy("dt_export") \
        .save(delta_path)

    delta_table = DeltaTable.forPath(spark, delta_path)

delta_table = DeltaTable.forPath(spark, "s3a://trusted/sptrans/stops")


(
    delta_table.alias("t")
    .merge(
        df_stops_export.alias("s"),
        """
        t.stop_lat = s.stop_lon
        AND t.stop_lat = s.stop_lon
        """
    )
    .whenMatchedUpdateAll()  # Atualiza a última posição
    .whenNotMatchedInsertAll()  # Insere se não existir
    .execute()
)



