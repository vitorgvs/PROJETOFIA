
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from delta.tables import DeltaTable
from delta import *
import os

print(f"--- JAVA_HOME DO SISTEMA: {os.environ.get('JAVA_HOME', 'Não definido')} ---")
os.environ.pop("SPARK_HOME", None)
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
caminho = f"s3a://raw/sptrans/stops/"

df = (
    spark.read
    .option("multiLine", "true")
    .option("recursiveFileLookup", "true")
    .json(caminho)
)

df_flat = df.select(
    col("stop_id").alias("id_parada"),
    col("stop_name").alias("nome_parada"),
    col("stop_desc").alias("descricao_parada"),
    col("stop_lat").alias("latitude_parada"),
    col("stop_lon").alias("longitude_parada")
)

df_flat.createOrReplaceTempView('stop')

query = """
        SELECT
            *,
            CURRENT_DATE() AS dt_export 
        FROM stop      
        """

resultado_df = spark.sql(query)

delta_path = "s3a://trusted/sptrans/stops"
if DeltaTable.isDeltaTable(spark, delta_path):
    delta_table = DeltaTable.forPath(spark, delta_path)
else:
    resultado_df.write.format("delta") \
        .mode("overwrite") \
        .partitionBy("dt_export") \
        .save(delta_path)
    delta_table = DeltaTable.forPath(spark, delta_path)


delta_table = DeltaTable.forPath(spark, "s3a://trusted/sptrans/stops")

(
    delta_table.alias("t")
    .merge(
        resultado_df.alias("s"),
        """
        t.id_parada = s.id_parada
        """
    )
    .whenMatchedUpdateAll()  # Atualiza a última posição
    .whenNotMatchedInsertAll()  # Insere se não existir
    .execute()
)
os.environ.pop("SPARK_HOME", None)



