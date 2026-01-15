
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from delta.tables import DeltaTable
from delta import *
import os
from datetime import datetime

def read_latest_file(spark_session, partition_path):
    """
    Lê o último arquivo de uma partição MinIO usando a API Hadoop do Spark.
    """ 
    # Acesso à JVM interna do Spark para usar a API do Hadoop
    sc = spark_session.sparkContext
    URI = sc._gateway.jvm.java.net.URI
    Path = sc._gateway.jvm.org.apache.hadoop.fs.Path
    FileSystem = sc._gateway.jvm.org.apache.hadoop.fs.FileSystem
    
    # Obtém o sistema de arquivos configurado
    fs = FileSystem.get(URI(partition_path), sc._jsc.hadoopConfiguration())
    
    # Lista os status dos arquivos (apenas metadados!) no caminho
    file_statuses = fs.listStatus(Path(partition_path))
    
    # Filtra para remover diretórios (se houver) e pega apenas arquivos
    files = [f for f in file_statuses if f.isFile()]
    
    if not files:
        print("Nenhum arquivo encontrado.")
        return None
    # Ordena pela data de modificação (descendente)
    latest_file_status = sorted(files, key=lambda f: f.getModificationTime(), reverse=True)[0]
    return latest_file_status.getPath().toString()


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

now = datetime.now()
ano = now.strftime('%Y')
mes = now.strftime('%m')
dia = now.strftime('%d')
particao = f"s3a://raw/sptrans/position/ano={ano}/mes={mes}/dia={dia}/"
caminho_particao = read_latest_file(spark, particao)

df = (
    spark.read
    .option("multiLine", "true")
    .option("recursiveFileLookup", "true")
    .json(caminho_particao)
    
)
#df.show()

df_linhas = df.select(
    explode(col("l")).alias("linha") 
)

df_veiculos = df_linhas.select(
    col("linha.c").alias("codigo_linha"),
    col("linha.cl").alias("codigo_linha_id"),
    col("linha.sl").alias("sentido"),
    col("linha.lt0").alias("origem"),
    col("linha.lt1").alias("destino"),
    explode(col("linha.vs")).alias("veiculo")
)


df_flat = df_veiculos.select(
    col("codigo_linha"),
    col("codigo_linha_id"),
    col("sentido"),
    col("origem"),
    col("destino"),
    col("veiculo.p").alias("prefixo"),
    col("veiculo.a").alias("ativo"),
    col("veiculo.py").alias("latitude"),
    col("veiculo.px").alias("longitude"),
    to_timestamp(col("veiculo.ta")).alias("timestamp_posicao") 
)
#df_flat


df_flat.createOrReplaceTempView('position_raw')


query = """
        WITH
        posicoes AS
        (
            SELECT 
                codigo_linha,
                codigo_linha_id,
                CASE
                WHEN sentido = 1
                    THEN 'TERMINAL PRINCIPAL PARA SECUNDÁRIO'
                    ELSE 'TERMINAL SECUNDÁRIO PARA PRINCIPAL'
                END AS sentido,
                origem,
                destino,
                prefixo,
                ativo,
                latitude,
                longitude,
                timestamp_posicao,
                DATE(timestamp_posicao) AS data,
                DATE_FORMAT(timestamp_posicao,'HH') AS hora,
                DATE_FORMAT(timestamp_posicao,'mm') AS minuto,
                ROW_NUMBER() OVER(PARTITION BY codigo_linha_id, prefixo ORDER BY timestamp_posicao DESC) AS rn
            FROM position_raw
        )
        SELECT
            *
        FROM posicoes
        WHERE
            rn = 1          
            """

resultado_df = spark.sql(query).drop('rn')
#resultado_df


delta_path = "s3a://trusted/sptrans/position"
if DeltaTable.isDeltaTable(spark, delta_path):
    delta_table = DeltaTable.forPath(spark, delta_path)
else:
    resultado_df.write.format("delta") \
        .mode("overwrite") \
        .partitionBy("data") \
        .save(delta_path)
    delta_table = DeltaTable.forPath(spark, delta_path)


delta_table = DeltaTable.forPath(spark, "s3a://trusted/sptrans/position")

(
    delta_table.alias("t")
    .merge(
        resultado_df.alias("s"),
        """
        t.codigo_linha_id = s.codigo_linha_id
        AND t.prefixo = s.prefixo
        """
    )
    .whenMatchedUpdateAll()  # Atualiza a última posição
    .whenNotMatchedInsertAll()  # Insere se não existir
    .execute()
)
os.environ.pop("SPARK_HOME", None)



