
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, LongType, ArrayType, BooleanType, DoubleType
from pyspark.sql.functions import *
from delta.tables import DeltaTable
from delta import *
import os
import sys
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

now = datetime.now()
ano = now.strftime('%Y')
mes = now.strftime('%m')
dia = now.strftime('%d')

caminho_particao = f"s3a://raw/sptrans/prevision_stop/ano={ano}/mes={mes}/dia={dia}/" 

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

schema_previsao = StructType([
    # Horário de referência
    StructField("hr", StringType(), True),

    # Objeto da Parada (pode ser nulo)
    StructField("p", StructType([
        StructField("cp", LongType(), True),      # Código da parada
        StructField("np", StringType(), True),    # Nome da parada
        StructField("py", DoubleType(), True),    # Latitude
        StructField("px", DoubleType(), True),    # Longitude
        
        # Lista de Linhas
        StructField("l", ArrayType(StructType([
            StructField("c", StringType(), True),   # Letreiro (ex: "809H-10")
            StructField("cl", LongType(), True),    # Código da linha
            StructField("sl", IntegerType(), True), # Sentido (1 ou 2)
            StructField("lt0", StringType(), True), # Destino
            StructField("lt1", StringType(), True), # Origem
            StructField("qv", IntegerType(), True), # Qtd Veículos
            
            # Lista de Veículos
            StructField("vs", ArrayType(StructType([
                StructField("p", StringType(), True),   # Prefixo (String no JSON)
                StructField("t", StringType(), True),   # Horário previsto
                StructField("a", BooleanType(), True),  # Acessível
                StructField("ta", StringType(), True),  # Timestamp ISO
                StructField("py", DoubleType(), True),  # Latitude Veículo
                StructField("px", DoubleType(), True),  # Longitude Veículo
                StructField("sv", StringType(), True),  # Campo extra (null no arquivo)
                StructField("is", StringType(), True)   # Campo extra (null no arquivo)
            ])), True)
        ])), True)
    ]), True),

    # Campo de rastreabilidade que você injetou
    StructField("codigo_parada_solicitado", LongType(), True)
])

df = (
    spark.read
    .option("multiLine", "true")
    .schema(schema_previsao) \
    .json(caminho_particao)
    
)
#df

df_linha = df.select(
    col("hr").alias("hora_captura"),
    col("p.cp").alias("codigo_parada"),
    col("p.np").alias("nome_parada"),
    col("p.py").alias("latitude_parada"),
    col("p.px").alias("longitude_parada"),
    explode(col("p.l")).alias("linha")
)
#df_linha

df_veiculo = df_linha.select(
    col("hora_captura"),
    col("codigo_parada"),
    col("nome_parada"),
    col("latitude_parada"),
    col("longitude_parada"),
    col("linha.c").alias("letreiro"),
    col("linha.cl").alias("codigo_linha"),
    col("linha.sl").alias("sentido"),
    col("linha.lt0").alias("destino"),
    col("linha.lt1").alias("origem"),
    col("linha.qv").alias("qtde_veiculos"),
    explode(col("linha.vs")).alias("veiculos")
)
#df_veiculo

df_flat = df_veiculo.select(
    col("hora_captura"),
    col("codigo_parada"),
    col("nome_parada"),
    col("latitude_parada"),
    col("longitude_parada"),
    col("letreiro"),
    col("codigo_linha"),
    col("sentido"),
    col("destino"),
    col("origem"),
    col("qtde_veiculos"),
    col("veiculos.p").alias("prefixo_veiculo"),
    col("veiculos.t").alias("horario_previsto"),
    col("veiculos.a").alias("acessivel"),
    col("veiculos.ta").alias("timestamp_posicao_veiculo"),
    col("veiculos.py").alias("latitude_veiculo"),
    col("veiculos.px").alias("longitude_veiculo")
)
#df_flat

df_flat.createOrReplaceTempView('prevision_stop')

query = """
        WITH
        posicoes AS
        (
            SELECT 
                codigo_parada,
                nome_parada,
                latitude_parada,
                longitude_parada,
                letreiro,
                codigo_linha,
                CASE
                WHEN sentido = 1
                    THEN 'TERMINAL PRINCIPAL PARA SECUNDÁRIO'
                    ELSE 'TERMINAL SECUNDÁRIO PARA PRINCIPAL'
                END AS sentido,
                origem,
                destino,
                prefixo_veiculo,
                horario_previsto,
                acessivel,
                DATE(timestamp_posicao_veiculo) AS data,
                CAST(timestamp_posicao_veiculo AS TIMESTAMP) AS timestamp_posicao_veiculo,
                latitude_veiculo,
                longitude_veiculo,
                ROW_NUMBER() OVER(PARTITION BY codigo_parada, codigo_linha,prefixo_veiculo  ORDER BY timestamp_posicao_veiculo DESC) AS rn
            FROM prevision_stop
        )
        SELECT
            *
        FROM posicoes
        WHERE
            rn = 1          
            """

resultado_df = spark.sql(query).drop('rn')
#resultado_df

delta_path = "s3a://trusted/sptrans/prevision_stop"
if DeltaTable.isDeltaTable(spark, delta_path):
    delta_table = DeltaTable.forPath(spark, delta_path)
else:
    resultado_df.write.format("delta") \
        .mode("overwrite") \
        .partitionBy("data") \
        .save(delta_path)
    delta_table = DeltaTable.forPath(spark, delta_path)

delta_table = DeltaTable.forPath(spark, "s3a://trusted/sptrans/prevision_stop")

(
    delta_table.alias("t")
    .merge(
        resultado_df.alias("s"),
        """
        t.codigo_parada = s.codigo_parada
        AND t.codigo_linha = s.codigo_linha
        AND t.prefixo_veiculo = s.prefixo_veiculo
        """
    )
    .whenMatchedUpdateAll()  # Atualiza a última posição
    .whenNotMatchedInsertAll()  # Insere se não existir
    .execute()
)




