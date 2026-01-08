from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
from dateutil.relativedelta import relativedelta
import json
import logging


class RandomUserRefined():
    def __init__(self):
        self.S3_CONN_ID = 'minio_conn'
        self.POSTGRES_CONN_ID = 'postgres_default'
        self.TRUSTED_BUCKET = 'trusted'
        self.PREFIX_TRUSTED = 'random_user/'
        self.DELIMITER='/'
        self.SQL = """
            INSERT INTO random_user.usuarios (id, dt_ingest, tratamento, nome, sobrenome, genero, dt_nascimento, idade, dt_registro, idade_registro,
            email,uuid,usuario,telefone,rua,cidade,estado,pais,cep,latitude,longitude)
            VALUES (%s, %s, %s, %s, %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (uuid)
            DO UPDATE SET 
                id = EXCLUDED.id,
                dt_ingest = EXCLUDED.dt_ingest,
                tratamento = EXCLUDED.tratamento,
                nome = EXCLUDED.nome,
                sobrenome = EXCLUDED.sobrenome,
                genero = EXCLUDED.genero,
                dt_nascimento = EXCLUDED.dt_nascimento,
                idade = EXCLUDED.idade,
                dt_registro = EXCLUDED.dt_registro,
                idade_registro = EXCLUDED.idade_registro,
                email = EXCLUDED.email,
                uuid = EXCLUDED.uuid,
                usuario = EXCLUDED.usuario,
                telefone = EXCLUDED.telefone,
                rua = EXCLUDED.rua,
                cidade = EXCLUDED.cidade,
                estado = EXCLUDED.estado,
                pais = EXCLUDED.pais,
                cep = EXCLUDED.cep,
                latitude = EXCLUDED.latitude,
                longitude = EXCLUDED.longitude;
        """

    def TrustedToRefined (self):
        s3_hook = self.ConexaoMinio()
        keys = self.GetChaves(s3_hook)
        if not keys:
            logging.info("Nenhum arquivo encontrado.")
            return

        for key in keys:
            try:
                if key == self.PREFIX_TRUSTED or not key.endswith('.json'):
                    continue
                logging.info(f"Extraindo dados de: {key}")        
                file_content = s3_hook.read_key(key=key, bucket_name=self.TRUSTED_BUCKET)
                data = json.loads(file_content)
                parameters = self.TratamentoDados(data)
                self.GravacaoRefined(parameters,key)
            except Exception as e:
                logging.error(f"Erro ao processar {key}: {str(e)}")

    def FuncDateDiff(self, start):
        start = datetime.fromisoformat(start).date()
        end = datetime.today()
        dif = relativedelta(end,start)
        return dif.years

    def ConexaoMinio(self):
        s3_hook = S3Hook(aws_conn_id=self.S3_CONN_ID)  
        return s3_hook

    def GetChaves(self, s3_hook):
         
        keys = s3_hook.list_keys(
            bucket_name=self.TRUSTED_BUCKET, 
            prefix=self.PREFIX_TRUSTED, 
            delimiter=self.DELIMITER
        )
        return keys
    
    def TratamentoDados(self,data):  
        parameters = (
            data.get('id_processamento'),
            data.get('data_ingestao'),
            data.get('payload').get('name').get('title'),
            data.get('payload').get('name').get('first'),
            data.get('payload').get('name').get('last'),
            data.get('payload').get('gender'),
            data.get('payload').get('dob').get('date'),
            self.FuncDateDiff(data.get('payload').get('dob').get('date')),
            data.get('payload').get('registered').get('date'),
            self.FuncDateDiff(data.get('payload').get('registered').get('date')),
            data.get('payload').get('email'),
            data.get('payload').get('login').get('uuid'),
            data.get('payload').get('login').get('username'),
            data.get('payload').get('phone'),
            data.get('payload').get('location').get('street').get('name'),
            data.get('payload').get('location').get('city'),
            data.get('payload').get('location').get('state'),
            data.get('payload').get('location').get('country'),
            data.get('payload').get('location').get('postcode'),
            float(data.get('payload').get('location').get('coordinates').get('latitude')),
            float(data.get('payload').get('location').get('coordinates').get('longitude')) 
        )
        return parameters
    
    def GravacaoRefined (self,parameters, key):
        pg_hook = PostgresHook(postgres_conn_id=self.POSTGRES_CONN_ID)
        pg_hook.run(self.SQL, parameters=parameters)
        print(f"Dados do arquivo {key} inseridos no Postgres com sucesso.")