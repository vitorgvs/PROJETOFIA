from datetime import datetime
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
import json
import logging
import os

class RandomUserTrusted():
    def __init__(self):
        self.S3_CONN_ID = 'minio_conn'
        self.RAW_BUCKET = 'raw'
        self.TRUSTED_BUCKET = 'trusted'
        self.PREFIX_RAW_STAGING = 'random_user/staging/'
        self.RAW_PROCESSSED = 'random_user/processed/'
        self.PREFIX_TRUSTED = 'random_user/'
        self.DELIMITER='/'

    def RawToTrusted (self):
        s3_hook = self.ConexaoMinio()
        keys = self.GetChaves(s3_hook)
        if not keys:
            logging.info("Nenhum arquivo encontrado.")
            return
        for key in keys:
            try:
                if key == self.PREFIX_RAW_STAGING or not key.lower().endswith('.json'):
                    continue             
                logging.info(f"Iniciando processamento de: {key}")           
                file_content = s3_hook.read_key(
                    key=key, 
                    bucket_name=self.RAW_BUCKET
                )
                data = json.loads(file_content)
                processed_data = self.TratamentoDados(key,data)
                self.GravacaoBucketTrusted(key,processed_data,s3_hook)
                self.ProcessoStagingAndProcessed(key,s3_hook)
            except Exception as e:
                logging.error(f"Erro ao processar {key}: {str(e)}") 

    def ConexaoMinio(self):
        s3_hook = S3Hook(aws_conn_id=self.S3_CONN_ID)  
        return s3_hook

    def GetChaves (self, s3_hook):
        keys = s3_hook.list_keys(
            bucket_name=self.RAW_BUCKET, 
            prefix=self.PREFIX_RAW_STAGING, 
            delimiter= self.DELIMITER
        )
        logging.info(f"Listando objetos em: s3://{self.RAW_BUCKET}/{self.PREFIX_RAW_STAGING}")
        return keys
    
    def TratamentoDados(self,key,data):       
        processed_data = {
            "id_processamento": f"proc_{datetime.now().strftime('%Y%m%d%H%M%S')}",
            "data_ingestao": datetime.now().isoformat(),
            "payload": data,
            "origem": key
        }
        return processed_data
    
    def GravacaoBucketTrusted(self, key, processed_data, s3_hook):            
        file_name = os.path.basename(key)
        new_key = f"{self.PREFIX_TRUSTED}{file_name}"
        s3_hook.load_string(
            string_data=json.dumps(processed_data),
            key=new_key,
            bucket_name=self.TRUSTED_BUCKET,
            replace=True
        )
    
    def ProcessoStagingAndProcessed (self, key, s3_hook):
        file_name = os.path.basename(key)
        archive_key = f"{self.RAW_PROCESSSED}{file_name}"
        s3_hook.copy_object(
            source_bucket_key=key,
            dest_bucket_key=archive_key,
            source_bucket_name=self.RAW_BUCKET,
            dest_bucket_name=self.RAW_BUCKET
        )     
        s3_hook.delete_objects(bucket=self.RAW_BUCKET, keys=key)
        logging.info(f"Sucesso: {key} movido para {archive_key}")
                