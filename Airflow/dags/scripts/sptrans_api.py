import requests
import boto3
import json
from datetime import datetime
import pandas as pd
from concurrent.futures import ThreadPoolExecutor

class SPTransAPI:
    def __init__(self, token):
        self.token = token
        self.base_url = "http://api.olhovivo.sptrans.com.br/v2.1"
        self.session = requests.Session()
        self.autenticado = False

    def Authenticate(self):
        url = f"{self.base_url}/Login/Autenticar?token={self.token}"
        try:
            response = self.session.post(url)
            if response.status_code == 200 and response.text.lower() == 'true':
                self.autenticado = True
                return True
            return False
        except Exception as e:
            print(f"Erro na autenticação: {e}")
            return False

    def _get(self, endpoint, params=None):
        if not self.autenticado:
            if not self.Authenticate():
                return {"erro": "Falha na autenticação"}    
        url = f"{self.base_url}{endpoint}"
        response = self.session.get(url, params=params)
        return response.json() if response.status_code == 200 else None
    
    def _get_dataframe(self, path):
        df = pd.read_csv(path,sep=',')
        return df

    def GetLine(self, termos):
        return self._get("/Linha/Buscar", params={'termosBusca': termos})
    
    def GetEnterprise(self):
        return self._get("/Empresa")
    
    def GetPosition(self):
        return self._get("/Posicao")

    def GetPrevisionLineStop(self, codigo_linha):
        return self._get("/Previsao/Linha", params={'codigoLinha': codigo_linha})
    
    def GetPrevisionStop(self, codigo_parada):
        return self._get("/Previsao/Parada", params={'codigoParada': codigo_parada})
    
    def GetAgencyDataFrame(self):
        return self._get_dataframe('dags/scripts/utils/agency.txt')
    
    def GetCalendarDataFrame(self):
        return self._get_dataframe('dags/scripts/utils/calendar.txt')
    
    def GetFareAttributesDataFrame(self):
        return self._get_dataframe('dags/scripts/utils/fare_attributes.txt')
    
    def GetFareRuleDataFrame(self):
        return self._get_dataframe('dags/scripts/utils/fare_rules.txt')
    
    def GetFrequenciesDataFrame(self):
        return self._get_dataframe('dags/scripts/utils/frequencies.txt')
    
    def GetRoutesDataFrame(self):
        return self._get_dataframe('dags/scripts/utils/routes.txt')
    
    def GetShapesDataFrame(self):
        return self._get_dataframe('dags/scripts/utils/shapes.txt')
    
    def GetStopTimesDataFrame(self):
        return self._get_dataframe('dags/scripts/utils/stop_times.txt')
    
    def GetStopsDataFrame(self):
        return self._get_dataframe('dags/scripts/utils/stops.txt')
    
    def GetTripsDataFrame(self):
        return self._get_dataframe('dags/scripts/utils/trips.txt')
 
class ExtractorSpTransAPI():
    def __init__(self):
        self.api = SPTransAPI("9dac5122b12c0e6aacfc7b6922dead75ce7368011c53b6ee9ea9011f5cc5ac24")

    def Authenticate(self): 
        if self.api.Authenticate():
            print("Conectado com sucesso!")
        else:
            print("Erro ao autenticar. Verifique o token.")

    def ExtractTotalLine(self):
        route_ids = self.api.GetRoutesDataFrame()['route_id'].unique()
        with ThreadPoolExecutor(max_workers=10) as executor:
        # map() aplica a função GetLine a cada ID da lista
            results = executor.map(self.api.GetLine, route_ids)
        data = [item for sublist in results if sublist for item in sublist]
        return data
    
    def ExtractEnterprise(self):
        data = []
        value = self.api.GetEnterprise()
        if value is not None:
                data.append(value)
        return data
    
    def ExtractTotalPosition(self):
        data = []
        value = self.api.GetPosition()
        if value is not None:
                data.append(value)
        return data
    
    def ExtractPrevisionLineStop(self):
        route_ids = self.api.GetRoutesDataFrame()['route_id'].unique()
        def get_data(route_id):
            linhas = self.api.GetLine(route_id) or []
            return [self.api.GetPrevisionLineStop(l['cl']) for l in linhas if 'cl' in l]
        
        with ThreadPoolExecutor(max_workers=10) as executor:
            results = executor.map(get_data, route_ids)
        data = [item for sublist in results for item in sublist if item]
        return data
    
    def ExtractPrevisionStop(self):
        stop_ids = self.api.GetStopsDataFrame()['stop_id'].unique()
        def fetch_data(stop_id):
            return self.api.GetPrevisionStop(stop_id)
        
        with ThreadPoolExecutor(max_workers=10) as executor:
            results = list(executor.map(fetch_data, stop_ids))
        data = [r for r in results if r is not None]
        return data
    
    def ExtractAgency(self):
        return json.loads(self.api.GetAgencyDataFrame().to_json(orient='records'))
    
    def ExtractCalendar(self):
        return json.loads(self.api.GetCalendarDataFrame().to_json(orient='records'))
    
    def ExtractFareAttributes(self):
        return json.loads(self.api.GetFareAttributesDataFrame().to_json(orient='records'))
    
    def ExtractFareRule(self):
        return json.loads(self.api.GetFareRuleDataFrame().to_json(orient='records'))
    
    def ExtractFrequencies(self):
        return json.loads(self.api.GetFrequenciesDataFrame().to_json(orient='records'))
    
    def ExtractRoutes(self):
        return json.loads(self.api.GetRoutesDataFrame().to_json(orient='records'))
    
    def ExtractShapes(self):
        return json.loads(self.api.GetShapesDataFrame().to_json(orient='records'))
    
    def ExtractStopTimes(self):
        return json.loads(self.api.GetStopsDataFrame().to_json(orient='records'))
    
    def ExtractStops(self):
        return json.loads(self.api.GetStopsDataFrame().to_json(orient='records'))
    
    def ExtractTrips(self):
        return json.loads(self.api.GetTripsDataFrame().to_json(orient='records'))
    
class Loader_Minio():
    def __init__(self):
        self.endpoint = 'http://minio:9000'
        self.key = 'projeto_final'
        self.secret = 'projeto_final'

    def LoaderMinio(self, dados, bucket_name, path, name_doc, partition):
         
         # 1. Configuração do cliente
        s3 = boto3.client(
            's3',
            endpoint_url=self.endpoint,
            aws_access_key_id=self.key,
            aws_secret_access_key=self.secret,
            region_name='us-east-1'
        )

        # 2. Montar a KEY (Caminho + Nome do Arquivo)
        if partition is True: 
            now = datetime.now()
            ano = now.strftime('%Y')
            mes = now.strftime('%m')
            dia = now.strftime('%d')
            timestamp = now.strftime('%H%M%S')
            key = f"{path}/ano={ano}/mes={mes}/dia={dia}/{name_doc}_{timestamp}.json"
        else:
            key = f"{path}/{name_doc}.json"

        # 4. Converter dados para JSON
        body = json.dumps(dados, ensure_ascii=False, indent=4)

        # 5. Upload
        s3.put_object(
            Bucket=bucket_name,
            Key=key,
            Body=body,
            ContentType='application/json'
        )
        print(f"Objeto salvo em: {bucket_name}/{key}")