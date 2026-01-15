import requests
import boto3
import json
import asyncio # Novo
import aiohttp # Novo
from datetime import datetime
import pandas as pd

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
        try: 
            response = self.session.get(url, params=params, timeout=60)
            if response.status_code == 200:
                return response.json()
            else:
                print(f"ERRO API {endpoint}: Status {response.status_code} - Body: {response.text[:200]}")
                return None
        except Exception as e:
            print(f"EXCEÇÃO no request {endpoint}: {e}")
            return None
    
    def _get_dataframe(self, path):
        # Ajuste o path conforme seu ambiente
        try:
            df = pd.read_csv(path, sep=',')
            return df
        except:
            print(f"Erro ao ler arquivo: {path}")
            return pd.DataFrame()

    # Métodos síncronos mantidos para compatibilidade
    def GetLine(self, termos):
        return self._get("/Linha/Buscar", params={'termosBusca': termos})
    
    def GetEnterprise(self):
        return self._get("/Empresa")
    
    def GetPosition(self):
        return self._get("/Posicao")

    def GetPrevisionLineStop(self, codigo_linha):
        return self._get("/Previsao/Linha", params={'codigoLinha': codigo_linha})
    
    # Este método será substituído pela versão assíncrona na classe Extractor
    def GetPrevisionStop(self, codigo_parada):
        return self._get("/Previsao/Parada", params={'codigoParada': codigo_parada})
    
    # ... (Seus métodos GetXDataFrame continuam iguais abaixo) ...
    def GetAgencyDataFrame(self): return self._get_dataframe('dags/scripts/utils/agency.txt')
    def GetCalendarDataFrame(self): return self._get_dataframe('dags/scripts/utils/calendar.txt')
    def GetFareAttributesDataFrame(self): return self._get_dataframe('dags/scripts/utils/fare_attributes.txt')
    def GetFareRuleDataFrame(self): return self._get_dataframe('dags/scripts/utils/fare_rules.txt')
    def GetFrequenciesDataFrame(self): return self._get_dataframe('dags/scripts/utils/frequencies.txt')
    def GetRoutesDataFrame(self): return self._get_dataframe('dags/scripts/utils/routes.txt')
    def GetShapesDataFrame(self): return self._get_dataframe('dags/scripts/utils/shapes.txt')
    def GetStopTimesDataFrame(self): return self._get_dataframe('dags/scripts/utils/stop_times.txt')
    def GetStopsDataFrame(self): return self._get_dataframe('dags/scripts/utils/stops.txt')
    def GetTripsDataFrame(self): return self._get_dataframe('dags/scripts/utils/trips.txt')
 
class ExtractorSpTransAPI():
    def __init__(self):
        self.api = SPTransAPI("9dac5122b12c0e6aacfc7b6922dead75ce7368011c53b6ee9ea9011f5cc5ac24")

    def Authenticate(self): 
        if self.api.Authenticate():
            print("Conectado com sucesso (Sessão Síncrona)!")
        else:
            print("Erro ao autenticar. Verifique o token.")

    def ExtractTotalLine(self):
        try:
            route_ids = self.api.GetRoutesDataFrame()['route_id'].unique()
            # route_ids = route_ids[:20] # Descomente para testes rápidos
            print(f"Iniciando busca assíncrona para {len(route_ids)} rotas...")
        except Exception as e:
            print(f"Erro ao carregar rotas: {e}")
            return []

        async def _run():
            try:
                # Timeout total generoso para processar a lista inteira
                async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=300)) as session:
                    
                    # 1. Autenticação
                    resp_auth = await session.post(f"{self.api.base_url}/Login/Autenticar?token={self.api.token}")
                    if (await resp_auth.text()).lower() != 'true':
                        print("Erro: Falha na autenticação.")
                        return []

                    # 2. Definição da Tarefa de Busca (com Limite de Concorrência)
                    sem = asyncio.Semaphore(50) # Max 50 requests simultâneos

                    async def fetch_line(term):
                        async with sem:
                            try:
                                async with session.get(f"{self.api.base_url}/Linha/Buscar?termosBusca={term}") as resp:
                                    if resp.status == 200:
                                        return await resp.json()
                            except Exception:
                                pass # Ignora erros individuais para não parar o fluxo
                            return []

                    # 3. Disparo das tarefas
                    tasks = [fetch_line(term) for term in route_ids]
                    raw_results = await asyncio.gather(*tasks)

                    # 4. Flattening (Achatamento da lista)
                    # A API retorna [[linhaA, linhaB], [linhaC], []]. Precisamos de [linhaA, linhaB, linhaC]
                    flat_data = [item for sublist in raw_results if sublist for item in sublist]
                    
                    print(f"Sucesso! {len(flat_data)} linhas encontradas.")
                    return flat_data

            except Exception as e:
                print(f"Erro crítico no loop assíncrono: {e}")
                return []

        return asyncio.run(_run())
    
    def ExtractEnterprise(self):
        data = []
        value = self.api.GetEnterprise()
        if value is not None:
                data.append(value)
        return data
    
    def ExtractTotalPosition(self):
        print("Iniciando extração robusta (/Posicao)...")
        
        MAX_RETRIES = 10
        DELAY_ENTRE_TENTATIVAS = 1 # segundos

        async def _run():
            # Loop de Tentativas
            for tentativa in range(1, MAX_RETRIES + 1):
                try:
                    async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=30)) as session:
                        
                        # 1. Autenticação
                        # print(f"Tentativa {tentativa}/{MAX_RETRIES}: Autenticando...")
                        resp_auth = await session.post(f"{self.api.base_url}/Login/Autenticar?token={self.api.token}")
                        
                        if resp_auth.status != 200 or (await resp_auth.text()).lower() != 'true':
                            print(f"[Tentativa {tentativa}] Falha na autenticação.")
                            await asyncio.sleep(DELAY_ENTRE_TENTATIVAS)
                            continue # Tenta de novo

                        # 2. Busca dos Dados
                        # print(f"Tentativa {tentativa}/{MAX_RETRIES}: Baixando dados...")
                        async with session.get(f"{self.api.base_url}/Posicao") as response:
                            
                            # Se a API der erro 500/502/503/504, isso vai cair no retry
                            if response.status != 200:
                                print(f"[Tentativa {tentativa}] Erro API: Status {response.status}")
                                await asyncio.sleep(DELAY_ENTRE_TENTATIVAS)
                                continue # Tenta de novo
                            
                            data = await response.json()
                            
                            # Validação: Se o JSON tem a chave 'l' e ela não está vazia
                            if data and data.get('l'):
                                print(f"SUCESSO na tentativa {tentativa}! {len(data['l'])} veículos obtidos.")
                                return [data]
                            else:
                                if tentativa < MAX_RETRIES + 1:
                                    print(f"[Tentativa {tentativa}] API retornou 200, mas sem lista de veículos. Realizando nova tentativa")
                                    continue
                                else:
                                    print(f"[Tentativa {tentativa}] API retornou 200, mas sem lista de veículos.")
                                    return []
                                # Não fazemos retry se a API retornou 200 vazio, pois pode ser horário noturno (sem ônibus)
                                # Se quiser forçar retry mesmo assim, mude para 'continue'
                                

                except asyncio.TimeoutError:
                    print(f"[Tentativa {tentativa}] Timeout! A API demorou demais para responder.")
                    await asyncio.sleep(DELAY_ENTRE_TENTATIVAS)
                
                except Exception as e:
                    print(f"[Tentativa {tentativa}] Erro de conexão/processamento: {e}")
                    await asyncio.sleep(DELAY_ENTRE_TENTATIVAS)

            print(f"FALHA DEFINITIVA após {MAX_RETRIES} tentativas.")
            return []

        return asyncio.run(_run())
    
    def ExtractPrevisionLineStop(self):
        """
        Extração Assíncrona em Duas Etapas:
        1. Busca os códigos identificadores de linha ('cl') baseado nas rotas.
        2. Busca as previsões para cada 'cl' encontrado.
        """
        
        # 1. Obtém lista de rotas do arquivo estático (GTFS)
        # Ex: '8000-10', '917H-10'...
        route_ids = self.api.GetRoutesDataFrame()['route_id'].unique()
        
        # Opcional: Limitar para testes
        # route_ids = route_ids[:20] 
        
        print(f"Iniciando extração de linhas para {len(route_ids)} rotas...")

        # Função auxiliar para buscar o 'cl' (Código Linha)
        async def fetch_line_code(session, route_id, sem):
            url = f"{self.api.base_url}/Linha/Buscar?termosBusca={route_id}"
            async with sem:
                try:
                    async with session.get(url) as response:
                        if response.status == 200:
                            return await response.json()
                        return []
                except Exception as e:
                    # Erros aqui são comuns se a rota do GTFS não existir na API Tempo Real
                    return []

        # Função auxiliar para buscar a previsão dado um 'cl'
        async def fetch_prevision(session, cl, sem):
            url = f"{self.api.base_url}/Previsao/Linha?codigoLinha={cl}"
            async with sem:
                try:
                    async with session.get(url) as response:
                        if response.status == 200:
                            return await response.json()
                        return None
                except Exception:
                    return None

        async def run_tasks(routes):
            async with aiohttp.ClientSession() as session:
                # 1. Autenticação Assíncrona
                auth_url = f"{self.api.base_url}/Login/Autenticar?token={self.api.token}"
                async with session.post(auth_url) as resp:
                    if resp.status != 200 or (await resp.text()).lower() != 'true':
                        print("Erro crítico: Falha na autenticação assíncrona.")
                        return []

                sem = asyncio.Semaphore(50) # Controle de concorrência

                # --- FASE 1: Descobrir os 'cl' (Códigos de Linha) ---
                print("Fase 1: Buscando códigos de linha (cl)...")
                tasks_lines = [fetch_line_code(session, rid, sem) for rid in routes]
                results_lines = await asyncio.gather(*tasks_lines)

                # Processa o resultado (Flattening): 
                # A API retorna uma lista de linhas para cada busca. Precisamos extrair apenas os 'cl'.
                list_of_cls = []
                for result in results_lines:
                    if result:
                        for linha in result:
                            if 'cl' in linha:
                                list_of_cls.append(linha['cl'])
                
                # Remove duplicatas de 'cl' (mesma linha pode aparecer em buscas diferentes)
                unique_cls = list(set(list_of_cls))
                print(f"Fase 1 Concluída. {len(unique_cls)} linhas únicas encontradas.")

                # --- FASE 2: Buscar Previsões ---
                print("Fase 2: Buscando previsões dos veículos...")
                tasks_prev = [fetch_prevision(session, cl, sem) for cl in unique_cls]
                results_prev = await asyncio.gather(*tasks_prev)

                return [r for r in results_prev if r is not None]

        # Executa o loop
        data = asyncio.run(run_tasks(route_ids))
        print(f"Extração concluída. {len(data)} previsões de linha obtidas.")
        return data
    
    def ExtractPrevisionStop(self):
        """
        Substitui ThreadPoolExecutor por AsyncIO para performance extrema na coleta.
        """
        
        # 1. Carrega os IDs das paradas
        stop_ids = self.api.GetStopsDataFrame()['stop_id'].unique()
        # Para teste rápido, descomente a linha abaixo para limitar a 10 paradas
        # stop_ids = stop_ids[:10] 
        print(f"Iniciando extração assíncrona para {len(stop_ids)} paradas...")

        async def fetch_async(session, stop_id, sem):
            url = f"{self.api.base_url}/Previsao/Parada?codigoParada={stop_id}"
            async with sem: # Limita a concorrência
                try:
                    async with session.get(url) as response:
                        if response.status == 200:
                            data = await response.json()
                            if data: # Se retornou dados válidos
                                # Injetamos o ID para saber de qual parada veio esse dado
                                data['codigo_parada_solicitado'] = int(stop_id)
                                return data
                        return None
                except Exception as e:
                    print(f"Erro no stop_id {stop_id}: {e}")
                    return None

        async def run_tasks(ids):
            # Cria uma sessão assíncrona dedicada
            async with aiohttp.ClientSession() as session:
                # 1. Autentica a sessão assíncrona (Necessário pois é uma nova conexão)
                auth_url = f"{self.api.base_url}/Login/Autenticar?token={self.api.token}"
                async with session.post(auth_url) as resp:
                    if resp.status != 200 or (await resp.text()).lower() != 'true':
                        print("Erro crítico: Falha na autenticação assíncrona.")
                        return []

                # 2. Prepara as tarefas com limitação (Semáforo)
                # 50 requests simultâneos é um bom número para não tomar timeout
                sem = asyncio.Semaphore(50) 
                tasks = [fetch_async(session, sid, sem) for sid in ids]
                
                # 3. Executa tudo
                results = await asyncio.gather(*tasks)
                return [r for r in results if r is not None]

        # Roda o loop assíncrono dentro do código síncrono
        data = asyncio.run(run_tasks(stop_ids))
        print(f"Extração concluída. {len(data)} registros obtidos.")
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
        return json.loads(self.api.GetStopTimesDataFrame().to_json(orient='records'))
    
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
        if not dados:
            print("Nenhum dado para salvar.")
            return 
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
            timestamp = now.strftime('%Y%m%d%H%M%S')
            key = f"{path}/ano={ano}/mes={mes}/dia={dia}/{name_doc}_{timestamp}.json"
        else:
            key = f"{path}/{name_doc}.json"

        # 4. Converter dados para JSON
        body = json.dumps(dados, ensure_ascii=False)
        
        # 5. Upload
        try:
            s3.put_object(
                Bucket=bucket_name,
                Key=key,
                Body=body,
                ContentType='application/json'
            )
            print(f"Objeto salvo em: {bucket_name}/{key}")
        except Exception as e:
            print(f"Erro ao salvar no MinIO: {e}")
        
        
