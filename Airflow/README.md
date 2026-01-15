#Airflow

Este repositório contém uma configuração do Apache Airflow utilizando Docker e Docker Compose. O projeto inclui DAGs relacionadas com a camada raw e trusted para ingestão de dados a partir da API pública Olho vivo da SPTrans (documentação: https://www.sptrans.com.br/desenvolvedores/api-do-olho-vivo-guia-de-referencia/documentacao-api/) com armazenamento no Minio.

## Requisitos Mínimos

- Docker (versão 20.10 ou superior)
- Docker Compose (versão 1.29 ou superior)
- Python (versão 3.9 ou superior, se desejar executar scripts localmente)
- Acesso à internet para baixar imagens e dependências

## Estrutura do Projeto

```
Airflow/
├── dags/
|   └── dag_sptrans_api_raw_dim.py
|   └── dag_sptrans_api_raw_fact.py
|   └── dag_sptrans_api_trusted_fact.py
|   └── dag_sptrans_api_trusted_dim.py
│   └── scripts/
|       └── raw_to_trusted_position.py
|       └── raw_to_trusted_prevision_stop.py
|       └── raw_to_trusted_stops.py
|       └── sptrans_api.py
│       └── utils/
|           └── agency.txt
|           └── calendar.txt
|           └── fare_attributes.txt
|           └── fare_rules.txt
|           └── frequencies.txt
|           └── routes.txt
|           └── shapes.txt
|           └── stop_times.txt
|           └── stops.txt
|           └── trips.txt
├── docker/
│   ├── Dockerfile
│   └── docker-compose.yml
├── .env
├── poetry.lock
├── pyproject.toml
├── requirements.txt
└── README.md
```

## Inicializar o Ambiente

Para inicializar o serviço, utilize o comando:

```shell
docker-compose up -d minio webserver scheduler
```

> *Caso não funcione o comando acima*, utilize `docker-compose` no lugar de `docker compose`

## Criar o Usuário para Fazer Login no Airflow

Após inicializar o serviço, crie um usuário admin para acessar a interface do Airflow:

- Executar o comando abaixo no terminal

```shell
docker compose exec webserver airflow users create \
    --username admin \
    --firstname Firstname \
    --lastname Lastname \
    --role Admin \
    --email admin@example.com \
    --password admin
```

### Utilizando interface gráfica

1. Acesse a interface do Airflow em [http://localhost:8080](http://localhost:8080) e faça login com as credenciais criadas.


