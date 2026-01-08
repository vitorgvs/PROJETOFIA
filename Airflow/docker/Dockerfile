FROM apache/airflow:2.7.2-python3.11

# Instalar dependências do sistema
USER root
RUN apt-get update && apt-get install -y \
    openjdk-17-jdk \
    build-essential \
    python3-dev \
    gcc \
    libffi-dev \
    libssl-dev \
    curl \
    && apt-get clean

# Configurar JAVA_HOME
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64/
ENV PATH=$JAVA_HOME/bin:$PATH

# Voltar para o usuário airflow
USER airflow

# Atualizar pip e instalar PySpark + delta-spark
RUN pip install --upgrade pip
RUN pip install --no-cache-dir \
    pyspark==3.5.1 \
    delta-spark==2.3.0 \
    pandas \
    requests
