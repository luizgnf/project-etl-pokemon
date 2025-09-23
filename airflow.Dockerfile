FROM apache/airflow:2.5.0

USER root

RUN apt-get update && \
    apt-get install -y openjdk-11-jdk-headless unzip curl && \
    curl -L https://github.com/duckdb/duckdb/releases/download/v1.4.0/duckdb_cli-linux-amd64.zip -o duckdb.zip && \
    unzip duckdb.zip -d /usr/local/bin && \
    rm duckdb.zip && \
    chmod +x /usr/local/bin/duckdb && \
    rm -rf /var/lib/apt/lists/*

USER airflow

COPY requirements.txt .
RUN pip install -r requirements.txt