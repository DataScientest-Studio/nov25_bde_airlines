FROM apache/airflow:2.8.1

USER root

# (optionnel) utilitaires de base
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
  && rm -rf /var/lib/apt/lists/*

USER airflow

# Installer les dépendances Python en respectant les constraints Airflow
COPY requirements.txt /opt/airflow/requirements.txt

ARG AIRFLOW_VERSION=2.8.1
ARG PYTHON_VERSION=3.8

RUN pip install --no-cache-dir \
    -r /opt/airflow/requirements.txt \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"
