FROM apache/airflow:2.7.1-python3.9
USER root
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*
USER airflow
RUN pip install --no-cache-dir \
    apache-airflow-providers-mongo==3.3.0 \
    apache-airflow-providers-postgres==5.7.0 \
    apache-airflow-providers-redis==3.3.0 \
    pymongo==4.5.0 \
    psycopg2-binary==2.9.9 \
    pandas==2.0.3