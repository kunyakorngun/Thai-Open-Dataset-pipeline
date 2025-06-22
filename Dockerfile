FROM --platform=linux/amd64 apache/airflow:2.9.0

RUN pip install ccxt==4.1.100 \
  apache-airflow-providers-mongo==3.5.0 \
  airflow-provider-great-expectations==0.2.7 \
  openpyxl==3.1.2 \
  pyodbc==5.1.0 \
  Requests==2.31.0 \
  tqdm==4.66.2 \ 
  minio==7.2.7\
  s3fs==2025.3.2
COPY dags/package_batches.json dags/
FROM apache/airflow:2.9.1-python3.10

USER airflow
RUN pip install --no-cache-dir psycopg2-binary pandas
USER airflow
RUN pip install --no-cache-dir minio