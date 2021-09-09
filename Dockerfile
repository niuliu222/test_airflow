FROM apache/airflow

# NOTE: dag path is set with the `dags.path` value
COPY ./dags /opt/airflow/dags