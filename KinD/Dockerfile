FROM apache/airflow:2.5.1-python3.10

# Copying dags to airflow dags folder
COPY ./dags/spark-pi.py /opt/airflow/dags

COPY ./dags/loading-raw-data.py /opt/airflow/dags

COPY ./dags/batch-pipeline.py /opt/airflow/dags

# Copying spark apps manifest file to resources folder in airflow
COPY ./manifests/spark-apps/test-job.yaml /opt/airflow/dags/resources/test-job.yaml

COPY ./manifests/spark-apps/loading-raw-data-job.yaml /opt/airflow/dags/resources/loading-raw-data-job.yaml

COPY ./manifests/spark-apps/checking-data-quality-job.yaml /opt/airflow/dags/resources/checking-data-quality-job.yaml

COPY ./manifests/spark-apps/processing-data-job.yaml /opt/airflow/dags/resources/processing-data-job.yaml

COPY ./manifests/spark-apps/loading-transf-data-job.yaml /opt/airflow/dags/resources/loading-transf-data-job.yaml