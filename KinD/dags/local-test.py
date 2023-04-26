# import the libraries
from datetime import timedelta,datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
from airflow.providers.cncf.kubernetes.sensors.spark_kubernetes import SparkKubernetesSensor
from airflow.utils.dates import days_ago
import pathlib

def get_date():
    return str(datetime.now())

default_args = {
    'owner': 'Nader Hachana',
    'start_date': days_ago(0),
    'email': ['mednader.hachana@ensi-uma.tn'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}

# define the DAG    
with DAG(
    dag_id='bpa-local-loading-raw-data',
    default_args=default_args,
    description='This dag is just for local airflow testing',
    schedule_interval=timedelta(days=1),
    catchup= False
    #concurrency=1 # so that the tasks run only once at a time
) as dag:
    # define the tasks

    # task_get_date = PythonOperator(
    #     task_id='task-get-date',
    #     python_callable=get_date,
    #     do_xcom_push=True
    # )
    loading_raw_data = SparkKubernetesOperator(
        task_id='loading-raw-data',
        namespace='airflow',
        application_file=pathlib.Path("/root/airflow/resources/loading-raw-data-job.yaml").read_text(),
        kubernetes_conn_id="kubernetes_default",
        do_xcom_push=True
    )
    checking_data_quality = SparkKubernetesOperator(
        task_id='checking-data-quality',
        namespace='airflow',
        application_file=pathlib.Path("/root/airflow/resources/checking-data-quality-job.yaml").read_text(),
        kubernetes_conn_id="kubernetes_default",
        do_xcom_push=True
    )
    processing_data = SparkKubernetesOperator(
        task_id='processing-data',
        namespace='airflow',
        application_file=pathlib.Path("/root/airflow/resources/processing-data-job.yaml").read_text(),
        kubernetes_conn_id="kubernetes_default",
        do_xcom_push=True
    )
    loading_transf_data = SparkKubernetesOperator(
        task_id='loading-transf-data',
        namespace='airflow',
        application_file=pathlib.Path("/root/airflow/resources/loading-transf-data-job.yaml").read_text(),
        kubernetes_conn_id="kubernetes_default",
        do_xcom_push=True
    )
    
    # task pipeline

    loading_raw_data >> checking_data_quality >> processing_data >> loading_transf_data
    #task_get_date >> loading_raw_data
