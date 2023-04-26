# import the libraries
from datetime import timedelta,datetime
from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
from airflow.providers.cncf.kubernetes.sensors.spark_kubernetes import SparkKubernetesSensor
from airflow.utils.dates import days_ago
import pathlib


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
    dag_id='batch_pipeline_automation',
    default_args=default_args,
    description='This is my gradution project dag titled Batch Pipeline Automation',
    schedule_interval=timedelta(days=1),
    catchup= False
    #concurrency=1 # so that the tasks run only once at a time
) as dag:
    # define the tasks
    loading_raw_data = SparkKubernetesOperator(
        task_id='loading-raw-data',
        namespace='airflow',
        application_file=pathlib.Path("/opt/airflow/dags/resources/loading-raw-data-job.yaml").read_text(),
        kubernetes_conn_id="kubernetes_default",
        do_xcom_push=True
    )
    loading_raw_data_status = SparkKubernetesSensor(
        task_id='loading-raw-data-status',
        namespace='airflow',
        application_name="{{ task_instance.xcom_pull(task_ids='loading-raw-data')['metadata']['name'] }}",
        kubernetes_conn_id="kubernetes_default",
        attach_log=True
    )
    checking_data_quality = SparkKubernetesOperator(
        task_id='checking-data-quality',
        namespace='airflow',
        application_file=pathlib.Path("/opt/airflow/dags/resources/checking-data-quality-job.yaml").read_text(),
        kubernetes_conn_id="kubernetes_default",
        do_xcom_push=True
    )
    checking_data_quality_status = SparkKubernetesSensor(
        task_id='checking-data-quality-status',
        namespace='airflow',
        application_name="{{ task_instance.xcom_pull(task_ids='checking-data-quality')['metadata']['name'] }}",
        kubernetes_conn_id="kubernetes_default",
        attach_log=True
    )
    processing_data = SparkKubernetesOperator(
        task_id='processing-data',
        namespace='airflow',
        application_file=pathlib.Path("/opt/airflow/dags/resources/processing-data-job.yaml").read_text(),
        kubernetes_conn_id="kubernetes_default",
        do_xcom_push=True
    )
    processing_data_status = SparkKubernetesSensor(
        task_id='processing-data-status',
        namespace='airflow',
        application_name="{{ task_instance.xcom_pull(task_ids='processing-data')['metadata']['name'] }}",
        kubernetes_conn_id="kubernetes_default",
        attach_log=True
    )
    loading_transf_data = SparkKubernetesOperator(
        task_id='loading-transf-data',
        namespace='airflow',
        application_file=pathlib.Path("/opt/airflow/dags/resources/loading-transf-data-job.yaml").read_text(),
        kubernetes_conn_id="kubernetes_default",
        do_xcom_push=True
    )
    loading_transf_data_status = SparkKubernetesSensor(
        task_id='loading-transf-data-status',
        namespace='airflow',
        application_name="{{ task_instance.xcom_pull(task_ids='loading-transf-data')['metadata']['name'] }}",
        kubernetes_conn_id="kubernetes_default",
        attach_log=True
    )
    
    # task pipeline
    loading_raw_data >> loading_raw_data_status >> checking_data_quality >> checking_data_quality_status >> processing_data >> processing_data_status >> loading_transf_data >> loading_transf_data_status
