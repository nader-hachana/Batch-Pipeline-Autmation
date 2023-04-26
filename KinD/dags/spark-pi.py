# import the libraries
import os
from datetime import timedelta,datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
from airflow.providers.cncf.kubernetes.sensors.spark_kubernetes import SparkKubernetesSensor
from airflow.utils.dates import days_ago
import pathlib


def get_date():
    return str(datetime.now())
#defining DAG arguments
# this_dag_dir = os.path.dirname(os.path.abspath(__file__))

default_args = {
    'owner': 'Nader Hachana',
    'start_date': days_ago(0),
    'email': ['naderhachana96@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}

# define the DAG    
with DAG(
    dag_id='bpa-spark-pi',
    default_args=default_args,
    description='Testing spark-pi on airflow helm chart',
    schedule_interval=timedelta(days=1),
    catchup= False
    #concurrency=1 # so that the tasks run only once at a time
) as dag:
    # define the tasks
    task_get_date = PythonOperator(
        task_id='task-get-date',
        python_callable=get_date,
        do_xcom_push=True
    )
    spark_pi = SparkKubernetesOperator(
        task_id='spark-pi',
        namespace='airflow',
        application_file=pathlib.Path("/opt/airflow/dags/resources/test-job.yaml").read_text(),
        kubernetes_conn_id="kubernetes_default",
        do_xcom_push=True
    )
    spark_pi_status = SparkKubernetesSensor(
        task_id='spark-pi-status',
        namespace='airflow',
        application_name="{{ task_instance.xcom_pull(task_ids='spark-pi')['metadata']['name'] }}",
        kubernetes_conn_id="kubernetes_default",
        attach_log=True
    )
    # task pipeline
    task_get_date >> spark_pi >> spark_pi_status
