# import the libraries

from datetime import timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from airflow.utils.dates import days_ago


#defining DAG arguments

default_args = {
    'owner': 'Nader Hachana',
    'start_date': days_ago(1),
    'email': ['naderhachana96@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}

# define the DAG    
dag = DAG(
    dag_id='bpa',
    default_args=default_args,
    description='This is a version of the Batch Pipeline Automation where I practiced the KubernetesPodOperator before switching to SparkKubernetesOperator.',
    schedule_interval=timedelta(days=1),
    start_date=days_ago(1),
    concurrency=1 # so that the tasks run only once at a time
)

# define the tasks

# This task is basically to make sure that we are working on the current directory
hw_local = BashOperator(
    task_id='hello_world_local',
    bash_command='echo "Hello World!"',
    dag=dag,
    # cwd="/mnt/c/Users/Intern/Desktop/BPA"
)
loading_raw_data = KubernetesPodOperator(
    task_id='loading_raw_data',
    namespace='spark-kubernetes',
    image='loading_raw_data:1.0',
    image_pull_policy='Never',
    cmds=[
        '/bin/bash',
        '-c',
        '/opt/spark/bin/spark-submit \
          --master k8s://https://kind-control-plane:6443 \
          --deploy-mode cluster \
          --name loading-raw-data \
          --class com.cognira.loadingRawData.Main \
          --driver-java-options -Dlog4j.configuration=file:/app/src/main/resources/log4j.properties \
          --conf spark.cassandra.auth.username=cassandra \
          --conf spark.cassandra.auth.password=cassandra \
          --conf spark.executor.instances=3 \
          --conf spark.kubernetes.authenticate.driver.serviceAccountName=spark \
          --conf spark.kubernetes.namespace=spark-kubernetes \
          --conf spark.kubernetes.container.image=loading_raw_data:1.0 \
          --conf spark.kubernetes.container.image.pullPolicy=Never \
          --conf spark.driver.host=$(hostname -i) \
          --conf spark.driver.pod.name=$(hostname) \
          --conf spark.dynamicAllocation.shuffleTracking.enabled=false \
          --conf spark.dynamicAllocation.enabled=false \
          --conf spark.kubernetes.executor.deleteOnTermination=true \
          --conf spark.kubernetes.driver.pod.name=loading-raw-data-driver \
          local:///opt/target/scala-2.12/loading_raw_data_2.12-0.1.jar'
    ],
    name='loading_raw_data',
    service_account_name='spark',
    is_delete_operator_pod=True,
    get_logs=True
)
# processing = KubernetesPodOperator(
#     task_id='processing',
#     namespace='spark-kubernetes',
#     image='processing_data:1.0',
#     image_pull_policy='Never',
#     cmds=[
#         '/bin/bash',
#         '-c',
#         '/opt/spark/bin/spark-submit \
#            --master k8s://https://kind-control-plane:6443 \
#            --deploy-mode cluster \
#            --name processing-data \
#            --class com.cognira.processing.Main \
#            --driver-java-options -Dlog4j.configuration=file:/app/src/main/resources/log4j.properties \
#            --conf spark.executor.instances=3 \
#            --conf spark.kubernetes.authenticate.driver.serviceAccountName=spark \
#            --conf spark.kubernetes.namespace=spark-kubernetes \
#            --conf spark.kubernetes.container.image=processing_data:1.0 \
#            --conf spark.kubernetes.container.image.pullPolicy=Never \
#            --conf spark.driver.host=$(hostname -i) \
#            --conf spark.driver.pod.name=$(hostname) \
#            --conf spark.dynamicAllocation.shuffleTracking.enabled=false \
#            --conf spark.dynamicAllocation.enabled=false \
#            --conf spark.kubernetes.executor.deleteOnTermination=true \
#            --conf spark.kubernetes.driver.pod.name=processing-data-driver \
#            --conf spark.kubernetes.driver.volumes.persistentVolumeClaim.metrics-pvc.options.claimName=metrics-pvc \
#            --conf spark.kubernetes.driver.volumes.persistentVolumeClaim.metrics-pvc.mount.path=/opt/transformations \
#            --conf spark.kubernetes.executor.volumes.persistentVolumeClaim.metrics-pvc.options.claimName=metrics-pvc \
#            --conf spark.kubernetes.executor.volumes.persistentVolumeClaim.metrics-pvc.mount.path=/opt/transformations \
#            local:///opt/target/scala-2.12/processing_2.12-0.1.jar'
#     ],
#     name='processing',
#     service_account_name='spark',
#     is_delete_operator_pod=True,
#     get_logs=True    
# )


# task pipeline
hw_local >> loading_raw_data