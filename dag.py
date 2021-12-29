from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG

from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'hdfs',
    'depends_on_past': False,
    'start_date' : datetime(2021,12,28),
    'email': ['mehroosali@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    'ABCStoresPipeline',
    default_args=default_args,
    description='ABC Stores DAG',
    schedule_interval=timedelta(days=1),
    catchup=False,
) as dag:

    t1 = BashOperator(
        task_id='DailyDataIngestAndRefine',
        depends_on_past=False,
        bash_command='spark-submit --master yarn --deploy-mode cluster --class main.scala.DailyDataIngestAndRefine /home/ABCStoresPipeline/Code/target/ABCStoresPipeline-1.0-ABCStores.jar',
    )

    t2 = BashOperator(
        task_id='EnrichProductReference',
        depends_on_past=False,
        bash_command='spark-submit --master yarn --deploy-mode cluster --class main.scala.EnrichProductReference /home/ABCStoresPipeline/Code/target/ABCStoresPipeline-1.0-ABCStores.jar',
    )
   
    t3 = BashOperator(
        task_id='VendorEnrichment',
        depends_on_past=False,
        bash_command='spark-submit --master yarn --deploy-mode cluster --class main.scala.VendorEnrichment /home/ABCStoresPipeline/Code/target/ABCStoresPipeline-1.0-ABCStores.jar',
    )

    t1 >> t2 >> t3
