from airflow import DAG
from datetime import timedelta
from airflow.operators.python_operator import PythonOperator
from airflow.operators.email import EmailOperator
from airflow.models import Variable
from airflow.utils.dates import days_ago

import logging
import time
import datetime
import goals_plus_swagger_code as util

doc = """Goals and Swagger"""


default_args = {
    'owner': 'Gulraiz',
    'start_date': days_ago(1),
    'email': ['developer.gulraiz@gmail.com','nouman@dotlabs.ai', 'gulraiz@dotlabs.ai'],  # List of email recipients
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}
dag = DAG('goals_plus_swagger_dag', default_args=default_args, schedule_interval='*/10 * * * *', concurrency=1)# Modified schedule_interval


dag.__doc__=doc

etl_task = PythonOperator(
    task_id='run_etl',
    provide_context=True,
    python_callable=util.dev_etl,
    op_kwargs={},
    dag=dag
)




