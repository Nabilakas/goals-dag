

from airflow import DAG
from datetime import timedelta
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable
from airflow.utils.dates import days_ago

import logging
import time
import datetime
import goals_buckets_data as util
doc=""""""
default_args = {
    'owner': 'Gulraiz',
    'start_date': days_ago(2),
    'email': ['developer.gulraiz@gmail.com','nouman@dotlabs.ai', 'gulraiz@dotlabs.ai'],  # List of email recipients
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

dag = DAG('goals_dag', default_args=default_args, schedule_interval='0 3 * * *')

dag.__doc__=doc

etl_task = PythonOperator(
    task_id='run_etl',
    provide_context=True,
    python_callable=util.dev_etl,
    op_kwargs={},
    dag=dag
)




# t1 = BashOperator(
#     task_id= "run_etl",
#     bash_command = 'python3 /usr/local/airflow/dags/analystics_dag.py', 
#     dag = dag
# )
# t1