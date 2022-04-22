from datetime import datetime
from oss_know.libs.util.log import logger
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2000, 1, 1),
    'schedule_interval': '0 0 * * *'
}
dag = DAG(dag_id='dag_auto_variable_v111', catchup=False, default_args=default_args, tags=['github'])

op_scheduler_auto_variable = BashOperator(
    task_id='op_auto_variable',
    bash_command='airflow variables --import ../../../../data_schema/variables/big_variable0.json',
    # bash_command='docker-compose run --rm webserver airflow variables --import /home/fskhex/Project/airflow-jobs/data_schema/variables/big_variable0.json',
    dag=dag)

op_scheduler_auto_variable
