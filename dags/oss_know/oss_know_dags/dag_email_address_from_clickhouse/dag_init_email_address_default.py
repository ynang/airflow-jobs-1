from airflow.utils.db import provide_session
from airflow.models import XCom
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from oss_know.libs.base_dict.variable_key import NEED_INIT_GITHUB_PROFILES_REPOS, CLICKHOUSE_DRIVER_INFO


@provide_session
def cleanup_xcom(session=None):
    dag_id = 'init_email_address_default_v1'
    session.query(XCom).filter(XCom.dag_id == dag_id).delete()


with DAG(
        dag_id='init_email_address_default_v1',
        schedule_interval=None,
        start_date=datetime(2021, 1, 1),
        catchup=False,
        tags=['email_address'],
        on_success_callback=cleanup_xcom
) as dag:
    def start_init_email_address_default(ds, **kwargs):
        return 'End start_init_email_address_default_v1'


    op_start_load_email_address_default = PythonOperator(
        task_id='load_email_address_default',
        python_callable=start_init_email_address_default,
        provide_context=True
    )


    def load_email_address_default(params, **kwargs):
        from airflow.models import Variable
        from oss_know.libs.email_address import init_email_address_from_ck
        clickhouse_server_info = Variable.get(CLICKHOUSE_DRIVER_INFO, deserialize_json=True)
        init_email_address_from_ck.load_email_address_default(clickhouse_server_info=clickhouse_server_info)
        return 'load_email_address_default:::end'


    op_load_email_address_default = PythonOperator(
        task_id='op_load_email_address_default',
        python_callable=load_email_address_default,

        provide_context=True
    )
    op_start_load_email_address_default >> op_load_email_address_default
