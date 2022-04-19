from datetime import datetime
from oss_know.libs.util.log import logger
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from oss_know.libs.base_dict.variable_key import CLICKHOUSE_DRIVER_INFO
from oss_know.libs.util.base import get_clickhouse_client
from oss_know.libs.util.base import get_opensearch_client

with DAG(
        dag_id='dag_check_os_ck_num_v1',
        schedule_interval='0 0 * * *',
        start_date=datetime(2000, 1, 1),
        catchup=False,
        tags=['clickhouse'],
) as dag:
    def do_check_os_ck_num(opensearch_conn_info, clickhouse_server_info):
        ck = get_clickhouse_client(clickhouse_server_info)
        opensearch_client = get_opensearch_client(opensearch_conn_info)
        need_check_owner_repo = [("taichi-dev", "taichi"), ("facebookincubator", "BOLT"), ("dapr", "dapr"),
                                 ("envoyproxy", "envoy"), ("CGAL", "cgal"), ("facebookresearch", "faiss"),
                                 ("ClickHouse", "ClickHouse"), ("systemd", "systemd"), ("sveltejs", "svelte"),
                                 ("jina-ai", "jina"), ("dcloudio", "uni-app")]

        need_check_table_name = ['gits', 'github_commits', 'github_pull_requests', 'github_issues',
                                 'github_issues_timeline', 'github_issues_comments']
        no_data_ck = []
        no_data_os = []
        for table_name in need_check_table_name:
            for owner, repo in need_check_owner_repo:
                sql = f"select count() from {table_name} where  search_key__owner = '{owner}' and search_key__repo = '{repo}'"
                # sql = f"select count() from gits where  search_key__owner = 'systemd' and search_key__repo = 'systemd'"
                ck_num_list = ck.execute_no_params(sql)
                ck_num = ck_num_list[0][0]

                opensearch_client.search()
                os_num_list = opensearch_client.search(index=table_name,
                                                       body={
                                                           "query": {
                                                               "bool": {
                                                                   "should": [
                                                                       {"match": {"search_key.owner": owner}},
                                                                       {"match": {"search_key.repo": repo}}

                                                                   ]
                                                               }
                                                           },
                                                           "track_total_hits": True
                                                       }
                                                       )
                os_num = os_num_list['hits']['total']['value']
                if (os_num == 0):
                    no_data_os.append((owner, repo, table_name))
                    logger.info(f'{owner}:::::{repo}的{table_name}在中os中无数据')
                elif (ck_num == os_num):
                    logger.info(f'{owner}:::::{repo}的{table_name}在ck_os数据幂等')
                elif (ck_num == 0):
                    no_data_ck.append((owner, repo, table_name))
                    logger.info(f'{owner}:::::{repo}的{table_name}在ck中无数据')
                else:
                    logger.info(f'{owner}:::::{repo}的{table_name}在os中数据为：{os_num}')
                    logger.info(f'{owner}:::::{repo}的{table_name}在ck中数据为：{ck_num}')
        logger.info(f"在os中没有数据的有{no_data_os}")
        logger.info("================================----------------------------------over")
        logger.info(f"在ck中没有数据的有{no_data_ck}")


    def dag_check_os_ck_num(ds, **kwargs):
        clickhouse_server_info = Variable.get(CLICKHOUSE_DRIVER_INFO, deserialize_json=True)
        opensearch_conn_info = Variable.get("opensearch_conn_data", deserialize_json=True)
        do_check_os_ck_num(opensearch_conn_info=opensearch_conn_info, clickhouse_server_info=clickhouse_server_info)

        return 'End:check_os_ck_num'


    op_scheduler_check_os_ck_num = PythonOperator(
        task_id='op_check_os_ck_num',
        python_callable=dag_check_os_ck_num
    )
