import psycopg2
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

# git_auto_init_sync_v0.0.3
from oss_know.libs.base_dict.variable_key import NEED_INIT_GITS

with DAG(
        dag_id='git_auto_init_v1',
        schedule_interval=None,
        start_date=datetime(2021, 1, 1),
        catchup=False,
        tags=['github'],
) as dag:
    conn = psycopg2.connect(host='192.168.8.108', database='airflow', user='airflow', password='airflow')
    try:
        sql = 'select * from oss_know_load_tasks'
        cur = conn.cursor()
        cur.execute(sql)
        print("rows", cur.rowcount)

        row = cur.fetchone()
        urls = set()
        while row is not None:
            if row[4] == 'queued':
                index_type = row[1]
                if index_type == 'gits':
                    url = row[5]
                    urls.add(url)
                print(row[5])
                print("type=====", type(row))

            row = cur.fetchone()
        print(urls)
        cur.close()
        git_info_list = []
        for url in urls:
            owner_and_repo = url[19:].split("/")
            owner = owner_and_repo[0]
            repo = owner_and_repo[1]
            gits_dict = {"owner": owner, "repo": repo, "url": f"{url}.git"}
            git_info_list.append(gits_dict)
        print(git_info_list)
    except(Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if not conn:
            conn.close()


    def init_sync_git_info(ds, **kwargs):
        return 'Start init_sync_git_info'


    op_auto_init_sync_git_info = PythonOperator(
        task_id='init_sync_git_info',
        python_callable=init_sync_git_info,
    )


    def do_sync_git_info(params):
        from airflow.models import Variable
        from oss_know.libs.github import init_gits
        owner = params["owner"]
        repo = params["repo"]
        url = params["url"]
        proxy_config = params.get("proxy_config")
        opensearch_conn_datas = Variable.get("opensearch_conn_data", deserialize_json=True)
        git_save_local_path = Variable.get("git_save_local_path", deserialize_json=True)
        init_sync_git_info = init_gits.init_sync_git_datas(git_url=url,
                                                           owner=owner,
                                                           repo=repo,
                                                           proxy_config=proxy_config,
                                                           opensearch_conn_datas=opensearch_conn_datas,
                                                           git_save_local_path=git_save_local_path)
        return 'do_sync_git_info:::end'


    for git_info in git_info_list:
        op_do_auto_init_sync_git_info = PythonOperator(
            task_id=f'do_sync_git_info_{git_info["owner"]}_{git_info["repo"]}',
            python_callable=do_sync_git_info,
            op_kwargs={'params': git_info},
        )

        op_auto_init_sync_git_info >> op_do_auto_init_sync_git_info
