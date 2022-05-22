# import airflow_client.client
# from airflow_client.client.api import dag_run_api, variable_api
# import json
# from airflow_client.client.model.variable import Variable
# from airflow_client.client.model.dag_run import DAGRun
from datetime import datetime, timezone
import psycopg2


#
# In case of the basic authentication below. Make sure:
#  - Airflow is configured with the basic_auth as backend:
#     auth_backend = airflow.api.auth.backend.basic_auth
#  - Make sure that the client has been generated with securitySchema Basic.

# Configure HTTP basic authorization: Basic
# configuration = airflow_client.client.Configuration(
#     host="http://192.168.8.108:8080/api/v1",
#     username='airflow',
#     password='airflow'
# )

dag_id_variable_key_pair = {"github_init_commits_v1": "need_init_github_commits_repos",
                            "github_init_pull_requests_v1": "need_init_github_pull_requests_repos",
                            "github_init_issues_v1": "need_init_github_issues_repos",
                            "github_init_issues_timeline_v1": "need_init_github_issues_timeline_repos",
                            "github_init_issues_comments_v1": "need_init_github_issues_comments_repos",
                            "github_init_profile_v1": "need_init_github_profiles_repos"}
# todo:
# if str(valid_classes[0]) != "<class \'airflow_client.client.model.sla_miss.SLAMiss\'>":

def add_jobs(add_json: str):
    pass
    #！！！add_json 拆开
    #！！！--保存json 单条到DB
    #！！！--写入数据库一条提示信息(https://github.com/fivestarsky/airflow-jobs 已经排入今晚数据获取任务)

    # 想明白怎么调度任务以来再写下面的，不明确前，手动激发
    # 等待3分钟(模仿定时任务，在任务开始前，airflow已经从db中读取任务并创建好task)
    # dag_run_api.DAGRunApi  启动Dag工作

    # !/usr/bin/python

    import psycopg2



""" insert a new vendor into the vendors table """
sql = """INSERT INTO oss_know_load_tasks(task_type,url,task_created_at,task_status)
         VALUES('gits','https://github.com/sgda/dsge',datetime.datetime.now().timestamp(),'queued') RETURNING id;"""
conn = None
vendor_id = None
try:
    conn = psycopg2.connect(host='192.168.8.108', database='airflow', user='airflow', password='airflow')
    cur = conn.cursor()
    cur.execute(sql, ('oss_know_load_tasks',))
    oss_know_load_task_id = cur.fetchone()[0]
    print(oss_know_load_task_id)
    conn.commit()
    cur.close()
except (Exception, psycopg2.DatabaseError) as error:
    print(error)
finally:
    if conn is not None:
        conn.close()




    # with airflow_client.client.ApiClient(configuration) as api_client:
    # # todo: Trigger a dag
    # api_dag_run_api = dag_run_api.DAGRunApi(api_client)
    # dag_ids = {'github_init_commits_v1', 'github_init_issues_v1', 'github_init_pull_requests_v1'}
    #
    # for dag_id in dag_ids:
    #     dag_run_id = dag_id + datetime.now(timezone.utc).isoformat()
    #
    #     dag_run = DAGRun(
    #         dag_run_id=dag_run_id,
    #         conf={}
    #     )
    #     try:
    #         # Trigger a new DAG run
    #         api_response = api_dag_run_api.post_dag_run(
    #             dag_id=dag_id,
    #             dag_run=dag_run)
    #         print(api_response)
    #
    #     except airflow_client.client.ApiException as e:
    #         print("Exception when calling DAGRunApi->get_dag_run: %s\n" % e)

#main()
    # add_jobs("json")