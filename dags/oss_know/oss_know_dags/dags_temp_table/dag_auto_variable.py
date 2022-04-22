import datetime
import json

from oss_know.libs.util.log import logger
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from oss_know.libs.base_dict.variable_key import CLICKHOUSE_DRIVER_INFO
from oss_know.libs.util.base import get_clickhouse_client
from oss_know.libs.util.base import get_opensearch_client
from datetime import datetime

with DAG(
        dag_id='auto_variable_v1',
        schedule_interval='0 0 * * *',
        start_date=datetime(2000, 1, 1),
        catchup=False,
        tags=['github'],
) as dag:
    def dag_auto_variable(ds, **kwargs):
        # clickhouse_server_info = Variable.get(CLICKHOUSE_DRIVER_INFO, deserialize_json=True)
        project_list = Variable.get("project_list", deserialize_json=True)
        print(project_list)
        now_time = datetime.utcfromtimestamp(int(datetime.now().timestamp())).strftime(
            "%Y-%m-%dT%H:%M:%SZ")
        variables = {"need_init_github_commits_repos": [], "need_init_gits": [], "need_sync_gits": []}
        for key in ("github_pull_requests", "github_issues", "github_issues_timeline", "github_issues_comments"):
            variables["need_init_" + key + "_repos"] = []
        for project in project_list:
            owner_and_repo = project[19:].split("/")
            owner = owner_and_repo[0]
            repo = owner_and_repo[1]
            commits_dict = {"owner": owner, "repo": repo, "since": "1970-01-01T00:00:00Z",
                            "until": now_time}
            gits_dict = {"owner": owner, "repo": repo, "url": f"{project}.git"}
            github_dict = {"owner": owner, "repo": repo}
            for key in variables.keys():
                if key == "need_init_github_commits_repos":
                    variables[key].append(commits_dict)
                elif key.endswith("gits"):
                    variables[key].append(gits_dict)
                else:
                    variables[key].append(github_dict)
        for variable in variables:
            print(variables[variable])
            print(type(variables[variable]))
            value = json.dumps(variables.get(variable))
            Variable.set(key=variable, value=value)
        return 'End:auto_variable'


    op_scheduler_auto_variable = PythonOperator(
        task_id='op_auto_variable',
        python_callable=dag_auto_variable
    )
