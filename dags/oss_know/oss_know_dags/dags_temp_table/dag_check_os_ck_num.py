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
        need_check_owner_repo = [("rust-lang", "rust"), ("rust-lang", "cargo"), ("rust-lang", "rfcs"), ("o3de", "o3de"),
                                 ("org", "pub"), ("org", "pub"), ("org", "pub"), ("org", "pub"), ("org", "pub"),
                                 ("taichi-dev", "taichi"), ("taichi-dev", "taichi_elements"),
                                 ("taichi-dev", "games201"), ("bioinfo-pf-curie", "TMB"),
                                 ("bioinfo-pf-curie", "mpiBWA"), ("bioinfo-pf-curie", "trainings"),
                                 ("bioinfo-pf-curie", "HiTC"), ("bioinfo-pf-curie", "nf-VIF"),
                                 ("bioinfo-pf-curie", "geniac"), ("qos-ch", "slf4j"), ("qos-ch", "logback"),
                                 ("qos-ch", "reload4j"), ("tensorflow", "tensorflow"), ("pytorch", "pytorch"),
                                 ("ray-project", "ray"), ("envoyproxy", "envoy"), ("cockroachdb", "cockroach"),
                                 ("microsoft", "DeepSpeed"), ("lxc", "lxc"), ("libarchive", "libarchive"),
                                 ("moby", "moby"), ("systemd", "systemd"), ("scanoss", "scanner.c"),
                                 ("scanoss", "engine"), ("scanoss", "scanner.py"), ("scanoss", "quickscan"),
                                 ("scanoss", "audit-workbench"), ("scanoss", "scanoss.py"), ("iovisor", "bcc"),
                                 ("facebookincubator", "BOLT"), ("oracle", "graal"), ("JuliaLang", "julia"),
                                 ("intel", "llvm"), ("eclipse-openj9", "openj9"), ("sveltejs", "svelte"),
                                 ("dcloudio", "uni-app"), ("dapr", "dapr"), ("jupyter", "notebook"),
                                 ("jupyter", "docker-stacks"), ("jupyter", "jupyter_client"), ("jupyter", "nbformat"),
                                 ("jupyter", "jupyter.github.io"), ("wasmerio", "wasmer"), ("google", "jax"),
                                 ("PaddlePaddle", "Paddle"), ("DPDK", "dpdk"), ("CGAL", "cgal"),
                                 ("mapbox", "mapbox-gl-js"), ("facebookresearch", "faiss"), ("jina-ai", "jina"),
                                 ("ZJU-OpenKS", "OpenKS"), ("delta-io", "delta"), ("greenplum-db", "gpdb"),
                                 ("apache", "hudi"), ("apache", "iceberg"), ("MariaDB", "server"),
                                 ("MariaDB", "mariadb-docker"), ("MariaDB", "galera"), ("MariaDB", "mariadb.org-tools"),
                                 ("MariaDB", "randgen"), ("MariaDB", "webscalesql-5.6"), ("mongodb", "mongo"),
                                 ("mongodb", "node-mongodb-native"), ("mongodb", "mongo-go-driver"),
                                 ("mongodb", "mongoid"), ("mongodb", "mongo-python-driver"),
                                 ("mongodb", "mongo-csharp-driver"), ("mysql", "mysql-server"), ("neo4j", "neo4j"),
                                 ("planetscale", "beam"), ("planetscale", "docs"), ("planetscale", "cli"),
                                 ("planetscale", "planetscale-go"), ("planetscale", "vitess"),
                                 ("ClickHouse", "ClickHouse"), ("questdb", "questdb"), ("facebook", "rocksdb"),
                                 ("P-H-C", "phc-winner-argon2"), ("hercules-team", "augeas"), ("Gandi", "bridge-utils"),
                                 ("google", "brotli"), ("libarchive", "bzip2"), ("coreutils", "coreutils"),
                                 ("Distrotech", "cpio"), ("cracklib", "cracklib"), ("cronie-crond", "cronie"),
                                 ("cyrusimap", "cyrus-sasl"), ("imartinezortiz", "7-zip-lzma-sdk-java"),
                                 ("alsa-project", "alsa-lib"), ("linux-audit", "audit-userspace"),
                                 ("linux-audit", "audit-kernel"), ("brltty", "brltty"), ("meefik", "busybox"),
                                 ("c-ares", "c-ares"), ("mbroz", "cryptsetup"), ("SSSD", "ding-libs"),
                                 ("imp", "dnsmasq"), ("docker", "compose"), ("docker", "roadmap"),
                                 ("docker", "awesome-compose"), ("docker", "build-push-action"),
                                 ("docker", "compose-cli"), ("docker", "node-sdk")]
        need_check_table_name = ['gits', 'github_commits', 'github_pull_requests', 'github_issues',
                                 'github_issues_timeline', 'github_issues_comments']
        no_data_ck = []
        no_data_os = []
        for table_name in need_check_table_name:
            for owner, repo in need_check_owner_repo:
                sql = f"select count() from {table_name} where  search_key__owner = '{owner}' and search_key__repo = '{repo}'"
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
