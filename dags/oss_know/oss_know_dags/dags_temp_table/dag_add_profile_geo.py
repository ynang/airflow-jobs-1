from datetime import datetime
from oss_know.libs.util.log import logger
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from oss_know.libs.base_dict.variable_key import LOCATIONGEO_TOKEN

with DAG(
        dag_id='add_profile_geo_v1',
        schedule_interval='0 0 * * *',
        start_date=datetime(2000, 1, 1),
        catchup=False,
        tags=['github'],
) as dag:
    def scheduler_add_profile_geo(ds, **kwargs):
        from oss_know.libs.util.base import init_geolocator

        geolocator_token = Variable.get(LOCATIONGEO_TOKEN, deserialize_json=False)
        init_geolocator(geolocator_token)
        opensearch_conn_info = Variable.get("opensearch_conn_data", deserialize_json=True)
        do_add_profile_geo(opensearch_conn_info)
        return 'End:scheduler_add_profile_geo'


    op_scheduler_sync_github_profiles = PythonOperator(
        task_id='op_scheduler_add_profile_geo',
        python_callable=scheduler_add_profile_geo
    )


    def do_add_profile_geo(opensearch_conn_info):
        from oss_know.libs.util.base import get_opensearch_client
        from oss_know.libs.base_dict.opensearch_index import OPENSEARCH_INDEX_GITHUB_PROFILE
        from oss_know.libs.util.base import infer_country_from_location
        from oss_know.libs.util.base import infer_geo_info_from_location
        opensearch_client = get_opensearch_client(opensearch_conn_info)
        # get the profiles with location but without inferred_from_location.
        need_check_profile = opensearch_client.search(index=OPENSEARCH_INDEX_GITHUB_PROFILE,
                                                      body={
                                                          "query": {
                                                              "bool": {
                                                                  "must": {
                                                                      "exists": {
                                                                          "field": "raw_data.location"
                                                                      }
                                                                  },
                                                                  "must_not": {
                                                                      "exists": {
                                                                          "field": "raw_data.inferred_from_location"
                                                                      }
                                                                  }
                                                              }
                                                          },
                                                          "size": 10000,
                                                          "track_total_hits": True
                                                      }
                                                      )
        profiles = need_check_profile["hits"]["hits"]
        if not profiles:
            logger.info("There is no profile need to add geo info.")
            return
        profiles_total = len(profiles)
        done_profiles_num = 0
        for profile in profiles:
            done_profiles_num = done_profiles_num + 1
            update_pair = {}
            if profile:
                id = profile["_id"]
                location = profile["_source"]["raw_data"]["location"]
                if location:
                    country_inferred_from_location = infer_country_from_location(location)
                    if country_inferred_from_location:
                        update_pair["country_inferred_from_location"] = country_inferred_from_location
                    inferred_from_location = infer_geo_info_from_location(location)
                    if inferred_from_location:
                        update_pair["inferred_from_location"] = inferred_from_location
            if update_pair:
                # doc = '{"doc":%a}' % update_pair
                # opensearch_client.update(index=OPENSEARCH_INDEX_GITHUB_PROFILE, type=type, id=id, body=doc)
                opensearch_client.update(index=OPENSEARCH_INDEX_GITHUB_PROFILE, id=id, body={"doc": {
                    "raw_data": update_pair
                }})
                logger.info(f"update success opensearch id:{id}")
                return
            if done_profiles_num % 50 == 0:
                logger.info(f"{done_profiles_num}/{profiles_total} finished.")

        # from oss_know.libs.util.base import infer_country_from_location
        # from oss_know.libs.util.base import infer_geo_info_from_location
        # update_pair = {}
        # infer_country_from_location = infer_country_from_location("beijing")
        # update_pair["country_inferred_from_location"] = infer_country_from_location
        # infer_geo_info_from_location = infer_geo_info_from_location("beijing")
        # update_pair["inferred_from_location"] = infer_geo_info_from_location
        #
        # opensearch_client.update(index=OPENSEARCH_INDEX_GITHUB_PROFILE, id='C1qKFIABBxzXxBORmfI4', body={"doc": {
        #     "raw_data": update_pair
        # }})
