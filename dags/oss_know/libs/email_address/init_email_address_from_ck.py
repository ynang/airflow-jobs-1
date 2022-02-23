import datetime

from oss_know.libs.util.clickhouse_driver import CKServer
from oss_know.libs.util.base import infer_country_from_emailcctld, infer_country_from_emaildomain, \
    infer_company_from_emaildomain, infer_country_from_company
from oss_know.libs.base_dict.clickhouse import CLICKHOUSE_EMAIL_ADDRESS, EMAIL_ADDRESS_SEARCH_KEY__UPDATED_AT, \
    EMAIL_ADDRESS_SEARCH_KEY__EMAIL, EMAIL_ADDRESS_EMIAL, EMAIL_ADDRESS_COUNTRY_INFERRED_FROM_EMAILCCTLD, \
    EMAIL_ADDRESS_COUNTRY_INFERRED_FROM_EMAILDOMAIN, EMAIL_ADDRESS_COMPANY_INFERRED_FROM_EMAIL, \
    EMAIL_ADDRESS_COUNTRY_INFERRED_FROM_COMPANY


def load_all_email_address(clickhouse_server_info):
    ck = CKServer(host=clickhouse_server_info["HOST"],
                  port=clickhouse_server_info["PORT"],
                  user=clickhouse_server_info["USER"],
                  password=clickhouse_server_info["PASSWD"],
                  database=clickhouse_server_info["DATABASE"])

    gits_sql = "SELECT DISTINCT author_email,committer_email FROM gits"
    gits_email = ck.execute_no_params(gits_sql)
    all_email_address_dict = {}
    for author_email, committer_email in gits_email:
        for item in author_email, committer_email:
            if item:
                all_email_address_dict[item] = 0
    for k, v in all_email_address_dict.items():
        committer_id_sql = f"SELECT DISTINCT committer__id,commit__committer__email from github_commits WHERE commit__committer__email = '{k}'"
        committer_id = ck.execute_no_params(committer_id_sql)
        author_id_sql = f"SELECT DISTINCT author__id,commit__author__email from github_commits WHERE commit__author__email = '{k}'"
        author_id = ck.execute_no_params(author_id_sql)
        for item in committer_id, author_id:
            if item and item[0][0]:
                email = item[0][1]
                id = item[0][0]
                all_email_address_dict[email] = id

    profile_sql = "SELECT DISTINCT email,id FROM github_profile"
    profile_email_id_pair = ck.execute_no_params(profile_sql)
    for email, id in profile_email_id_pair:
        if email:
            all_email_address_dict[email] = id

    get_profile_table_columns_sql = "select distinct name from system.columns where database = 'default' AND table = 'github_profile'"
    profile_columns = ck.execute_no_params(get_profile_table_columns_sql)
    profile_columns_len = len(profile_columns)
    profile_value = {}

    count = 0
    values_to_insert = []
    for k, v in all_email_address_dict.items():
        value = {}
        value[EMAIL_ADDRESS_SEARCH_KEY__UPDATED_AT] = int(datetime.datetime.now().timestamp() * 1000)
        value[EMAIL_ADDRESS_SEARCH_KEY__EMAIL] = k
        value[EMAIL_ADDRESS_EMIAL] = k
        country_by_emailcctld = infer_country_from_emailcctld(k)
        country_by_emaildomain = infer_country_from_emaildomain(k)
        company_by_email = infer_company_from_emaildomain(k)
        for m, n in (EMAIL_ADDRESS_COUNTRY_INFERRED_FROM_EMAILCCTLD, country_by_emailcctld), (
                EMAIL_ADDRESS_COUNTRY_INFERRED_FROM_EMAILDOMAIN, country_by_emaildomain), (
                            EMAIL_ADDRESS_COMPANY_INFERRED_FROM_EMAIL, company_by_email):
            if n:
                value[m] = n
            else:
                value[m] = ''
        if company_by_email:
            country_by_company = infer_country_from_company(company_by_email)
            if not country_by_company:
                value[EMAIL_ADDRESS_COUNTRY_INFERRED_FROM_COMPANY] = country_by_company
        else:
            value[EMAIL_ADDRESS_COUNTRY_INFERRED_FROM_COMPANY] = ''

        github_profile_sql = f"select * from github_profile where search_key__updated_at = (select MAX(search_key__updated_at) from github_profile where github_profile.id = '{v}'); "
        github_profile_list = ck.execute_no_params(github_profile_sql)
        if github_profile_list:
            github_profile = github_profile_list[0]
            count = count + 1
            if count == 5:
                break
            for index in range(1, profile_columns_len):
                profile_property = github_profile[index]
                if isinstance(profile_property, str):
                    profile_property = profile_property.replace('\'', '\"')
                profile_value['github__profile__' + profile_columns[index][0]] = profile_property
            value = dict(value, **profile_value)
            values_to_insert.append(value)
    insert_email_address_sql = f"INSERT INTO {CLICKHOUSE_EMAIL_ADDRESS} (*) VALUES"
    ck.execute(insert_email_address_sql, values_to_insert)
    ck.close()


    # todo: 从clickhouse中的gits中根据指定email获取该email参与过的owner、repo
    # for email_address in all_email_address:
    #     gits_owner_repo_sql = f"SELECT DISTINCT search_key__owner,search_key__repo FROM gits WHERE author_email = '{email_address}' OR committer_email = '{email_address}' "
    #     gits_owner_repo = ck.execute_no_params(gits_owner_repo_sql)
    #     print(type(gits_owner_repo))
    #     print(gits_owner_repo)

    # todo: 从clickhouse中的github_commit中根据指定email获取该email对应的github profile对应的id
    # github_id_from_github_commits = set()
    # for email_address in all_email_address:
    #     github_committer_id_from_committer_email_sql = f"SELECT DISTINCT author__id from github_commits WHERE commit__author__email = '{email_address}'"
    #     github_author_id_from_author_email_sql = f"SELECT DISTINCT committer__id from github_commits WHERE commit__committer__email = '{email_address}'"
    #     github_committer_id_from_committer_email = ck.execute_no_params(github_committer_id_from_committer_email_sql)
    #     github_author_id_from_author_email = ck.execute_no_params(github_author_id_from_author_email_sql)
    #     for item in github_committer_id_from_committer_email, github_author_id_from_author_email:
    #         if item and item[0][0]:
    #             github_id_from_github_commits.add(item[0][0])
    # print(github_id_from_githgithub_profile_sqlub_commits)

    # todo: 根据github profile对应的id查找在github issues中的owner、repo信息
    # profile_id = 200109
    # owner_repo_by_profile_id_from_github_issues_sql = f"SELECT DISTINCT search_key__owner,search_key__repo,has(assignees.id,{profile_id}) as has_id from github_issues where user__id={profile_id} OR assignee__id={profile_id} or milestone__creator__id={profile_id} or has_id=1 "
    # owner_repo_by_profile_id_from_github_issues = ck.execute_no_params(owner_repo_by_profile_id_from_github_issues_sql)
    # if owner_repo_by_profile_id_from_github_issues:
    #     for owner_repo in owner_repo_by_profile_id_from_github_issues:
    #         print(owner_repo[0])
    #         print(owner_repo[1])
    #         print(owner_repo[2])
    # print(owner_repo_by_profile_id_from_github_issues)

    # todo: 从clickhouse中获取的email对接晨琪的根据email推断信息
    # for email_address in all_email_address:
    #     if email_address:
    #         country_inferred_from_emailcctld = infer_country_from_emailcctld(email_address)
    #         country_inferred_from_emaildomain = infer_country_from_emaildomain(email_address)
    #         company_inferred_from_email = infer_company_from_emaildomain(email_address)
    #         for item in country_inferred_from_emailcctld, country_inferred_from_emaildomain, company_inferred_from_email:
    #             if item:
    #                 print(item)
    #         if company_inferred_from_email:
    #             country_inferred_from_company = infer_country_from_company(company_inferred_from_email)
    #             if country_inferred_from_company:
    #                 print(country_inferred_from_company)

    # github_profile_sql = "select * from github_profile where id = 3309585"
    # github_profile_email_id_pair = ck.execute_no_params(github_profile_sql)
    # print(type(github_profile_email_id_pair))
    # print(github_profile_email_id_pair)

    # todo: table columns
    # "raw_data": {
    #     "email": "",
    #     "country_inferred_from_emailcctld": "",
    #     "country_inferred_from_emaildomain": "",
    #     "company_inferred_from_email": "",
    #     "country_inferred_from_company": "",
    #     "github": {
    #         "profile": {
    #             "login": "",
    #             "id": 0,
    #             "node_id": "",
    #             "avatar_url": "",
    #             "gravatar_id": "",
    #             "url": "",
    #             "html_url": "",
    #             "followers_url": "",
    #             "following_url": "",
    #             "gists_url": "",
    #             "starred_url": "",
    #             "subscriptions_url": "",
    #             "organizations_url": "",
    #             "repos_url": "",
    #             "events_url": "",
    #             "received_events_url": "",
    #             "type": "",
    #             "site_admin": false,
    #             "name": "",
    #             "company": "",
    #             "blog": "",
    #             "location": "",
    #             "email": "",
    #             "hireable": false,
    #             "bio": "",
    #             "twitter_username": "",
    #             "public_repos": 0,
    #             "public_gists": 0,
    #             "followers": 0,
    #             "following": 0,
    #             "created_at": "",
    #             "updated_at": "",
    #             "country_inferred_from_email_cctld": "",
    #             "country_inferred_from_email_domain_company": "",
    #             "country_inferred_from_location": "",
    #             "country_inferred_from_company": "",
    #             "final_company_inferred_from_company": "",
    #             "company_inferred_from_email_domain_company": "",
    #             "inferred_from_location": {"administrative_area_level_1": "", "administrative_area_level_2": "",
    #                                        "administrative_area_level_3": "", "colloquial_area": "",
    #                                        "continent": "", "country": "", "locality": "", "political": "",
    #                                        "postal_code": "", "postal_code_suffix": "", "postal_town": "",
    #                                        "route": "", "street_number": ""}
    #         }
    #     }}
    ck.close()
