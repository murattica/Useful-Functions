from airflow.utils.state import State
from airflow.hooks.base import BaseHook
from airflow.contrib.operators.slack_webhook_operator import SlackWebhookOperator
from airflow.providers.slack.hooks.slack_webhook import SlackWebhookHook

import requests

SLACK_CONN_ID = 'slack_connection'
slack_webhook_token = BaseHook.get_connection(SLACK_CONN_ID).password

def task_success_slack_alert(context):
    """
    Callback task that can be used in DAG to alert of successful task completion
    Args:
        context (dict): Context variable passed in from Airflow
    Returns:
        None: Calls the SlackWebhookOperator execute method internally
    """
    slack_webhook_token = BaseHook.get_connection(SLACK_CONN_ID).password
    slack_msg = """
            :large_blue_circle: Task Succeeded! 
            *Task*: {task}  
            *Dag*: {dag} 
            *Execution Time*: {exec_date}  
            *Log Url*: {log_url} 
            """.format(
        task=context.get("task_instance").task_id,
        dag=context.get("task_instance").dag_id,
        ti=context.get("task_instance"),
        exec_date=context.get("execution_date"),
        log_url=context.get("task_instance").log_url,
    )

    success_alert = SlackWebhookOperator(
        task_id="slack_success_notif",
        http_conn_id=SLACK_CONN_ID,
        webhook_token=slack_webhook_token,
        message=slack_msg,
        username="airflow",
    )

    return success_alert.execute(context=context)


def task_fail_slack_alert(context):
    tis_dagrun = context['ti'].get_dagrun().get_task_instances()
    failed_tasks = []
    for ti in tis_dagrun:
        if ti.state == State.FAILED:
            # Adding log url
            failed_tasks.append(f"<{ti.log_url}|{ti.task_id}>")

    dag=context.get('task_instance').dag_id
    exec_date=context.get('execution_date')

    blocks = [
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": ":red_circle: Dag Failed."
            }
        },
        {
            "type": "section",
            "block_id": f"section{uuid.uuid4()}",
            "text": {
                "type": "mrkdwn",
                "text": f"*Dag*: {dag} \n *Execution Time*: {exec_date}"
            },
            "accessory": {
                "type": "image",
                "image_url": "https://raw.githubusercontent.com/apache/airflow/main/airflow/www/static/pin_100.png",
                "alt_text": "Airflow"
            }
        },
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": f"Failed Tasks: {', '.join(failed_tasks)}"
            }
        }
    ]

    failed_alert = SlackWebhookHook(
        http_conn_id = SLACK_CONN_ID,
        webhook_token = slack_webhook_token,
        blocks = blocks,
        username='airflow'
    )

    failed_alert.execute()
    return

def post_message_to_slack(text):
    return requests.post('https://slack.com/api/chat.postMessage', {
        'token': 'xoxb-603265646480-3173018663442-CpnF6FxiJOepOojX9GQvGkDC',
        'channel': '#data-team-errors',
        'text': text,
    }).json()




###################################################################  CREATE VARIABLES  ###################################################################
##  This function generates a game related variable lake which can be utilized as input in any type modelling problem.
## Output variables are created as tables under model_variables schema
## parameters:
## horizon: defines variable windows. default value is 3.
##           e.g.: 7 then variables created for each day starting from day 0 to day 6. (7 excluded)
##
## suffix: table suffix. If user wants to create different sets of variables, user can define suffix for eac set of parameters. e.g.: _7 (for variab set of 7 days horizon )


def create_variables(horizon=3, suffix=''):
    from google.oauth2 import service_account
    from google.cloud import bigquery
    import gspread

    import pandas as pd
    import numpy as np

    key_path = "/home/data/ssh-key.json"

    credentials = service_account.Credentials.from_service_account_file(
        key_path,
        scopes=[
            "https://www.googleapis.com/auth/cloud-platform",
            "https://spreadsheets.google.com/feeds",
            "https://www.googleapis.com/auth/drive.file",
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive"
        ]
    )

    client_sheets = gspread.authorize(credentials)
    client = bigquery.Client(credentials=credentials, project=credentials.project_id)

    def session_group_variables():
        session_group_query = r"""
        create or replace table model_variables.session_group_variables""" + suffix + """ as
        select 
            t1.user_id,"""

        for i in range(0, horizon):
            session_group_query = session_group_query + '\n' + """\tcoalesce(count(case when nth_day_calendar = """ + str(
                i) + """ then duration_min end),0) as sess_cnt_d""" + str(i) + ""","""

        session_group_query = session_group_query + '\n' + """\tcoalesce(count(case when nth_day_calendar < """ + str(
            horizon) + """ then duration_min end),0) as sess_cnt_d0""" + str(horizon - 1) + ""","""

        for i in range(0, horizon):
            session_group_query = session_group_query + '\n' + """\tcoalesce(max(case when nth_day_calendar = """ + str(
                i) + """ then duration_min end),0) as max_sess_len_d""" + str(i) + ""","""

        session_group_query = session_group_query + '\n' + """\tcoalesce(max(case when nth_day_calendar < """ + str(
            horizon) + """ then duration_min end),0) as max_sess_len_d0""" + str(horizon - 1) + ""","""

        for i in range(0, horizon):
            session_group_query = session_group_query + '\n' + """\tcoalesce(avg(case when nth_day_calendar = """ + str(
                i) + """ then duration_min end),0) as avg_sess_len_d""" + str(i) + ""","""

        session_group_query = session_group_query + '\n' + """\tcoalesce(avg(case when nth_day_calendar < """ + str(
            horizon) + """ then duration_min end),0) as avg_sess_len_d0""" + str(horizon - 1) + ""","""

        for i in range(0, horizon):
            session_group_query = session_group_query + '\n' + """\tcoalesce(sum(case when nth_day_calendar = """ + str(
                i) + """ then duration_min end),0) as tot_sess_len_d""" + str(i) + ""","""

        session_group_query = session_group_query + '\n' + """\tcoalesce(sum(case when nth_day_calendar < """ + str(
            horizon) + """ then duration_min end),0) as tot_sess_len_d0""" + str(horizon - 1) + ""","""

        for i in range(0, horizon):
            session_group_query = session_group_query + '\n' + """\tsafe_divide(coalesce(sum(case when nth_day_calendar = """ + str(
                i) + """ then duration_min end),0),coalesce(sum(case when nth_day_calendar <""" + str(
                horizon) + """  then duration_min end),0) ) as rat_sess_len_d""" + str(i) + "_0" + str(
                horizon - 1) + ""","""

        for i in range(0, horizon):
            session_group_query = session_group_query + '\n' + """\tsafe_divide(coalesce(count(case when nth_day_calendar = """ + str(
                i) + """ then duration_min end),0),coalesce(count(case when nth_day_calendar <""" + str(
                horizon) + """  then duration_min end),0) ) as rat_sess_cnt_d""" + str(i) + "_0" + str(
                horizon - 1) + ""","""

        session_group_query = session_group_query + '\n' + """\tsafe_divide(coalesce(count(case when nth_day_calendar = 0 then duration_min end),0),coalesce(count(case when nth_day_calendar >0 and nth_day_calendar < """ + str(
            horizon) + """ then duration_min end),0) ) as rat_sess_cnt_d0_d1""" + str(horizon - 1) + ""","""

        session_group_query = session_group_query + '\n' + """\tsafe_divide(coalesce(count(case when nth_day_calendar =""" + str(
            horizon - 1) + """ then duration_min end),0),coalesce(count(case when nth_day_calendar <""" + str(
            horizon - 1) + """ then duration_min end),0) ) as rat_sess_cnt_d""" + str(horizon - 1) + """_d0""" + str(
            horizon - 2) + ""","""

        session_group_query = session_group_query + '\n' + """\tmax(case when session_start = first_sess then lvl_progress end) as first_sess_dur,
            max(case when session_start = first_sess then lvl_progress end) as first_sess_puzzle_prog,"""

        for i in range(0, horizon):
            session_group_query = session_group_query + '\n' + """\tcoalesce(sum(case when nth_day_calendar = """ + str(
                i) + """ then lvl_progress end),0) as puz_succed_d""" + str(i) + """ ,"""

        for i in range(0, horizon):
            session_group_query = session_group_query + '\n' + """\tcoalesce(sum(case when nth_day_calendar = """ + str(
                i) + """ then puzzle_dur_min end),0) as puz_dur_d""" + str(i) + """ ,"""

        for i in range(0, horizon):
            session_group_query = session_group_query + '\n' + """\tsafe_divide(coalesce(sum(case when nth_day_calendar = """ + str(
                i) + """ then puzzle_dur_min end),0),coalesce(sum(case when nth_day_calendar = """ + str(
                i) + """ then duration_min end),0) ) as rat_puz_sess_d""" + str(i) + ""","""

        session_group_query = session_group_query + '\n' + """from `fiona-s-farm.Looker_db.user_sessions` t1
        left join (select user_id,min(session_start) as first_sess from `fiona-s-farm.Looker_db.user_sessions` group by 1) t2 on t1.user_id = t2.user_id
        where duration_min < 350
        group by 1"""

        results = client.query(session_group_query).result()

    def tutorial_variables():
        tutorial_query = """
        create or replace table model_variables.tutorial_variables""" + suffix + """ as
        select 
            user_id, 
            user_first_touch_timestamp,
            max(flag_tut_finihed_first_ses) as flag_tut_finihed_first_ses,
            max(flag_tut_finihed_d0)        as flag_tut_finihed_d0,
            max(flag_tut_finihed_d0""" + str(horizon - 1) + """)       as flag_tut_finihed_d0""" + str(horizon - 1) + """
        from
        (    
            select distinct
                user_id, 
                user_first_touch_timestamp, 
                case when first_value((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') ignore nulls) over(partition by user_id,user_first_touch_timestamp order by event_ts ) =
                    first_value(case when event_name = 'tutorial_finish' then (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') end ignore nulls) over(partition by user_id,user_first_touch_timestamp order by event_ts )
                then 1 else 0 end as flag_tut_finihed_first_ses,
                case when timestamp_diff(timestamp_trunc(min(case when event_name = 'tutorial_finish' then event_ts end) over(partition by user_id,user_first_touch_timestamp order by event_ts) ,DAY), timestamp_trunc(user_first_touch_timestamp,DAY), DAY) = 0 then 1 else 0 end as flag_tut_finihed_d0,
                case when timestamp_diff(timestamp_trunc(min(case when event_name = 'tutorial_finish' then event_ts end) over(partition by user_id,user_first_touch_timestamp order by event_ts) ,DAY), timestamp_trunc(user_first_touch_timestamp,DAY), DAY) < """ + str(
            horizon) + """ then 1 else 0 end as flag_tut_finihed_d0""" + str(horizon - 1) + """ ,
            from `fiona-s-farm.analytics_278577021.raw_partitioned` 
        )
        group by 
            1,2"""

        results = client.query(tutorial_query).result()

    def coin_variables():

        source_list = ['PuzzleLifePopup', 'PuzzleContinue', 'FactoryBuyMissing', 'RuinBuyMissing', 'FertilizerBuy',
                       'TicketBuy', 'BoosterBuy', 'Purchase', 'SpeedUp Order', 'MarketBuy', 'RuinBuild',
                       'FactoryBuySlot', 'PuzzleWinCoin', 'PowerUpCollect']

        coin_query = r"""
        create or replace table model_variables.coin_variables""" + suffix + """ as
        select 
            user_id,"""

        for i in range(0, len(source_list)):
            for j in range(0, horizon):
                coin_query = coin_query + '\n' + """\tcoalesce(sum(case when source =  '""" + source_list[i].replace(
                    " ", "") + """' and nth_day_calendar = """ + str(
                    j) + """ then abs(delta) end ),0) as coin_spend_""" + source_list[i].replace(" ",
                                                                                                 "") + """_d""" + str(
                    j) + ""","""

            coin_query = coin_query + '\n' + """\tcoalesce(sum(case when source =  '""" + source_list[i].replace(" ",
                                                                                                                 "") + """' and nth_day_calendar < """ + str(
                horizon) + """ then abs(delta) end ),0) as coin_spend_""" + source_list[i].replace(" ",
                                                                                                   "") + """_d0""" + str(
                horizon - 1) + ""","""

        coin_query = coin_query + '\n' + """from 
        (
            select 
                t.*,
                timestamp_diff(timestamp_trunc(event_ts,DAY), timestamp_trunc(install_time,DAY), DAY) nth_day_calendar
            from `fiona-s-farm.Looker_db.coin_transactions` t
        )
        group by 1"""

        results = client.query(coin_query).result()

    def build_variables():
        var_list_query = """
        select distinct  
            (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'building_type')  as building,
            (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'action') as action,
           --- (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'prefab_id') prefab_id,


        from `fiona-s-farm.analytics_278577021.raw_partitioned`

          where event_name = 'build' 
          and (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'building_type') in  ('Ruin','Farm','Factory')"""

        var_list = client.query(var_list_query)
        var_list = var_list.result().to_dataframe()

        build_query = r"""
        create or replace table model_variables.build_variables""" + suffix + """ as
        select distinct
            user_id,"""

        for i in range(0, len(var_list)):

            for j in range(0, horizon):
                build_query = build_query + '\n' + """\tsum(case when (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'building_type')  = '""" + str(
                    var_list.loc[i, 'building']) + """' 
                           and (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'action') = '""" + str(
                    var_list.loc[i, 'action']) + """' 
                           and nth_day_calendar = """ + str(j) + """  
                    then 1 else 0 end) as cnt_""" + str(var_list.loc[i, 'building']) + "_" + str(
                    var_list.loc[i, 'action']) + "_d" + str(j) + """,""" + """
                """
            build_query = build_query + '\n' + """\tsum(case when (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'building_type')  = '""" + str(
                var_list.loc[i, 'building']) + """' 
                       and (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'action') = '""" + str(
                var_list.loc[i, 'action']) + """' 
                       and nth_day_calendar < """ + str(horizon) + """  
                then 1 else 0 end) as cnt_""" + str(var_list.loc[i, 'building']) + "_" + str(
                var_list.loc[i, 'action']) + "_d0" + str(horizon - 1) + """,""" + """
            """

        build_query = build_query + '\n' + """from         (
            select 
                t.*,
                timestamp_diff(timestamp_trunc(event_ts,DAY), timestamp_trunc(user_first_touch_timestamp,DAY), DAY) nth_day_calendar
            from `fiona-s-farm.analytics_278577021.raw_partitioned` t
            where timestamp_diff(timestamp_trunc(event_ts,DAY), timestamp_trunc(user_first_touch_timestamp,DAY), DAY)  <""" + str(
            horizon) + """
            and  event_name = 'build' 
        )
        group by 1"""

        results = client.query(build_query).result()

    def retention_variables():
        retention_horizons = [1, 3, 7, 14, 30, 60, 90]
        active_horizons = [x for x in retention_horizons if x <= horizon]

        ret_query = r"""
        create or replace table model_variables.retention_variables""" + suffix + """ as
        select distinct
            user_id,"""

        for i in range(0, len(active_horizons)):
            ret_query = ret_query + '\n' + """\tmax(case when date_add(date(user_first_touch_timestamp), interval  """ + str(
                active_horizons[
                    i]) + """  day) = date(event_ts) then 1 else 0 end) over(partition by user_id) is_d""" + str(
                active_horizons[i]) + """ ,"""

        ret_query = ret_query + """    
        from `fiona-s-farm.analytics_278577021.raw_partitioned`
        where event_name = 'session_start' """

        results = client.query(ret_query).result()

    def loc_change_variables():
        loc_change_query = r"""
        create or replace table model_variables.loc_change_variables""" + suffix + """ as
        select 
            user_id,"""

        for i in range(0, horizon):
            loc_change_query = loc_change_query + '\n' + """\tcount(distinct case when nth_day_calendar =  """ + str(
                i) + """ then too end ) as cnt_dist_loc_d""" + str(i) + """ ,"""

        loc_change_query = loc_change_query + """
        \tmax(flag_loc_change_d0) as flag_loc_change_d0,
        \tmax(flag_loc_change_d0""" + str(horizon - 1) + """) as flag_loc_change_d0""" + str(horizon - 1)

        loc_change_query = loc_change_query + """
        from 
        (
            select distinct
                user_id,
                max(case when timestamp_diff(timestamp_trunc(event_ts,DAY), timestamp_trunc(user_first_touch_timestamp,DAY), DAY) = 0 
                    and (SELECT value.string_value FROM UNNEST(event_params) WHERE key ='to') = 'Adventure 1' then 1 else 0 end) over(partition by user_id order by event_ts rows between unbounded preceding and current row) as flag_loc_change_d0,
                max(case when timestamp_diff(timestamp_trunc(event_ts,DAY), timestamp_trunc(user_first_touch_timestamp,DAY), DAY) <""" + str(
            horizon) + """
           and (SELECT value.string_value FROM UNNEST(event_params) WHERE key ='to') = 'Adventure 1' then 1 else 0 end) over(partition by user_id order by event_ts rows between unbounded preceding and current row) as flag_loc_change_d0""" + str(
            horizon - 1) + """,
                (SELECT value.string_value FROM UNNEST(event_params) WHERE key ='from') fromm,
                (SELECT value.string_value FROM UNNEST(event_params) WHERE key ='to') too,
                timestamp_diff(timestamp_trunc(event_ts,DAY), timestamp_trunc(user_first_touch_timestamp,DAY), DAY) nth_day_calendar
            from `fiona-s-farm.analytics_278577021.raw_partitioned` t
            where timestamp_diff(timestamp_trunc(event_ts,DAY), timestamp_trunc(user_first_touch_timestamp,DAY), DAY)  <""" + str(
            horizon) + """
            and (SELECT value.string_value FROM UNNEST(event_params) WHERE key ='from')  != 'Puzzle'
        )
        group by 1"""

        results = client.query(loc_change_query).result()

    def production_variables():
        sheet = client_sheets.open_by_url(
            'https://docs.google.com/spreadsheets/d/1bGg0bsxPxSIWDePLL1eMRn_zVxWNAcJeSIaGmhL6EyM/edit#gid=852288677')
        worksheet = sheet.worksheet("Recipes")

        list_of_items = worksheet.get_all_values()
        list_of_items = pd.DataFrame(data=list_of_items[1:],
                                     columns=[col.replace(" ", "_") for col in list_of_items[0]])
        list_of_items = list_of_items[list_of_items['Duration'].astype('bool')].reset_index().loc[:,
                        ['Name', 'Duration']]

        production_query = r"""
        create or replace table model_variables.production_variables""" + suffix + """ as
        select 
            user_id,
            sum(case when action = 'collect' then duration end) as cnt_harvest_dur_d0""" + str(horizon - 1) + """,
            sum(case when action = 'collect' then 1 else 0 end) as cnt_collect,
            sum(case when action = 'speed_up' then 1 else 0 end) as cnt_speedup,
            safe_divide(sum(case when nth_day_calendar = """ + str(
            horizon - 1) + """ and action = 'collect' then duration end) , sum(case when nth_day_calendar = 0 and action = 'collect' then duration end)) as rat_harv_d""" + str(
            horizon - 1) + """_d0,
            safe_divide(sum(case when nth_day_calendar = """ + str(
            horizon - 1) + """ and action = 'collect' then duration end) , sum(case when nth_day_calendar != """ + str(
            horizon - 1) + """ and nth_day_calendar < """ + str(
            horizon) + """ and action = 'collect' then duration end)) rat_harv_d""" + str(
            horizon - 1) + """_d0""" + str(horizon - 1) + ""","""

        for i in range(0, len(list_of_items)):
            for j in range(0, horizon):
                production_query = production_query + '\n' + """\tsum(case when nth_day_calendar =""" + str(
                    j) + """ and action = 'collect' and item_name = '""" + str(
                    list_of_items.loc[i, 'Name']) + """' then 1 else 0 end) as cnt_""" + str(
                    list_of_items.loc[i, 'Name']).replace(" ", "") + """_collect_d""" + str(j) + ""","""

        for i in range(0, horizon):
            production_query = production_query + '\n' + """\tcoalesce(sum(case when nth_day_calendar =""" + str(
                i) + """  and action = 'collect' then duration end),0) as harvest_dur_d""" + str(i) + ""","""

        production_query = production_query + '\n' + """from
        (
            select 
                user_id, 
                event_ts,
                user_first_touch_timestamp,
                (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'action') as action,
                (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'item_name') as item_name,
                case """

        for i in range(0, len(list_of_items)):
            production_query = production_query + """when (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'item_name') =  '""" + str(
                list_of_items.loc[i, 'Name']) + """' then """ + str(list_of_items.loc[i, 'Duration']) + '\n' + '\t\t\t'

        production_query = production_query + '\n' + """        end as duration,
                timestamp_diff(timestamp_trunc(event_ts,DAY), timestamp_trunc(user_first_touch_timestamp,DAY), DAY)  as nth_day_calendar
            from `fiona-s-farm.analytics_278577021.raw_partitioned` 
            where timestamp_diff(timestamp_trunc(event_ts,DAY), timestamp_trunc(user_first_touch_timestamp,DAY), DAY)  <""" + str(
            horizon) + """
            and  event_name = 'production' 
        )
        group by 1"""

        results = client.query(production_query).result()

    def energy_spend_variables():
        energy_query = r"""
        create or replace table model_variables.energy_variables""" + suffix + """ as
        select 
            user_id,"""

        for i in range(0, horizon):
            energy_query = energy_query + '\n' + """\tcoalesce(sum(case when nth_day_calendar = """ + str(
                i) + """  then delta end),0) as tot_energy_spend_d""" + str(i) + ""","""

        energy_query = energy_query + '\n' + """\tcoalesce(sum(case when nth_day_calendar < """ + str(
            horizon) + """  then delta end),0) as tot_energy_spend_d0""" + str(horizon - 1) + ""","""

        energy_query = energy_query + """
        from
        (
            select 
                user_id, 
                event_ts,
                user_first_touch_timestamp,
                (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'action') as action,
                (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'map_level') as map_level,
                abs((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'delta')) as delta,
                (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'source') as source,
                (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'currency_name') as currency_name,
                timestamp_diff(timestamp_trunc(event_ts,DAY), timestamp_trunc(user_first_touch_timestamp,DAY), DAY)  as nth_day_calendar
            from `fiona-s-farm.analytics_278577021.raw_partitioned` 
            where timestamp_diff(timestamp_trunc(event_ts,DAY), timestamp_trunc(user_first_touch_timestamp,DAY), DAY)  <""" + str(
            horizon) + """
            and  event_name = 'currency' 
            and (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'action') = 'dec'
            and (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'currency_name') = 'Energy'
            and (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'source') = 'Gatherable Work'
        )
        group by 1"""

        results = client.query(energy_query).result()

    def quest_variables():
        quest_query = r"""
        create or replace table model_variables.quest_variables""" + suffix + """ as
        select 
            user_id,"""

        for i in range(0, horizon):
            quest_query = quest_query + '\n' + """\tcoalesce(sum(case when nth_day_calendar = """ + str(
                i) + """ then 1 else 0 end),0) as cnt_quest_fin_d""" + str(i) + ""","""

        quest_query = quest_query + '\n' + """\tcoalesce(sum(case when nth_day_calendar < """ + str(
            horizon) + """ then 1 else 0 end),0) as cnt_quest_fin_d0""" + str(horizon - 1) + ""","""

        quest_query = quest_query + """
        from
        (
            select 
                user_id, 
                event_ts,
                user_first_touch_timestamp, event_name,
                (SELECT value.string_value FROM UNNEST(user_properties) WHERE key = 'SeenQuest') as SeenQuest,
                (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'action') as action,
                timestamp_diff(timestamp_trunc(event_ts,DAY), timestamp_trunc(user_first_touch_timestamp,DAY), DAY)  as nth_day_calendar
            from `fiona-s-farm.analytics_278577021.raw_partitioned` 
            where timestamp_diff(timestamp_trunc(event_ts,DAY), timestamp_trunc(user_first_touch_timestamp,DAY), DAY)  <""" + str(
            horizon) + """
            and event_name = 'quest'
            and (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'action')  = 'finish'
        )
        group by 1"""

        results = client.query(quest_query).result()

    def order_variables():
        order_query = r"""
        create or replace table model_variables.order_variables""" + suffix + """ as
        select 
            t1.user_id,
            count(distinct case when action = 'speed_up' then event_ts end) as cnt_speed_up,
            count(distinct case when action = 'order_delivered' and reason = 'AllLoaded' then event_ts end) as cnt_all_loaded,
            count(distinct case when action = 'order_delivered' and reason = 'UserDeliver' and loaded_count = 0 then event_Ts end)  cnt_order_deliver_lc0,
            count(distinct case when action = 'order_delivered' and reason = 'UserDeliver' and loaded_count > 0 and loaded_count < 4  then  event_Ts end)  cnt_order_deliver_lc13,
            count(distinct case when action = 'order_delivered' and reason = 'UserDeliver' and loaded_count > 3 and loaded_count < 7  then  event_Ts end)  cnt_order_deliver_lc46,
            count(distinct case when action = 'order_delivered' and reason = 'UserDeliver' and loaded_count > 6 and loaded_count < 10 then  event_Ts end)  cnt_order_deliver_lc79,"""

        for i in range(0, horizon):
            order_query = order_query + '\n' + "\tcount(distinct case when timestamp_diff(timestamp_trunc(t1.event_ts,DAY), timestamp_trunc(t2.install_time,DAY), DAY)  = """ + str(
                i) + """  and action= 'load' then event_ts end) as cnt_loaded_d""" + str(i) + ""","""

        order_query = order_query + '\n' + "\tcount(distinct case when timestamp_diff(timestamp_trunc(t1.event_ts,DAY), timestamp_trunc(t2.install_time,DAY), DAY)  < """ + str(
            horizon) + """  and action= 'load' then event_ts end) as cnt_loaded_d0""" + str(horizon - 1) + """
        from `fiona-s-farm.Looker_db.order_trx` t1
        left join `fiona-s-farm.Looker_db.user_summary` t2 on t1.user_id = t2.user_id
        where timestamp_diff(timestamp_trunc(t1.event_ts,DAY), timestamp_trunc(t2.install_time,DAY), DAY)  <3
        group by 1"""

        results = client.query(order_query).result()

    def puzzle_variables():
        puzzle_query = r"""
        create or replace table model_variables.puzzle_variables""" + suffix + """ as
        select 
            user_id,
            sum(case when resultt = 'kill' then 1 else 0 end ) as cnt_puzzle_kill,
            sum(case when resultt = 'quit' then 1 else 0 end ) as cnt_puzzle_quit,"""

        for i in range(0, horizon):
            puzzle_query = puzzle_query + '\n' + """\tcoalesce(max(case when nth_day_calendar = """ + str(
                i) + """ then level end ),0) as max_lvl_d""" + str(i) + ""","""

        puzzle_query = puzzle_query + '\n' + """\tcoalesce(max(case when nth_day_calendar < """ + str(
            horizon) + """ then level end ),0) as max_lvl_d0""" + str(horizon - 1) + ""","""

        for i in range(0, horizon):
            puzzle_query = puzzle_query + '\n' + """\tcoalesce(sum(case when nth_day_calendar = """ + str(
                i) + """ then 1 else 0 end),0) as lvl_tries_d""" + str(i) + ""","""

        puzzle_query = puzzle_query + '\n' + """\tcoalesce(sum(case when nth_day_calendar < """ + str(
            horizon) + """ then 1 else 0 end),0) as lvl_tries_d0""" + str(horizon - 1) + ""","""

        for i in range(0, horizon):
            puzzle_query = puzzle_query + '\n' + """\tcoalesce(sum(case when nth_day_calendar = """ + str(
                i) + """ then continue_count end),0) as cnt_continueCount_d""" + str(i) + ""","""

        puzzle_query = puzzle_query + '\n' + """\tcoalesce(sum(case when nth_day_calendar < """ + str(
            horizon) + """ then continue_count end),0) as cnt_continueCount_d0""" + str(horizon - 1) + ""","""

        for i in range(0, horizon):
            puzzle_query = puzzle_query + '\n' + """\tcoalesce(sum(case when nth_day_calendar = """ + str(
                i) + """ then rocket end),0) as cnt_rocket_d""" + str(i) + ""","""

        puzzle_query = puzzle_query + '\n' + """\tcoalesce(sum(case when nth_day_calendar < """ + str(
            horizon) + """ then rocket end),0) as cnt_rocket_d0""" + str(horizon - 1) + ""","""

        for i in range(0, horizon):
            puzzle_query = puzzle_query + '\n' + """\tcoalesce(sum(case when nth_day_calendar = """ + str(
                i) + """ then bomb end),0) as cnt_bomb_d""" + str(i) + ""","""

        puzzle_query = puzzle_query + '\n' + """\tcoalesce(sum(case when nth_day_calendar < """ + str(
            horizon) + """ then bomb end),0) as cnt_bomb_d0""" + str(horizon - 1) + ""","""

        for i in range(0, horizon):
            puzzle_query = puzzle_query + '\n' + """\tcoalesce(sum(case when nth_day_calendar = """ + str(
                i) + """ then rainbow end),0) as cnt_rainbow_d""" + str(i) + ""","""

        puzzle_query = puzzle_query + '\n' + """\tcoalesce(sum(case when nth_day_calendar < """ + str(
            horizon) + """ then rainbow end),0) as cnt_rainbow_d0""" + str(horizon - 1) + ""","""

        for i in range(0, horizon):
            puzzle_query = puzzle_query + '\n' + """\tcoalesce(sum(case when nth_day_calendar = """ + str(
                i) + """ then shovel end),0) as cnt_shovel_d""" + str(i) + ""","""

        puzzle_query = puzzle_query + '\n' + """\tcoalesce(sum(case when nth_day_calendar < """ + str(
            horizon) + """ then shovel end),0) as cnt_shovel_d0""" + str(horizon - 1) + ""","""

        for i in range(0, horizon):
            puzzle_query = puzzle_query + '\n' + """\tcoalesce(sum(case when nth_day_calendar = """ + str(
                i) + """ then wheelbarrow end),0) as cnt_wheelbarrow_d""" + str(i) + ""","""

        puzzle_query = puzzle_query + '\n' + """\tcoalesce(sum(case when nth_day_calendar < """ + str(
            horizon) + """ then wheelbarrow end),0) as cnt_wheelbarrow_d0""" + str(horizon - 1) + ""","""

        for i in range(0, horizon):
            puzzle_query = puzzle_query + '\n' + """\tcoalesce(sum(case when nth_day_calendar = """ + str(
                i) + """ then rake end),0) as cnt_rake_d""" + str(i) + ""","""

        puzzle_query = puzzle_query + '\n' + """\tcoalesce(sum(case when nth_day_calendar < """ + str(
            horizon) + """ then rake end),0) as cnt_rake_d0""" + str(horizon - 1) + ""","""

        puzzle_query = puzzle_query + """
        from
        (
            select 
                t.*,
                timestamp_diff(timestamp_trunc(puzzle_start_ts,DAY), timestamp_trunc(install_time,DAY), DAY)  as nth_day_calendar
            from Looker_db.puzzle_sessions t
            where timestamp_diff(timestamp_trunc(puzzle_start_ts,DAY), timestamp_trunc(install_time,DAY), DAY) <""" + str(
            horizon) + """
        )
        group by 1"""

        results = client.query(puzzle_query).result()

    puzzle_variables()
    order_variables()
    session_group_variables()
    tutorial_variables()
    coin_variables()
    build_variables()
    retention_variables()
    loc_change_variables()
    production_variables()
    energy_spend_variables()
    quest_variables()
