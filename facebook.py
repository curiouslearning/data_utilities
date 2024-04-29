from google.cloud import bigquery
from google.cloud.exceptions import NotFound
from datetime import datetime as dt
import datetime
from datetime import timezone
from facebook_business.api import FacebookAdsApi
from facebook_business.adobjects.adaccount import AdAccount
from facebook_business.adobjects.adaccountuser import AdAccountUser
from facebook_business.adobjects.adsinsights import AdsInsights
from facebook_business.adobjects.campaign import Campaign
import settings
from retry import retry
import ast
from rich import print

logger = settings.init_logging()
attributes = settings.get_secrets()


campaigns_query_fields = [
    Campaign.Field.id,
    Campaign.Field.created_time,
    Campaign.Field.start_time,
    Campaign.Field.stop_time,
    Campaign.Field.status,
    Campaign.Field.objective,
]
campaigns_query_params = {
    "limit": "500",
    "date_preset": "maximum",
}


insights_query_fields = [
    AdsInsights.Field.account_id,
    AdsInsights.Field.campaign_id,
    AdsInsights.Field.campaign_name,
    AdsInsights.Field.spend,
    AdsInsights.Field.impressions,
    AdsInsights.Field.reach,
    AdsInsights.Field.cpc,
    AdsInsights.Field.clicks,
    AdsInsights.Field.actions,
    AdsInsights.Field.conversions,
]


def set_insights_query_params(daterange):
    insights_query_params = {
        "level": "campaign",
        "limit": "1000",
        "time_range": daterange,
        "time_increment": 1,
    }
    return insights_query_params


# create a list of days to query based on the last inserted data
def get_time_ranges(bq_client):
    date = get_last_insert_date(bq_client)

    time_ranges = "["
    day = date

    while day <= datetime.datetime.now(timezone.utc):  # go until today
        daystr = day.strftime("%Y-%m-%d")
        nextday = day + datetime.timedelta(days=1)
        time_ranges += (
            '\'{"since": "' + daystr + '", "until": "' + daystr + "\"}',"
        )  # this makes it go midnight to midnight on the same day
        day = nextday

    time_ranges += "]"

    # time_ranges = '[ \'{"since": "2023-09-05", "until": "2023-12-05"}\']'
    return ast.literal_eval(time_ranges)


@retry(backoff=3, tries=6, delay=5)
def get_insights_retry(account, insights_query_fields, qp):
    insights = account.get_insights(insights_query_fields, qp)
    return insights


@retry(NotFound, delay=5, tries=6)
def insert_rows_json_retry(client, data, table):
    resp = client.insert_rows_json(json_rows=data, table=table)
    return resp


def insert_rows_bigquery(client, table_id, dataset_id, project_id, data):
    table_ref = "{}.{}.{}".format(project_id, dataset_id, table_id)
    table = client.get_table(table_ref)
    resp = None
    while resp is None:
        try:
            resp = insert_rows_json_retry(client, data, table)
            if len(resp) > 0:
                logger.info(str(resp))
            else:
                logger.info("Success uploaded to table {}".format(table.table_id))
        except Exception as e:
            logger.error(e)


def lookup_campaign(campaign_id, campaigns):
    campaign_ret = Campaign()
    for index in range(len(campaigns)):
        campaign = campaigns[index]
        id = campaign.get("id")
        if id == campaign_id:
            campaign_ret = campaign
    return campaign_ret


def get_last_insert_date(bq_client):
    table_name = (
        attributes["gcp_project_id"]
        + "."
        + attributes["dataset_id"]
        + "."
        + attributes["table_id"]
    )
    sql_query = f"""
        select max(date_inserted)
        FROM `{table_name}`
    """

    query_job = bq_client.query(sql_query)
    rows = query_job.result()
    row = next(rows)
    return row[0]


def get_facebook_data():
    logger.info("Facebook import function is running. ")

    bigquery_client = bigquery.Client()
    FacebookAdsApi.init(
        attributes["fb_app_id"],
        attributes["fb_app_secret"],
        attributes["fb_access_token"],
    )
    time_ranges = get_time_ranges(bigquery_client)
    logger.info(time_ranges)

    account = AdAccount("act_" + str(attributes["fb_account_id"]))
    campaigns = account.get_campaigns(campaigns_query_fields, campaigns_query_params)
    rows = 0

    for timerange in time_ranges:
        logger.info("Processing timerange: " + str(timerange))

        qp = set_insights_query_params(timerange)
        insights = get_insights_retry(account, insights_query_fields, qp)

        fb_source = []
        for index, item in enumerate(insights):
            actions = []
            conversions = []

            id = item.get("campaign_id")

            campaign = lookup_campaign(id, campaigns)

            if "actions" in item:
                for i, value in enumerate(item["actions"]):
                    actions.append(
                        {"action_type": value["action_type"], "value": value["value"]}
                    )

            if "conversions" in item:
                for i, value in enumerate(item["conversions"]):
                    conversions.append(
                        {"action_type": value["action_type"], "value": value["value"]}
                    )
            bq_date_time = dt.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f")
            fb_source.append(
                {
                    "date_inserted": bq_date_time,
                    "data_date_start": item.get("date_start"),
                    "campaign_id": item.get("campaign_id"),
                    "campaign_name": item.get("campaign_name"),
                    "created_time": campaign.get("created_time", ""),
                    "start_time": campaign.get("start_time", ""),
                    "end_time": campaign.get("stop_time", ""),
                    "location": "deprecated",
                    "status": campaign.get("status", ""),
                    "objective": campaign.get("objective", ""),
                    "clicks": item.get("clicks"),
                    "impressions": item.get("impressions"),
                    "reach": item.get("reach"),
                    "cpc": item.get("cpc", 0),
                    "spend": item.get("spend"),
                    "conversions": conversions,
                    "actions": actions,
                }
            )

        insert_rows_bigquery(
            bigquery_client,
            attributes["table_id"],
            attributes["dataset_id"],
            attributes["gcp_project_id"],
            fb_source,
        )
        rows = rows + len(fb_source)
    if rows > 0:
        logger.info("Execution complete.  Rows inserted: " + str(rows))
    else:
        logger.warning("Execution complete.  Rows inserted: " + str(rows))
