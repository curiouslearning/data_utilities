from google.cloud import bigquery
from google.api_core.retry import Retry
from google.cloud.exceptions import NotFound
from datetime import datetime
from facebook_business.api import FacebookAdsApi
from facebook_business.adobjects.adaccount import AdAccount
from facebook_business.adobjects.adaccountuser import AdAccountUser
from facebook_business.adobjects.adsinsights import AdsInsights
from facebook_business.adobjects.campaign import Campaign
import settings
from retry import retry

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


def get_insights_query_params(bq_client):
    import datetime

    date = get_last_insert_date(bq_client)
    last_run = date.strftime("%Y-%m-%d")
    now = datetime.datetime.now().strftime("%Y-%m-%d")
    daterange = "["
    day = date
    while day <= datetime.datetime.now():
        daystr = day.strftime("%Y-%m-%d")
        nextday = day + datetime.timedelta(days=1)
        nextdaystr = nextday.strftime("%Y-%m-%d")
        daterange += '{"since": "' + daystr + '", "until": "' + nextdaystr + '"},'
        day = nextday

    daterange += "]"

    insights_query_params = {
        "level": "campaign",
        "limit": "1000",
        "time_ranges": daterange,
        "increment": 1,
    }
    return insights_query_params


@retry(backoff=3, tries=6, delay=5)
def get_insights_retry(account, bq_client):
    qp = get_insights_query_params(bq_client)
    print("qp = " + str(qp))
    breakpoint()
    insights = account.get_insights(fields=insights_query_fields, params=qp)
    return insights


@retry(NotFound, delay=5, tries=6)
def insert_rows_json_retry(client, data, table):
    print("trying insert")
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
    sql_query = f"""
        select max(date_inserted)
        FROM `dataexploration-193817.marketing_data.facebook_ads_data`
    """
    query_job = bq_client.query(sql_query)
    rows = query_job.result()
    row = next(rows)
    return row[0]

    return last


def get_facebook_data():
    logger.info("Facebook import function is running. ")

    bigquery_client = bigquery.Client()
    try:
        FacebookAdsApi.init(
            attributes["fb_app_id"],
            attributes["fb_app_secret"],
            attributes["fb_access_token"],
        )

        account = AdAccount("act_" + str(attributes["fb_account_id"]))
        campaigns = account.get_campaigns(
            campaigns_query_fields, campaigns_query_params
        )
        insights = get_insights_retry(account, bigquery_client)

    except Exception as e:
        logger.error(e)
        raise

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
        bq_date_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
        fb_source.append(
            {
                "date_inserted": bq_date_time,
                "data_date_start": item.get("date_start"),
                "campaign_id": item.get("campaign_id"),
                "campaign_name": item.get("campaign_name"),
                "created_time": campaign.get("created_time", ""),
                "start_time": campaign.get("start_time", ""),
                "end_time": campaign.get("end_time", ""),
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
        print("Data = " + str(fb_source))


"""        insert_rows_bigquery(
            bigquery_client,
            attributes["table_id"],
            attributes["dataset_id"],
            attributes["gcp_project_id"],
            fb_source,
        )
"""
