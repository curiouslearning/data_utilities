from google.cloud import bigquery
from google.api_core.retry import Retry
from google.cloud.exceptions import NotFound
from datetime import datetime
import time
from facebook_business.api import FacebookAdsApi
from facebook_business.adobjects.adaccount import AdAccount
from facebook_business.adobjects.adaccountuser import AdAccountUser
from facebook_business.adobjects.adsinsights import AdsInsights
from facebook_business.adobjects.campaign import Campaign
import settings
from retry import retry
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
    AdsInsights.Field.location,
]


time_ranges = [
    '{"since": "2023-11-05", "until": "2023-12-04"}',
    '{"since": "2023-10-04", "until": "2023-11-04"}',
    '{"since": "2023-08-04", "until": "2023-10-04"}',
    '{"since": "2023-06-04", "until": "2023-08-04"}',
    '{"since": "2023-03-04", "until": "2023-06-04"}',
    '{"since": "2023-01-04", "until": "2023-03-04"}',
    '{"since": "2022-10-04", "until": "2023-01-04"}',
    '{"since": "2022-07-04", "until": "2022-10-04"}',
    '{"since": "2022-04-04", "until": "2022-07-04"}',
    '{"since": "2022-01-04", "until": "2022-04-04"}',
    '{"since": "2021-10-04", "until": "2022-01-04"}',
    '{"since": "2021-07-04", "until": "2021-10-04"}',
    '{"since": "2021-04-04", "until": "2021-07-04"}',
    '{"since": "2021-02-14", "until": "2021-04-04"}',
]


@retry(NotFound, delay=10, tries=6)
def insert_rows_json_retry(client, data, table):
    print("trying insert")
    resp = client.insert_rows_json(json_rows=data, table=table)
    return resp


@retry(backoff=3, tries=6, delay=5)
def get_insights_retry(account, insights_query_fields, qp):
    print("Insights query")
    insights = account.get_insights(insights_query_fields, qp)
    return insights


def set_insights_query_params(daterange):
    insights_query_params = {
        "level": "campaign",
        "limit": "1000",
        "time_range": daterange,
        "time_increment": 1,
    }
    return insights_query_params


clustering_fields_facebook = ["campaign_id", "campaign_name"]


@retry(delay=1, tries=3)
def truncate_table(bq_client):
    table_ref = "{}.{}.{}".format(
        attributes["gcp_project_id"], attributes["dataset_id"], attributes["table_id"]
    )
    print("Truncating " + str(table_ref))
    query_job = bq_client.query(f"TRUNCATE table {table_ref}")


def insert_rows_bigquery(client, table_id, dataset_id, project_id, data):
    table_ref = "{}.{}.{}".format(project_id, dataset_id, table_id)

    table = client.get_table(table_ref)

    resp = None
    if len(data) > 0:
        while resp is None:
            resp = insert_rows_json_retry(client, data, table)
            if len(resp) > 0:
                logger.error(str(resp))
                print("ERROR:  Didn't insert for timeframe")
                print(str(resp))
                break
            else:
                logger.info("Success uploaded to table {}".format(table.table_id))
    else:
        logger.info("No rows to insert")
        print("No rows")


def lookup_campaign(campaign_id, campaigns):
    campaign_ret = Campaign()
    for index in range(len(campaigns)):
        campaign = campaigns[index]
        id = campaign.get("id")
        if id == campaign_id:
            campaign_ret = campaign
    return campaign_ret


def get_facebook_data():
    logger.info("Facebook import function is running. ")

    bigquery_client = bigquery.Client()

    FacebookAdsApi.init(
        attributes["fb_app_id"],
        attributes["fb_app_secret"],
        attributes["fb_access_token"],
    )

    account = AdAccount("act_" + str(attributes["fb_account_id"]))
    campaigns = account.get_campaigns(campaigns_query_fields, campaigns_query_params)
    truncate_table(bigquery_client)

    for timerange in time_ranges:
        print("Processing timerange " + str(timerange))
        qp = set_insights_query_params(timerange)
        insights = get_insights_retry(account, insights_query_fields, qp)

        fb_source = []
        temp = []

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
                    "end_time": campaign.get("stop_time", ""),
                    "status": campaign.get("status", ""),
                    "objective": campaign.get("objective", ""),
                    "clicks": item.get("clicks"),
                    "impressions": item.get("impressions"),
                    "reach": item.get("reach"),
                    "cpc": item.get("cpc", 0),
                    "spend": item.get("spend"),
                    "location": item.get("location", ""),
                    "conversions": conversions,
                    "actions": actions,
                }
            )

        print("Inserting rows: " + str(len(fb_source)))
        insert_rows_bigquery(
            bigquery_client,
            attributes["table_id"],
            attributes["dataset_id"],
            attributes["gcp_project_id"],
            fb_source,
        )

    logger.info("Execution complete")
