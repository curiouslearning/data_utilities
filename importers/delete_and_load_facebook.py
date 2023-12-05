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
    AdsInsights.Field.date_start,
    AdsInsights.Field.date_stop,
    AdsInsights.Field.location,
]


@retry(NotFound, delay=5, tries=6)
def insert_rows_json_retry(client, data, table):
    print("trying insert")
    resp = client.insert_rows_json(json_rows=data, table=table)
    return resp


@retry(backoff=3, tries=6, delay=5)
def get_insights_retry(account, insights_query_fields, qp):
    insights = account.get_insights(insights_query_fields, qp)
    return insights


@retry(NotFound, delay=5, tries=6)
def check_table_existence(client, table_id):
    print("checking for table: " + table_id)
    try:
        client.get_table(table_id)  # Make an API request.
        print("Table {} already exists.".format(table_id))
    except NotFound:
        print("Table Not found")
        raise


def set_insights_query_params(daterange):
    insights_query_params = {
        "level": "campaign",
        "limit": "1000",
        "time_range": daterange,
        "time_increment": 1,
    }
    return insights_query_params


time_ranges = [
    #    '{"since": "2023-11-20", "until": "2023-12-04"}',
    #    '{"since": "2023-08-15", "until": "2023-11-20"}',
    #   '{"since": "2023-05-15", "until": "2023-08-15"}',
    #   '{"since": "2023-02-15", "until": "2023-05-15"}',
    #   '{"since": "2022-11-15", "until": "2023-02-15"}',
    #   '{"since": "2022-08-15", "until": "2022-11-15"}',
    #   '{"since": "2022-05-15", "until": "2022-08-15"}',
    #   '{"since": "2022-02-15", "until": "2022-05-15"}',
    #   '{"since": "2021-11-15", "until": "2022-02-15"}',
    #   '{"since": "2021-08-15", "until": "2021-11-15"}',
    #   '{"since": "2021-05-15", "until": "2021-08-15"}',
    #   '{"since": "2021-02-15", "until": "2021-05-15"}',
    #   '{"since": "2020-11-15", "until": "2021-02-15"}',
    '{"since": "2020-09-05", "until": "2020-11-15"}',
    #   '{"since": "2020-05-15", "until": "2020-08-15"}',
    #   '{"since": "2020-02-15", "until": "2020-05-15"}',
    #   '{"since": "2020-01-01", "until": "2020-02-15"}',
]


schema_facebook_stat = [
    bigquery.SchemaField("date_inserted", "DATETIME", mode="REQUIRED"),
    bigquery.SchemaField("data_date_start", "DATETIME", mode="REQUIRED"),
    bigquery.SchemaField("campaign_id", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("campaign_name", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("created_time", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("start_time", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("end_time", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("status", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("objective", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("clicks", "INTEGER", mode="REQUIRED"),
    bigquery.SchemaField("impressions", "INTEGER", mode="REQUIRED"),
    bigquery.SchemaField("reach", "INTEGER", mode="REQUIRED"),
    bigquery.SchemaField("cpc", "FLOAT", mode="REQUIRED"),
    bigquery.SchemaField("spend", "FLOAT", mode="REQUIRED"),
    bigquery.SchemaField("location", "STRING", mode="REQUIRED"),
    bigquery.SchemaField(
        "conversions",
        "RECORD",
        mode="REPEATED",
        fields=(
            bigquery.SchemaField("action_type", "STRING"),
            bigquery.SchemaField("value", "STRING"),
        ),
    ),
    bigquery.SchemaField(
        "actions",
        "RECORD",
        mode="REPEATED",
        fields=(
            bigquery.SchemaField("action_type", "STRING"),
            bigquery.SchemaField("value", "STRING"),
        ),
    ),
]

clustering_fields_facebook = ["campaign_id", "campaign_name"]


def setup_bigquery_table(
    first_run, client, table_id, dataset_id, project_id, schema, clustering_fields=None
):
    try:
        dataset_ref = "{}.{}".format(project_id, dataset_id)
        client.get_dataset(dataset_ref)  # Make an API request.

    except NotFound:
        dataset_ref = "{}.{}".format(project_id, dataset_id)
        dataset = bigquery.Dataset(dataset_ref)
        dataset.location = "US"
        dataset = client.create_dataset(dataset)  # Make an API request.
        logger.info("Created dataset {}.{}".format(client.project, dataset.dataset_id))

    try:
        table_ref = "{}.{}.{}".format(project_id, dataset_id, table_id)
        table = client.get_table(table_ref)  # Make an API request.

        if first_run:
            try:
                client.delete_table(table)
            except Exception as e:
                logger.info("Table delete failed")
                logger.error(e)
            try:
                create_table_bigquery(
                    client, table_id, dataset_id, project_id, schema, clustering_fields
                )
            except Exception as e:
                logger.info("Table recreate failed")
                logger.error(e)

    except NotFound:
        create_table_bigquery(
            client, table_id, dataset_id, project_id, schema, clustering_fields
        )
    return "ok"


def create_table_bigquery(
    client, table_id, dataset_id, project_id, schema, clustering_fields
):
    table_ref = "{}.{}.{}".format(project_id, dataset_id, table_id)

    table = bigquery.Table(table_ref, schema=schema)

    if clustering_fields is not None:
        table.clustering_fields = clustering_fields

    table = client.create_table(table)  # Make an API request.
    logger.info("table created")
    logger.info(
        "Created table {}.{}.{}".format(table.project, table.dataset_id, table.table_id)
    )


def insert_rows_bigquery(client, table_id, dataset_id, project_id, data):
    table_ref = "{}.{}.{}".format(project_id, dataset_id, table_id)
    check_table_existence(client, table_ref)

    table = client.get_table(table_ref)

    resp = None
    if len(data) > 0:
        while resp is None:
            resp = insert_rows_json_retry(client, data, table)
            if len(resp) > 0:
                logger.info(str(resp))
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
    first_run = True
    for timerange in time_ranges:
        print(timerange)
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
                    "location": item.get("location", ""),
                    "conversions": conversions,
                    "actions": actions,
                }
            )
        if (
            setup_bigquery_table(
                first_run,
                bigquery_client,
                attributes["table_id"],
                attributes["dataset_id"],
                attributes["gcp_project_id"],
                schema_facebook_stat,
                clustering_fields_facebook,
            )
            == "ok"
        ):
            insert_rows_bigquery(
                bigquery_client,
                attributes["table_id"],
                attributes["dataset_id"],
                attributes["gcp_project_id"],
                fb_source,
            )
            first_run = False
    logger.info("Execution complete")
