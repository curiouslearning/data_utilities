from google.cloud import bigquery
from google.cloud.exceptions import NotFound
from datetime import datetime, date, timedelta
import logging
from facebook_business.api import FacebookAdsApi
from facebook_business.adobjects.adaccount import AdAccount
from facebook_business.adobjects.adaccountuser import AdAccountUser
from facebook_business.adobjects.adsinsights import AdsInsights
from facebook_business.adobjects.campaign import Campaign
from google.cloud import secretmanager

client = secretmanager.SecretManagerServiceClient()
logger = logging.getLogger()
attributes = {}


schema_exchange_rate = [
    bigquery.SchemaField("date", "DATE", mode="REQUIRED"),
]

schema_facebook_stat = [
    bigquery.SchemaField("date", "DATE", mode="REQUIRED"),
    bigquery.SchemaField("campaign_id", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("campaign_name", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("start_date", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("end_date", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("clicks", "INTEGER", mode="REQUIRED"),
    bigquery.SchemaField("impressions", "INTEGER", mode="REQUIRED"),
    bigquery.SchemaField("reach", "INTEGER", mode="REQUIRED"),
    bigquery.SchemaField("cpc", "FLOAT", mode="REQUIRED"),
    bigquery.SchemaField("spend", "FLOAT", mode="REQUIRED"),
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


def get_secret(secret):
    client = secretmanager.SecretManagerServiceClient()
    name = "projects/405806232197/secrets/" + secret + "/versions/latest"
    response = client.access_secret_version(name=name)
    return response.payload.data.decode("UTF-8")


def set_attributes():
    attributes = {
        "table_id": get_secret("table_id"),
        "dataset_id": get_secret("dataset_id"),
        "fb_access_token": get_secret("fb_access_token"),
        "fb_account_id": get_secret("fb_account_id"),
        "fb_app_id": get_secret("fb_app_id"),
        "fb_app_secret": get_secret("fb_app_secret"),
        "gcp_project_id": get_secret("gcp_project_id"),
    }
    return attributes


def exist_dataset_table(
    client, table_id, dataset_id, project_id, schema, clustering_fields=None
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
        client.get_table(table_ref)  # Make an API request.

    except NotFound:
        table_ref = "{}.{}.{}".format(project_id, dataset_id, table_id)

        table = bigquery.Table(table_ref, schema=schema)

        table.time_partitioning = bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY, field="date"
        )

        if clustering_fields is not None:
            table.clustering_fields = clustering_fields

        table = client.create_table(table)  # Make an API request.
        logger.info(
            "Created table {}.{}.{}".format(
                table.project, table.dataset_id, table.table_id
            )
        )
    return "ok"


def insert_rows_bq(client, table_id, dataset_id, project_id, data):
    table_ref = "{}.{}.{}".format(project_id, dataset_id, table_id)
    table = client.get_table(table_ref)
    resp = client.insert_rows_json(
        json_rows=data,
        table=table_ref,
    )
    print(resp)


# logger.info("Success uploaded to table {}".format(table.table_id))


def get_facebook_data(event):
    attributes = set_attributes()

    bigquery_client = bigquery.Client()

    yesterday = date.today() - timedelta(1)

    try:
        FacebookAdsApi.init(
            attributes["fb_app_id"],
            attributes["fb_app_secret"],
            attributes["fb_access_token"],
        )

        account = AdAccount("act_" + str(attributes["fb_account_id"]))
        insights = account.get_insights(
            fields=[
                AdsInsights.Field.account_id,
                AdsInsights.Field.campaign_id,
                AdsInsights.Field.campaign_name,
                AdsInsights.Field.date_start,
                AdsInsights.Field.date_stop,
                AdsInsights.Field.spend,
                AdsInsights.Field.impressions,
                AdsInsights.Field.reach,
                AdsInsights.Field.cpc,
                AdsInsights.Field.clicks,
                AdsInsights.Field.actions,
                AdsInsights.Field.conversions,
            ],
            params={
                "level": "campaign",
            },
        )
    except Exception as e:
        logger.info(e)
        print(e)
        raise

    fb_source = []

    for index, item in enumerate(insights):
        actions = []
        conversions = []

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

        fb_source.append(
            {
                "date": datetime.now().strftime("%Y-%m-%d"),
                "campaign_id": item.get("campaign_id"),
                "campaign_name": item.get("campaign_name"),
                "start_date": item.get("date_start"),
                "end_date": item.get("date_stop"),
                "clicks": item.get("clicks"),
                "impressions": item.get("impressions"),
                "reach": item.get("reach"),
                "cpc": item.get("cpc", 0),
                "spend": item.get("spend"),
                "conversions": conversions,
                "actions": actions,
            }
        )
        print("fb_source = " + str(fb_source))
        if (
            exist_dataset_table(
                bigquery_client,
                attributes["table_id"],
                attributes["dataset_id"],
                attributes["gcp_project_id"],
                schema_facebook_stat,
                clustering_fields_facebook,
            )
            == "ok"
        ):
            insert_rows_bq(
                bigquery_client,
                attributes["table_id"],
                attributes["dataset_id"],
                attributes["gcp_project_id"],
                fb_source,
            )
            return "ok"
