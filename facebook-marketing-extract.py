from facebook_business.api import FacebookAdsApi
import gspread
from google.oauth2 import service_account
from dotenv import load_dotenv
import os
import json
import pandas as pd
import time
from facebook_business.adobjects.adaccount import AdAccount
from facebook_business.adobjects.adsinsights import AdsInsights
from facebook_business.adobjects.adset import AdSet

load_dotenv()
account_id = os.getenv("account_id")
google_sheets_credentials = os.getenv("google_sheets_credentials")
google_sheets_spreadsheet_id = os.getenv("google_sheets_spreadsheet_id")
google_sheets_worksheet_name = os.getenv("google_sheets_worksheet_name")
app_id = os.getenv("app_id")
app_secret = os.getenv("app_secret")
access_token = os.getenv("access_token")

insight_fields = [
    "adset_id",
    "campaign_name",
    "impressions",
    "reach",
    "clicks",
    "actions",
    "cpc",
    "cpm",
    "ctr",
    "objective",
    "spend",
]

# Initialize Facebook Ads API
FacebookAdsApi.init(app_id, app_secret, access_token)

# Fetch campaign data from Facebook Ads API (modify as needed)
# from facebook_business.adobjects.adaccount import AdAccount


def get_campaigns(fb_account_id):
    campaign_fields = ["id", "name", "status"]
    campaign_params = {"effective_status": ["ACTIVE"], "limit": "500"}
    return AdAccount(fb_account_id).get_campaigns(
        fields=campaign_fields, params=campaign_params
    )


def get_adsets(fb_account_id):
    adset_fields = ["id", "status", "campaign_id"]
    adset_params = {"effective_status": ["ACTIVE"], "limit": "500"}
    return AdAccount(fb_account_id).get_ad_sets(
        fields=adset_fields, params=adset_params
    )


# helper function to determine if an adset is part of our active campaigns list
def is_in_campaigns(id, campaigns):
    for campaign in campaigns:
        id = campaign["id"]
        if len(id) > 0:
            return True
    return False


# get the insights data off an adset
def get_insights(adset):
    insights_params = {"level": "ad"}
    return ad_set.get_insights(fields=insight_fields, params=insights_params)


# determine api load for rate limiting
def get_api_usage_count(response_headers):
    x_business_use_case_usage = json.loads(
        response_headers.get("x-business-use-case-usage")
    )

    # Access the value associated with the key "1180800068778728" - I don't know why this is the key yet
    response_values = x_business_use_case_usage.get("1180800068778728")
    response_values = dict(response_values[0])
    call_count = response_values["call_count"]
    total_time = response_values["total_time"]
    total_cputime = response_values["total_cputime"]
    print(str(call_count) + str(total_time) + str(total_cputime))
    usage = call_count + total_cputime + total_time
    return usage


# mobile_app_install is part of a list in the 'actions' field.  So this logic exracts that value
def extract_mobile_installs(ad_insights):
    # Extract the 'actions' list from the AdInsights object
    first_ad_insights = ad_insights[0]
    # ad_insight = ad_insights[0]
    actions_list = first_ad_insights.get("actions", [])

    # Initialize a variable to store the 'mobile_app_install' value
    mobile_app_install_value = 0

    # Iterate through the 'actions' list and find the 'mobile_app_install' action
    for action in actions_list:
        if action.get("action_type") == "mobile_app_install":
            mobile_app_install_value = int(action.get("value"))
            break  # Exit the loop once you find the 'mobile_app_install' action

    return mobile_app_install_value


# Convert the AdInsights data to a dictionary row and add mobile installs as a column
def build_new_row(insights_data):
    mobile_installs = extract_mobile_installs(insights_data)

    for insights in insights_data:
        # Extract insight data for the current ad set
        insights_dict = {}
        for field in insight_fields:
            try:
                insights_dict[field] = insights[field]
            except KeyError:
                continue

        insights_dict["actions"] = mobile_installs
    return insights_dict


def write_google_sheet(data_frame):
    # Initialize Google Sheets API
    gc = gspread.service_account(filename=google_sheets_credentials)
    sh = gc.open_by_key(google_sheets_spreadsheet_id)
    worksheet = sh.worksheet(google_sheets_worksheet_name)

    # Push data to Google Sheets
    adsetsData_with_header = [adsetsData.columns.tolist()] + adsetsData.values.tolist()
    # Update the worksheet with new data
    worksheet.clear()
    worksheet.update("A1", adsetsData_with_header)


# Get the insights for each Adset and store in a DataFrame
adsetsData = pd.DataFrame(columns=insight_fields)

campaigns = get_campaigns(account_id)
adsets = get_adsets(account_id)
i = 0
for adset in adsets:
    adset_id = adset.get_id()
    if is_in_campaigns(adset_id, campaigns):
        ad_set = AdSet(fbid=adset_id)
        insights_data = get_insights(adset)

        response_headers = insights_data.headers()
        usage = get_api_usage_count(response_headers)

        if usage >= 7:
            time.sleep(6)

        if len(insights_data) != 0:
            insights_dict = build_new_row(insights_data)
            df_row = pd.DataFrame(insights_dict, index=[0])
            adsetsData = pd.concat([adsetsData, df_row], ignore_index=True)
    i = i + 1
    print(str(i))
    time.sleep(1)
# handle NaN values

adsetsData = adsetsData.fillna(0)
print(adsetsData)
write_google_sheet(adsetsData)
