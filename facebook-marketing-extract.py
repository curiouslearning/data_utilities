import facebook_business
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

# Fetch campaign data from Facebook Ads API (modify as needed)
# from facebook_business.adobjects.adaccount import AdAccount


load_dotenv()
account_id = os.getenv("account_id")
google_sheets_credentials = os.getenv("google_sheets_credentials")
google_sheets_spreadsheet_id = os.getenv("google_sheets_spreadsheet_id")
google_sheets_worksheet_name = os.getenv("google_sheets_worksheet_name")
app_id = os.getenv("app_id")
app_secret = os.getenv("app_secret")
access_token = os.getenv("access_token")

insight_fields = [
    "ad_id",
    "campaign_name",
    "impressions",
    "reach",
    "clicks",
    "cpc",
    "cpm",
    "ctr",
    "objective",
    "spend",
]


# Initialize Facebook Ads API
FacebookAdsApi.init(app_id, app_secret, access_token)


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


def is_in_campaigns(id, campaigns):
    for campaign in campaigns:
        id = campaign["id"]
        if len(id) > 0:
            return True
    return False


def get_insights(adset):
    insights_params = {"level": "ad"}
    return ad_set.get_insights(fields=insight_fields, params=insights_params)


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


def write_file(data):
    f = open("insights.csv", "w")
    f.write(data)
    f.close()


adsetsData = pd.DataFrame(columns=insight_fields)

campaigns = get_campaigns(account_id)
adsets = get_adsets(account_id)
i = 0
for adset in adsets:
    adset_id = adset.get_id()
    if is_in_campaigns(adset_id, campaigns):
        print("ad is part of campaign")

    status = adset[AdSet.Field.status]
    print(f"Processing ad set {adset_id}")

    ad_set = AdSet(fbid=adset_id)

    insights_data = get_insights(adset)
    print("insights_data: " + str(insights_data))

    response_headers = insights_data.headers()
    usage = get_api_usage_count(response_headers)
    print("usage = " + str(usage))

    if usage >= 7:
        time.sleep(10)

    if len(insights_data) != 0:
        print("length of insights data: " + str(len(insights_data)))
        for insights in insights_data:
            # Extract insight data for the current ad set
            print("#####")
            print(insight_fields)
            print("#####")
            print(insights)
            insights_dict = {field: insights[field] for field in insight_fields}
            print(insights_dict)
            adsetsData = adsetsData.append(insights_dict, ignore_index=True)
    print("number of iterations = " + str(i))
    i = i + 1
write_file(adsetsData)

"""

# Initialize Google Sheets API
gc = gspread.service_account(filename=google_sheets_credentials)
sh = gc.open_by_key(google_sheets_spreadsheet_id)
worksheet = sh.worksheet(google_sheets_worksheet_name)

# Push data to Google Sheets
data_to_push = [["Campaign Name", "Spend", "Impressions", "Clicks"]]
# for adset in adsets:
#    data_to_push.append([adset['campaign_name'], adset['spend'], adset['impressions'], adset['clicks']])

# Update the worksheet with new data
worksheet.clear()
worksheet.update("A1", data_to_push)

print("Data successfully updated in Google Sheets!")
"""
print(adsetsData)
