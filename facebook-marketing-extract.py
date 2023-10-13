import facebook_business
from facebook_business.api import FacebookAdsApi
import gspread
from google.oauth2 import service_account
from dotenv import load_dotenv
import os

# Fetch campaign data from Facebook Ads API (modify as needed)
#from facebook_business.adobjects.adaccount import AdAccount
from facebook_business.adobjects.adaccount import AdAccount

load_dotenv()
account_id = os.getenv("account_id")
google_sheets_credentials = os.getenv("google_sheets_credentials")
google_sheets_spreadsheet_id = os.getenv("google_sheets_spreadsheet_id")
google_sheets_worksheet_name = os.getenv("google_sheets_worksheet_name")
app_id = os.getenv("app_id")
app_secret = os.getenv("app_secret")
access_token = os.getenv("access_token")

# Initialize Facebook Ads API
FacebookAdsApi.init(app_id, app_secret, access_token)

campaign_fields = ['id','name']
adset_fields = ['id']
campaigns = AdAccount(account_id).get_campaigns(fields=campaign_fields)
adsets = AdAccount(account_id).get_ad_sets(fields = adset_fields)

breakpoint()
# Initialize Google Sheets API
gc = gspread.service_account(filename=google_sheets_credentials)
sh = gc.open_by_key(google_sheets_spreadsheet_id)
worksheet = sh.worksheet(google_sheets_worksheet_name)

# Push data to Google Sheets
data_to_push = [['Campaign Name', 'Spend', 'Impressions', 'Clicks']]
#for adset in adsets:
#    data_to_push.append([adset['campaign_name'], adset['spend'], adset['impressions'], adset['clicks']])

# Update the worksheet with new data
worksheet.clear()
worksheet.update('A1', data_to_push)

print('Data successfully updated in Google Sheets!')
