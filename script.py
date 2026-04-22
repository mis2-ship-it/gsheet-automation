print("Running latest version 🚀")
import os
import json
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials

# 🔐 Credentials from GitHub Secret
creds_dict = json.loads(os.environ["GOOGLE_CREDENTIALS"])

creds = Credentials.from_service_account_info(
    creds_dict,
    scopes=["https://www.googleapis.com/auth/spreadsheets"]
)

# 🔗 Connect to Google Sheets
client = gspread.authorize(creds)

# 👉 Your actual logic continues below
print("Connected successfully")
