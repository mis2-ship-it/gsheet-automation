import os
import time
import jwt
import requests
import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials

# --- Load secrets ---
api_key = os.environ["API_KEY"]
secret_key = os.environ["SECRET_KEY"]
google_credentials = os.environ["GOOGLE_CREDENTIALS"]

sheet_id = "1YAzHR1djQQSyW8Cz9-y6HxLV7XQY9xSm6mVnBy8a7lc"
tab_name = "Sample_Data"

# --- Write Google credentials to file ---
with open("service_account.json", "w") as f:
    f.write(google_credentials)

# --- Google Sheets Auth ---
scope = [
    "https://spreadsheets.google.com/feeds",
    "https://www.googleapis.com/auth/drive"
]
creds = ServiceAccountCredentials.from_json_keyfile_name("service_account.json", scope)
client = gspread.authorize(creds)
sheet = client.open_by_key(sheet_id).worksheet(tab_name)

# --- API Setup ---
base_url = "https://api.ristaapps.com/v1"  # Correct v1 base URL for Rista API

def generate_jwt():
    """Generates the required JWT token using PyJWT."""
    payload = {
        "iss": api_key,
        "iat": int(time.time()),
        "exp": int(time.time()) + 300  # Token valid for 5 minutes
    }
    return jwt.encode(payload, secret_key, algorithm="HS256")

def safe_fetch(endpoint, method="GET", params=None, payload=None):
    """Fetches endpoint data safely without breaking execution on error."""
    url = base_url + endpoint
    print(f"Calling ({method}): {url}")
    
    headers = {
        "x-api-key": api_key,
        "x-api-token": generate_jwt(),
        "Content-Type": "application/json"
    }
    
    try:
        if method == "GET":
            response = requests.get(url, headers=headers, params=params)
        else:
            response = requests.post(url, headers=headers, json=payload or {})
            
        response.raise_for_status()
        data = response.json()
        print(f"✅ Success fetching {endpoint}")
        
        if isinstance(data, dict):
            return data.get("data", [])
        elif isinstance(data, list):
            return data
        return []
        
    except Exception as e:
        print(f"⚠️ Warning: Failed for {endpoint} | Error: {e}")
        return []

# --- Fetch Data ---
print("\n--- Fetching Inventory Data ---")
transfer_data = safe_fetch("/inventory/transfer/page", "GET")
grn_data = safe_fetch("/inventory/grn/page", "GET")
stock_data = safe_fetch("/inventory/item/stock", "POST", payload={})

# --- Convert & Combine ---
df_transfer = pd.DataFrame(transfer_data)
df_grn = pd.DataFrame(grn_data)
df_stock = pd.DataFrame(stock_data)

dataframes = [df for df in [df_transfer, df_grn, df_stock] if not df.empty]

if dataframes:
    combined = pd.concat(dataframes, ignore_index=True)
    combined = combined.fillna("")  # Replace NaN values with empty strings
    
    # Push to Google Sheet
    sheet.clear()
    sheet.update([combined.columns.tolist()] + combined.values.tolist())
    print("\n✅ Available inventory data successfully pushed to Google Sheet!")
else:
    print("\n❌ No data returned. Please verify your API_KEY and SECRET_KEY in GitHub Secrets.")
