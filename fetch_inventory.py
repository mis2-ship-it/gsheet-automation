import os
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
scope = ["https://spreadsheets.google.com/feeds",
         "https://www.googleapis.com/auth/drive"]
creds = ServiceAccountCredentials.from_json_keyfile_name("service_account.json", scope)
client = gspread.authorize(creds)
sheet = client.open_by_key(sheet_id).worksheet(tab_name)

# --- API Setup ---
base_url = "https://api.ristaapps.com"

# API headers
headers = {
    "x-api-key": api_key,
    "x-secret-key": secret_key,
    "Content-Type": "application/json"
}

def fetch_data(endpoint, method="GET", params=None, payload=None):
    url = base_url + endpoint
    print(f"Calling: {url}")
    
    try:
        if method == "GET":
            response = requests.get(url, headers=headers, params=params)
        else:
            response = requests.post(url, headers=headers, json=payload or {})
            
        response.raise_for_status()
        return response.json()
        
    except requests.exceptions.HTTPError as e:
        print(f"\n❌ API Call Failed for {endpoint}")
        print(f"Status Code: {response.status_code}")
        print(f"Server Message: {response.text}\n")
        raise e

# --- Fetch Data ---

# TEMPORARILY COMMENTED OUT - Will update once correct endpoint is provided
# transfer = fetch_data("/inventory/transfer/page", "GET")
df_transfer = pd.DataFrame() 

print("⏳ Fetching GRN data...")
grn = fetch_data("/inventory/grn/page", "GET")

print("⏳ Fetching Stock data...")
stock = fetch_data("/inventory/item/stock", "POST")

# --- Combine Data ---
df_grn = pd.DataFrame(grn.get("data", []))
df_stock = pd.DataFrame(stock.get("data", []))

# Collect non-empty DataFrames
dataframes = [df for df in [df_transfer, df_grn, df_stock] if not df.empty]

if dataframes:
    combined = pd.concat(dataframes, ignore_index=True)
    combined = combined.fillna("")  # Replace NaN with empty strings for Google Sheets
    
    # --- Push to Sheet ---
    sheet.clear()
    sheet.update([combined.columns.tolist()] + combined.values.tolist())
    print("✅ GRN and Stock data pushed to Google Sheet successfully!")
else:
    print("⚠️ No data returned from endpoints.")
