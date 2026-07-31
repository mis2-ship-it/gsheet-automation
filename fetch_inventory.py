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

# Reverted to standard Bearer token authorization format
headers = {
    "Authorization": f"Bearer {api_key}",
    "Content-Type": "application/json"
}

def safe_fetch(endpoint, method="GET", payload=None):
    """Fetches data and handles errors gracefully so script doesn't crash."""
    url = base_url + endpoint
    print(f"Calling ({method}): {url}")
    
    try:
        if method == "GET":
            response = requests.get(url, headers=headers)
        else:
            response = requests.post(url, headers=headers, json=payload or {})
            
        response.raise_for_status()
        data = response.json()
        print(f"✅ Success fetching {endpoint}")
        return data.get("data", []) if isinstance(data, dict) else data
        
    except Exception as e:
        print(f"⚠️ Warning: Call failed for {endpoint} | Error: {e}")
        return []

# --- Fetch Data ---
print("\n--- Fetching Data ---")
transfer_data = safe_fetch("/inventory/transfer/page", "GET")
grn_data = safe_fetch("/inventory/grn/page", "GET")
stock_data = safe_fetch("/inventory/item/stock", "POST")

# --- Process & Combine ---
df_transfer = pd.DataFrame(transfer_data)
df_grn = pd.DataFrame(grn_data)
df_stock = pd.DataFrame(stock_data)

dataframes = [df for df in [df_transfer, df_grn, df_stock] if not df.empty]

if dataframes:
    combined = pd.concat(dataframes, ignore_index=True)
    combined = combined.fillna("")
    
    sheet.clear()
    sheet.update([combined.columns.tolist()] + combined.values.tolist())
    print("\n✅ Available inventory data pushed to Google Sheet successfully!")
else:
    print("\n❌ No data was retrieved from any endpoint. Check endpoint routes or credentials.")
