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
scope = ["https://spreadsheets.google.com/feeds",
         "https://www.googleapis.com/auth/drive"]
creds = ServiceAccountCredentials.from_json_keyfile_name("service_account.json", scope)
client = gspread.authorize(creds)
sheet = client.open_by_key(sheet_id).worksheet(tab_name)

# --- Base URL ---
base_url = "https://api.ristaapps.com/v1"  # Added /v1 per Rista documentation

def generate_jwt():
    """Generate dynamic JWT token required by Rista API."""
    payload = {
        "iss": api_key,
        "iat": int(time.time()),
        "exp": int(time.time()) + 300  # Expires in 5 minutes
    }
    return jwt.encode(payload, secret_key, algorithm="HS256")

def fetch_data(endpoint, method="GET", params=None, payload=None):
    url = base_url + endpoint
    print(f"Calling ({method}): {url}")
    
    # Rista required authentication headers
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
        return data.get("data", []) if isinstance(data, dict) else data
        
    except Exception as e:
        print(f"⚠️ Warning: Failed for {endpoint} | Error: {e}")
        return []

# --- Fetch Data ---
print("\n--- Fetching Inventory Data ---")

# Pass required store parameters if needed by your account
params = {
    # "date": "2026-07-31" # Format YYYY-MM-DD if mandatory for date queries
}

transfer_data = fetch_data("/inventory/transfer/page", "GET", params=params)
grn_data = fetch_data("/inventory/grn/page", "GET", params=params)
stock_data = fetch_data("/inventory/item/stock", "POST", payload={})

# --- Combine Data ---
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
    print("\n❌ No data was retrieved. Verify API_KEY, SECRET_KEY, and permissions.")
