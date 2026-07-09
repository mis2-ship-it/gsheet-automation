import os
import requests
import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials

# --- Load secrets from environment ---
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
headers = {
    "x-api-key": api_key,
    "x-secret-key": secret_key,
    "Content-Type": "application/json"
}

def fetch_data(endpoint, method="GET", payload=None):
    url = base_url + endpoint
    print(f"Calling: {url}")  # Debug log
    if method == "GET":
        response = requests.get(url, headers=headers)
    else:
        response = requests.post(url, headers=headers, json=payload or {})
    response.raise_for_status()
    return response.json()

# --- Fetch Data ---
transfer = fetch_data("/inventory/transfer/page", "GET")
grn = fetch_data("/inventory/grn/page", "GET")
stock = fetch_data("/inventory/item/stock", "POST")

# --- Convert to DataFrame ---
df_transfer = pd.DataFrame(transfer.get("data", []))
df_grn = pd.DataFrame(grn.get("data", []))
df_stock = pd.DataFrame(stock.get("data", []))

combined = pd.concat([df_transfer, df_grn, df_stock], ignore_index=True)

# --- Push to Sheet ---
sheet.clear()
sheet.update([combined.columns.tolist()] + combined.values.tolist())

print("✅ Inventory data pushed to Google Sheet successfully!")
