import yaml
import requests
import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials

# --- Load Config ---
with open("config.yml", "r") as f:
    config = yaml.safe_load(f)

base_url = config["rista"]["base_url"]
headers = {
    "API-KEY": config["rista"]["api_key"],
    "SECRET-KEY": config["rista"]["secret_key"]
}

# --- Google Sheets Auth ---
scope = ["https://spreadsheets.google.com/feeds",
         "https://www.googleapis.com/auth/drive"]
creds = ServiceAccountCredentials.from_json_keyfile_name(
    config["google_sheets"]["credentials_file"], scope)
client = gspread.authorize(creds)

sheet = client.open_by_key(config["google_sheets"]["sheet_id"]).worksheet(
    config["google_sheets"]["tab_name"])

# --- Fetch Data ---
def fetch_data(endpoint, method="GET", payload=None):
    url = base_url + endpoint
    if method == "GET":
        response = requests.get(url, headers=headers)
    else:
        response = requests.post(url, headers=headers, json=payload or {})
    response.raise_for_status()
    return response.json()

transfer = fetch_data(config["rista"]["endpoints"]["transfer"], "GET")
grn = fetch_data(config["rista"]["endpoints"]["grn"], "GET")
stock = fetch_data(config["rista"]["endpoints"]["stock"], "POST")

# --- Convert to DataFrame ---
df_transfer = pd.DataFrame(transfer.get("data", []))
df_grn = pd.DataFrame(grn.get("data", []))
df_stock = pd.DataFrame(stock.get("data", []))

combined = pd.concat([df_transfer, df_grn, df_stock], ignore_index=True)

# --- Push to Sheet ---
sheet.clear()
sheet.update([combined.columns.tolist()] + combined.values.tolist())

print("✅ Data pushed to Google Sheet successfully!")
