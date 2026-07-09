import requests
import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials

# --- Google Sheets Auth ---
scope = ["https://spreadsheets.google.com/feeds",
         "https://www.googleapis.com/auth/drive"]
creds = ServiceAccountCredentials.from_json_keyfile_name("service_account.json", scope)
client = gspread.authorize(creds)

sheet = client.open_by_key("1YAzHR1djQQSyW8Cz9-y6HxLV7XQY9xSm6mVnBy8a7lc").worksheet("Sample_Data")

# --- Fetch Data ---
base_url = "https://rista-api-url.com"   # replace with actual base

transfer = requests.get(f"{base_url}/inventory/transfer/page").json()
grn = requests.get(f"{base_url}/inventory/grn/page").json()
stock = requests.post(f"{base_url}/inventory/item/stock", json={}).json()

# --- Convert to DataFrame ---
df_transfer = pd.DataFrame(transfer.get("data", []))
df_grn = pd.DataFrame(grn.get("data", []))
df_stock = pd.DataFrame(stock.get("data", []))

# --- Combine & Push ---
combined = pd.concat([df_transfer, df_grn, df_stock], ignore_index=True)

sheet.clear()
sheet.update([combined.columns.tolist()] + combined.values.tolist())
