import os
import json
import time
import jwt
import requests
import pandas as pd
from datetime import datetime, timedelta
import gspread
from google.oauth2.service_account import Credentials

print("🚀 Live Script Started")

# ---------------- AUTH ---------------- #

API_KEY = os.environ["API_KEY"]
SECRET_KEY = os.environ["SECRET_KEY"]

def get_token():
    payload = {"iss": API_KEY, "iat": int(time.time())}
    return jwt.encode(payload, SECRET_KEY, algorithm="HS256")

def headers():
    return {
        "x-api-key": API_KEY,
        "x-api-token": get_token(),
        "content-type": "application/json"
    }

# ---------------- GOOGLE ---------------- #

creds_dict = json.loads(os.environ["GOOGLE_CREDENTIALS"])

creds = Credentials.from_service_account_info(
    creds_dict,
    scopes=[
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive"
    ]
)

client = gspread.authorize(creds)

# ✅ Use KEY (stable)
spreadsheet = client.open_by_key("1CVUS-BSBfDIoQI4Yk2GB4_Zp1CIJRF-9YRfpvCih-FM")

print("✅ Connected to Google Sheet")

# ---------------- FETCH BRANCH ---------------- #

b_url = "https://api.ristaapps.com/v1/branch/list"
b_resp = requests.get(b_url, headers=headers())

data = b_resp.json()
if isinstance(data, dict):
    data = data.get("data", [])

branches = [
    b["branchCode"] for b in data
    if isinstance(b, dict) and b.get("status") == "Active"
]

print("🏪 Branch count:", len(branches))

# ---------------- DATE ---------------- #

now = datetime.now()
today = now.strftime("%Y-%m-%d")
last_week = (now - timedelta(days=7)).strftime("%Y-%m-%d")

# ---------------- FETCH SALES ---------------- #

def fetch_sales(day):
    print(f"\n📥 Fetching sales for {day}")
    all_data = []

    for b in branches:
        last_key = None

        while True:
            params = {"branch": b, "day": day}
            if last_key:
                params["lastKey"] = last_key

            r = requests.get(
                "https://api.ristaapps.com/v1/sales/summary",
                headers=headers(),
                params=params,
                timeout=30
            )

            if r.status_code != 200:
                break

            js = r.json()
            data = js.get("data", [])

            if not data:
                break

            df = pd.json_normalize(data)
            all_data.append(df)

            last_key = js.get("lastKey")
            if not last_key:
                break

    if not all_data:
        print(f"❌ No data for {day}")
        return pd.DataFrame()

    final_df = pd.concat(all_data, ignore_index=True)
    print("📊 Rows:", final_df.shape)

    return final_df

# ---------------- RUN ---------------- #

today_df = fetch_sales(today)
lastweek_df = fetch_sales(last_week)

if today_df.empty:
    print("❌ No today data")
    exit()

# ---------------- MAPPING ---------------- #

print("\n🔗 Applying Mapping...")

help_ws = spreadsheet.worksheet("Help Sheet")

# Store Type & Region
data = help_ws.get("G:M")
branch_master = pd.DataFrame(data[1:], columns=data[0])
branch_master = branch_master[["Store Name","Ownership","Region"]]

store_map = dict(zip(branch_master["Store Name"], branch_master["Ownership"]))
region_map = dict(zip(branch_master["Store Name"], branch_master["Region"]))

# Source
source_data = help_ws.get("D:E")
source_master = pd.DataFrame(source_data[1:], columns=source_data[0])
source_map = dict(zip(source_master["Channel"], source_master["Source"]))

# Apply to both
for df in [today_df, lastweek_df]:

    df["Store Type"] = df["branchName"].map(store_map).fillna("Unknown")
    df["Region"] = df["branchName"].map(region_map).fillna("Unknown")
    df["Source"] = df["channel"].map(source_map).fillna("Other")

    df["invoiceDate"] = pd.to_datetime(df["invoiceDate"])
    df["Hour"] = df["invoiceDate"].dt.hour

print("✅ Mapping Done")

# ---------------- NET SALES ---------------- #

for df in [today_df, lastweek_df]:

    df["netAmount"] = pd.to_numeric(df["netAmount"], errors="coerce").fillna(0)
    df["chargeAmount"] = pd.to_numeric(df["chargeAmount"], errors="coerce").fillna(0)

    df["Net Sales"] = (
        (df["netAmount"] + df["chargeAmount"])
        .where(df["status"] == "Closed", 0)
    )

print("✅ Net Sales calculated")

# ---------------- SAME TIME FILTER ---------------- #

now_time = datetime.now().time()

today_df = today_df[today_df["invoiceDate"].dt.time <= now_time]
lastweek_df = lastweek_df[lastweek_df["invoiceDate"].dt.time <= now_time]

print("⏱ Same time filter applied")

# ---------------- SUMMARY ---------------- #

today_sales = today_df["Net Sales"].sum()
lastweek_sales = lastweek_df["Net Sales"].sum()

growth = ((today_sales - lastweek_sales) / lastweek_sales * 100) if lastweek_sales else 0

summary = pd.DataFrame({
    "Metric": ["Today Sales", "Last Week Sales", "Growth %"],
    "Value": [round(today_sales,2), round(lastweek_sales,2), round(growth,2)]
})

# ---------------- PUSH ---------------- #

def push(sheet_name, df):
    print(f"\n📤 Updating sheet: {sheet_name}")

    try:
        ws = spreadsheet.worksheet(sheet_name)
    except:
        ws = spreadsheet.add_worksheet(title=sheet_name, rows="1000", cols="30")

    print("Rows:", len(df))

    df = df.fillna("").astype(str)
    data = [df.columns.tolist()] + df.values.tolist()

    ws.clear()
    ws.update(data, value_input_option="USER_ENTERED")

    print(f"✅ {sheet_name} updated")

# ---------------- EXECUTE ---------------- #

print("\n📊 Pushing data...")

push("Summary", summary)
push("Raw Data", today_df)

# Optional debug sheet
# push("Last Week Raw", lastweek_df)

print("\n🎉 SUCCESS")
