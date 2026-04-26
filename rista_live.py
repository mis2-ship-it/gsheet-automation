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

spreadsheet = client.open_by_url(
    "https://docs.google.com/spreadsheets/d/1CVUS-BSBfDIoQI4Yk2GB4_Zp1CIJRF-9YRfpvCih-FM/edit?gid=1217602119#gid=1217602119"
)

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
        print("❌ No data fetched")
        return pd.DataFrame()

    final_df = pd.concat(all_data, ignore_index=True)
    print("📊 Rows:", final_df.shape)

    return final_df

# ---------------- RUN ---------------- #

today_df = fetch_sales(today)

if today_df.empty:
    print("❌ No data")
    exit()

# ---------------- MASTER MAPPING ---------------- #

print("\n🔗 Applying Mapping...")

help_ws = spreadsheet.worksheet("Help Sheet")

# Store Type & Region
data = help_ws.get("G:M")
branch_master = pd.DataFrame(data[1:], columns=data[0])
branch_master = branch_master[["Store Name","Ownership","Region"]]

store_map = dict(zip(branch_master["Store Name"], branch_master["Ownership"]))
region_map = dict(zip(branch_master["Store Name"], branch_master["Region"]))

today_df["Store Type"] = today_df["branchName"].map(store_map)
today_df["Region"] = today_df["branchName"].map(region_map)

# Source
source_data = help_ws.get("D:E")
source_master = pd.DataFrame(source_data[1:], columns=source_data[0])
source_map = dict(zip(source_master["Channel"], source_master["Source"]))

today_df["Source"] = today_df["channel"].map(source_map)

# Hour
today_df["invoiceDate"] = pd.to_datetime(today_df["invoiceDate"])
today_df["Hour"] = today_df["invoiceDate"].dt.hour

# Fill blanks
today_df["Store Type"] = today_df["Store Type"].fillna("Unknown")
today_df["Region"] = today_df["Region"].fillna("Unknown")
today_df["Source"] = today_df["Source"].fillna("Other")

print("✅ Mapping Done")

# ---------------- NET SALES (ONLY CLOSED) ---------------- #

today_df["netAmount"] = pd.to_numeric(today_df["netAmount"], errors="coerce").fillna(0)
today_df["chargeAmount"] = pd.to_numeric(today_df["chargeAmount"], errors="coerce").fillna(0)

today_df["Net Sales"] = 0

today_df.loc[
    today_df["status"] == "Closed",
    "Net Sales"
] = today_df["netAmount"] + today_df["chargeAmount"]

print("✅ Net Sales Calculated (Closed only)")

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
    ws.update(
        data,
        value_input_option="USER_ENTERED"
    )

    print(f"✅ {sheet_name} updated")

# ---------------- EXECUTE ---------------- #

print("\n📊 Pushing Raw Data...")

push("Raw Data", today_df)

print("\n🎉 SUCCESS")
