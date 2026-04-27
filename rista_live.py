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

spreadsheet = client.open_by_key("1CVUS-BSBfDIoQI4Yk2GB4_Zp1CIJRF-9YRfpvCih-FM")

print("✅ Connected to Google Sheet")

# ---------------- TIME (IST SAFE) ---------------- #

now = datetime.utcnow() + timedelta(hours=5, minutes=30)

today = now.strftime("%Y-%m-%d")
last_week = (now - timedelta(days=7)).strftime("%Y-%m-%d")

print("🕒 IST Time:", now)

# ---------------- FETCH BRANCH ---------------- #

b_resp = requests.get("https://api.ristaapps.com/v1/branch/list", headers=headers())

data = b_resp.json()
if isinstance(data, dict):
    data = data.get("data", [])

branches = [
    b["branchCode"] for b in data
    if isinstance(b, dict) and b.get("status") == "Active"
]

print("🏪 Branch count:", len(branches))

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

# ---------------- TIME FILTER ---------------- #

# ---------------- TIME FILTER (FIXED) ---------------- #

today_df["invoiceDate"] = pd.to_datetime(today_df["invoiceDate"], errors="coerce")
lastweek_df["invoiceDate"] = pd.to_datetime(lastweek_df["invoiceDate"], errors="coerce")

# Convert timezone properly
today_df["invoiceDate"] = today_df["invoiceDate"].dt.tz_convert("Asia/Kolkata")
lastweek_df["invoiceDate"] = lastweek_df["invoiceDate"].dt.tz_convert("Asia/Kolkata")

# Get current IST datetime
now_ist = datetime.utcnow() + timedelta(hours=5, minutes=30)

# ✅ Correct filtering
today_df = today_df[today_df["invoiceDate"] <= now_ist]

print("⏱ Time filter applied correctly")

# ---------------- ADD EXTRA COLUMNS ---------------- #

today_df["Date"] = today_df["invoiceDate"].dt.strftime("%Y-%m-%d")
lastweek_df["Date"] = lastweek_df["invoiceDate"].dt.strftime("%Y-%m-%d")

today_df["Hour"] = today_df["invoiceDate"].dt.hour
lastweek_df["Hour"] = lastweek_df["invoiceDate"].dt.hour

today_df["Data_Type"] = "Today"
lastweek_df["Data_Type"] = "Last Week"

# ---------------- MERGE ---------------- #

final_df = pd.concat([today_df, lastweek_df], ignore_index=True)

# ---------------- MAPPING ---------------- #

print("\n🔗 Applying Mapping...")

help_ws = spreadsheet.worksheet("Help Sheet")

# Store mapping
branch_data = help_ws.get("G:M")
branch_master = pd.DataFrame(branch_data[1:], columns=branch_data[0])

branch_master["Store Name"] = branch_master["Store Name"].str.strip().str.lower()

store_map = dict(zip(branch_master["Store Name"], branch_master["Ownership"]))
region_map = dict(zip(branch_master["Store Name"], branch_master["Region"]))

# Source mapping
source_data = help_ws.get("D:E")
source_master = pd.DataFrame(source_data[1:], columns=source_data[0])

source_master["Channel"] = source_master["Channel"].str.strip().str.lower()

source_map = dict(zip(source_master["Channel"], source_master["Source"]))

# Apply mapping
final_df["branchName"] = final_df["branchName"].str.strip().str.lower()
final_df["channel"] = final_df["channel"].str.strip().str.lower()

final_df["Store Type"] = final_df["branchName"].map(store_map).fillna("Missing")
final_df["Region"] = final_df["branchName"].map(region_map).fillna("Missing")
final_df["Source"] = final_df["channel"].map(source_map).fillna("Missing")

print("✅ Mapping Done")

# ---------------- NET SALES ---------------- #

final_df["netAmount"] = pd.to_numeric(final_df["netAmount"], errors="coerce").fillna(0)
final_df["chargeAmount"] = pd.to_numeric(final_df["chargeAmount"], errors="coerce").fillna(0)

final_df["Net Sales"] = (
    (final_df["netAmount"] + final_df["chargeAmount"])
    .where(final_df["status"] == "Closed", 0)
)

print("✅ Net Sales Done")

# ---------------- SUMMARY ---------------- #

today_sales = final_df[final_df["Data_Type"] == "Today"]["Net Sales"].sum()
lastweek_sales = final_df[final_df["Data_Type"] == "Last Week"]["Net Sales"].sum()

growth = ((today_sales - lastweek_sales) / lastweek_sales * 100) if lastweek_sales else 0

summary = pd.DataFrame({
    "Metric": ["Today Sales", "Last Week Sales", "Growth %"],
    "Value": [round(today_sales, 2), round(lastweek_sales, 2), round(growth, 2)]
})

# ---------------- PUSH ---------------- #

def push(sheet_name, df):
    print(f"\n📤 Updating sheet: {sheet_name}")

    try:
        ws = spreadsheet.worksheet(sheet_name)
    except:
        ws = spreadsheet.add_worksheet(title=sheet_name, rows="2000", cols="40")

    df = df.fillna("").astype(str)
    data = [df.columns.tolist()] + df.values.tolist()

    ws.clear()
    ws.update(data, value_input_option="USER_ENTERED")

    print(f"✅ {sheet_name} updated | Rows: {len(df)}")

# ---------------- EXECUTE ---------------- #

print("\n📊 Pushing Data...")

push("Raw Data", final_df)
push("Summary", summary)

print("\n🎉 FINAL SUCCESS")
