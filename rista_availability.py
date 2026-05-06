# =========================================================
# 🔥 IMPORTS
# =========================================================

import pandas as pd
import requests
import time
import json
import os
import jwt
from datetime import datetime
import pytz
import gspread
from google.oauth2.service_account import Credentials

# =========================================================
# 🔐 API AUTH (RISTA JWT)
# =========================================================

API_KEY = os.environ["API_KEY"]
SECRET_KEY = os.environ["SECRET_KEY"]

def get_token():
    payload = {
        "iss": API_KEY,
        "iat": int(time.time())
    }
    return jwt.encode(payload, SECRET_KEY, algorithm="HS256")

def api_headers():
    return {
        "x-api-key": API_KEY,
        "x-api-token": get_token(),
        "content-type": "application/json"
    }

# =========================================================
# 🔐 CONFIG
# =========================================================

WORKSHEET_NAME = "Hourly_Availability"

# =========================================================
# 🔐 GOOGLE SHEETS AUTH
# =========================================================

scope = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive"
]

creds_dict = json.loads(os.environ["GOOGLE_CREDENTIALS"])

creds = Credentials.from_service_account_info(
    creds_dict, scopes=scope
)

client = gspread.authorize(creds)

spreadsheet = client.open_by_key("1umqb0k_G0F-cAzMbrmqSYnEz06-NjmCANWtWEa_NS9w")

try:
    ws = spreadsheet.worksheet(WORKSHEET_NAME)
except:
    ws = spreadsheet.add_worksheet(WORKSHEET_NAME, 1000, 50)

help_ws = spreadsheet.worksheet("Help_Sheet")
help_df = pd.DataFrame(help_ws.get_all_records())

# Clean column names
help_df.columns = (
    help_df.columns
    .str.strip()
    .str.replace(" ", "")
)

print("Help Sheet Columns:", help_df.columns.tolist())


# =========================================================
# ⏰ TIME
# =========================================================

ist = pytz.timezone("Asia/Kolkata")
now = datetime.now(ist)

print("⏰ Run Time:", now)

# =========================================================
# 📡 FETCH BRANCHES
# =========================================================
b_resp = requests.get(
    "https://api.ristaapps.com/v1/branch/list",
    headers=api_headers()
)

data = b_resp.json()
branch_data = data.get("data", []) if isinstance(data, dict) else data

branches = []

for b in branch_data:
    if b.get("status") == "Active":
        code = b.get("branchCode")   # ✅ correct key

        if code:
            branches.append(str(code).strip().upper())

print("🏪 Branch count:", len(branches))

# COCO Stores

help_df["branchCode"] = (
    help_df["branchCode"]
    .astype(str)
    .str.strip()
    .str.upper()
)

# Normalize API branches
branches = [
    str(b).strip().upper()
    for b in branches
]

# COCO branch list
coco_branches = help_df[
    help_df["Ownership"].str.upper() == "COCO"
]["branchCode"].tolist()

print("RAW API SAMPLE:", branch_data[:5])
print("Sample API branches:", branches[:10])
print("Sample Help Sheet COCO:", coco_branches[:10])

# Filter only COCO
# Normalize everything properly
def clean(x):
    return str(x).strip().upper()

branches = [clean(b) for b in branches]
coco_branches = [clean(b) for b in coco_branches]

branches = list(set(branches) & set(coco_branches))

print("🏪 COCO Branch count:", len(branches))

# =========================================================
# 🍽️ FETCH AVAILABILITY
# =========================================================

def fetch_availability(branch):
    try:
        r = requests.get(
            "https://api.ristaapps.com/v1/menu/items",
            headers=api_headers(),
            params={"branch": branch},
            timeout=30
        )

        if r.status_code != 200:
            print(f"❌ API Fail for {branch}: {r.status_code}")
            return pd.DataFrame()

        data = r.json().get("data", [])

        if not data:
            return pd.DataFrame()

        df = pd.json_normalize(data)
        df["branch"] = branch

        return df

    except Exception as e:
        print(f"❌ Error for {branch}:", e)
        return pd.DataFrame()

# =========================================================
# 🚀 FETCH ALL DATA
# =========================================================

all_data = []

for b in branches:

    df = fetch_availability(b)

    if not df.empty:
        all_data.append(df)

    time.sleep(0.2)

# =========================================================
# ✅ CHECK DATA EXISTS
# =========================================================

if not all_data:
    print("❌ No availability data fetched")
    exit()

final_df = pd.concat(all_data, ignore_index=True)

print("✅ Data Fetched:", final_df.shape)

# =========================================================
# 🧠 CLEAN DATA
# =========================================================

if "available" in final_df.columns:
    final_df["Available_Flag"] = final_df["available"]
elif "isAvailable" in final_df.columns:
    final_df["Available_Flag"] = final_df["isAvailable"]
else:
    final_df["Available_Flag"] = 1

final_df["Available_Flag"] = final_df["Available_Flag"].astype(int)

# =========================================================
# 🔗 MERGE STORE DETAILS
# =========================================================

final_df["branch"] = final_df["branch"].astype(str)

final_df = final_df.merge(
    help_df,
    left_on="branch",
    right_on="branchCode",
    how="left"
)

# =========================================================
# 📊 SUMMARY
# =========================================================

summary = final_df.groupby(
    ["branch", "Store_Name", "Ownership"]
).agg(
    Total_Items=("Available_Flag", "count"),
    Available_Items=("Available_Flag", "sum")
).reset_index()

summary["Out_of_Stock"] = summary["Total_Items"] - summary["Available_Items"]

summary["Availability %"] = (
    summary["Available_Items"] / summary["Total_Items"].replace(0, 1)
) * 100

summary["Run_Time"] = now.strftime("%Y-%m-%d %H:%M")

summary = summary.round(2)

print("✅ Availability Calculated")

# =========================================================
# 📤 PUSH
# =========================================================

def push(df):
    try:
        ws.clear()
        ws.update(
            [df.columns.tolist()] +
            df.astype(str).values.tolist()
        )
        print("✅ Sheet Updated")

    except Exception as e:
        print("❌ Sheet Error:", e)

push(summary)

print("🚀 Availability Report Completed Successfully")
