# =========================================================
# 🔥 IMPORTS
# =========================================================

import pandas as pd
import requests
import time
import json
from datetime import datetime, timedelta
import pytz
import gspread
from google.oauth2.service_account import Credentials

# =========================================================
# 🔐 CONFIG
# =========================================================

SHEET_NAME = "Rista_Availability_Report"
WORKSHEET_NAME = "Hourly_Availability"

# =========================================================
# 🔐 GOOGLE SHEETS AUTH
# =========================================================

scope = ["https://www.googleapis.com/auth/spreadsheets"]

creds_dict = json.loads(os.environ["GOOGLE_CREDENTIALS"])

creds = Credentials.from_service_account_info(
    creds_dict, scopes=scope
)
client = gspread.authorize(creds)

spreadsheet = client.open(SHEET_NAME)

try:
    ws = spreadsheet.worksheet(WORKSHEET_NAME)
except:
    ws = spreadsheet.add_worksheet(title=WORKSHEET_NAME, rows=1000, cols=50)

# =========================================================
# 🔐 HEADERS FUNCTION (RISTA AUTH)
# =========================================================

def headers():
    return {
        "Content-Type": "application/json",
        "X-Api-Key": "YOUR_API_KEY"
    }

# =========================================================
# ⏰ TIME (IST)
# =========================================================

ist = pytz.timezone("Asia/Kolkata")
now = datetime.now(ist)

print("⏰ Run Time:", now)

# =========================================================
# 🏪 FETCH COCO STORES
# =========================================================

def fetch_branches():
    try:
        r = requests.get(
            "https://api.ristaapps.com/v1/branch/list",
            headers=headers(),
            timeout=30
        )

        data = r.json().get("data", [])

        df = pd.DataFrame(data)

        # 👉 FILTER COCO ONLY
        df = df[
            (df["status"] == "Active") &
            (df["ownership"] == "COCO")
        ]

        print("🏪 COCO Stores:", len(df))

        return df["branchCode"].tolist()

    except Exception as e:
        print("❌ Branch Fetch Error:", e)
        return []

# =========================================================
# 🍽️ FETCH ITEM AVAILABILITY
# =========================================================

def fetch_availability(branch):
    try:
        r = requests.get(
            "https://api.ristaapps.com/v1/menu/items",
            headers=headers(),
            params={"branch": branch},
            timeout=30
        )

        if r.status_code != 200:
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

branches = fetch_branches()

all_data = []

for b in branches:
    df = fetch_availability(b)
    if not df.empty:
        all_data.append(df)

    time.sleep(0.2)  # prevent rate limit

if not all_data:
    print("❌ No availability data")
    exit()

final_df = pd.concat(all_data, ignore_index=True)

print("✅ Data Fetched:", final_df.shape)

# =========================================================
# 🧠 CLEAN DATA
# =========================================================

# 👉 Standardize availability column
if "available" in final_df.columns:
    final_df["Available_Flag"] = final_df["available"]
elif "isAvailable" in final_df.columns:
    final_df["Available_Flag"] = final_df["isAvailable"]
else:
    final_df["Available_Flag"] = 1  # fallback

final_df["Available_Flag"] = final_df["Available_Flag"].astype(int)

# =========================================================
# 📊 BUILD AVAILABILITY METRICS
# =========================================================

summary = final_df.groupby("branch").agg(
    Total_Items=("Available_Flag", "count"),
    Available_Items=("Available_Flag", "sum")
).reset_index()

summary["Out_of_Stock"] = (
    summary["Total_Items"] - summary["Available_Items"]
)

summary["Availability %"] = (
    summary["Available_Items"] / summary["Total_Items"].replace(0,1)
) * 100

# Add timestamp
summary["Run_Time"] = now.strftime("%Y-%m-%d %H:%M")

summary = summary.round(2)

print("✅ Availability Calculated")

# =========================================================
# 📤 PUSH TO GOOGLE SHEETS
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

# =========================================================
# ✅ DONE
# =========================================================

print("🚀 Availability Report Completed Successfully")
