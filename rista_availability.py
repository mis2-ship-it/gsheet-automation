# =========================================================
# 🔥 IMPORTS
# =========================================================

import pandas as pd
import requests
import time
import json
import os
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

scope = [
"https://www.googleapis.com/auth/spreadsheets",
"https://www.googleapis.com/auth/drive"
]

# 👉 Load from GitHub Secret
creds_dict = json.loads(os.environ["GOOGLE_CREDENTIALS"])

creds = Credentials.from_service_account_info(
creds_dict, scopes=scope
)

client = gspread.authorize(creds)

# Open spreadsheet
spreadsheet = client.open(SHEET_NAME)

# Open or create worksheet
try:
ws = spreadsheet.worksheet(WORKSHEET_NAME)
except:
ws = spreadsheet.add_worksheet(
    title=WORKSHEET_NAME, rows=1000, cols=50
)

help_ws = spreadsheet.worksheet("Help_Sheet")

help_df = pd.DataFrame(help_ws.get_all_records())

print("✅ Help Sheet Loaded:", help_df.shape)

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

# ---------------- FETCH BRANCH ---------------- #

def fetch_branches():
try:
    b_resp = requests.get(
        "https://api.ristaapps.com/v1/branch/list",
        headers=headers(),
        timeout=30
    )

    print("Status Code:", b_resp.status_code)

    data = b_resp.json()
    data = data.get("data", []) if isinstance(data, dict) else data

    if not data:
        print("❌ No branches from API")
        return []

    df = pd.DataFrame(data)

    # 👉 Clean columns
    df.columns = df.columns.str.strip()
    help_df.columns = help_df.columns.str.strip()

    # 👉 Ensure type match
    df["branchCode"] = df["branchCode"].astype(str)
    help_df["branchCode"] = help_df["branchCode"].astype(str)

    # =====================================================
    # 🔗 MERGE (CORRECTLY INDENTED)
    # =====================================================
    merged = df.merge(
        help_df,
        on="branchCode",
        how="left"
    )

    # 👉 Handle missing ownership
    merged["Ownership"] = merged["Ownership"].fillna("UNKNOWN")

    # 👉 Filter COCO
    merged = merged[
        merged["Ownership"].str.upper() == "COCO"
    ]

    print("🏪 COCO Branch count:", len(merged))

    return merged["branchCode"].tolist()

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
# 🔗 MERGE STORE DETAILS
# =========================================================

final_df = final_df.merge(
help_df,
on="branchName",
how="left"
)

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
