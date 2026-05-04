import pandas as pd
import requests
from datetime import datetime, timedelta
import gspread
from oauth2client.service_account import ServiceAccountCredentials

# =========================================================
# 🔐 AUTH (UPDATE YOUR FILE)
# =========================================================

def headers():
    return {
        "Content-Type": "application/json",
        "api_key": "YOUR_API_KEY"
    }

# Google Sheet Auth
scope = ["https://spreadsheets.google.com/feeds",
         "https://www.googleapis.com/auth/drive"]

creds = ServiceAccountCredentials.from_json_keyfile_name(
    "service_account.json", scope
)
client = gspread.authorize(creds)

spreadsheet = client.open("Rista Item Availability")  # NEW SHEET

# =========================================================
# ⏰ TIME
# =========================================================

now = datetime.utcnow() + timedelta(hours=5, minutes=30)

print("⏰ Run Time:", now)

today = now.strftime("%Y-%m-%d")

# =========================================================
# 🏪 FETCH BRANCH (COCO ONLY)
# =========================================================

b_resp = requests.get(
    "https://api.ristaapps.com/v1/branch/list",
    headers=headers()
)

branches_data = b_resp.json().get("data", [])

branch_df = pd.DataFrame(branches_data)

# 🔥 COCO FILTER (IMPORTANT)
coco_branches = branch_df[
    (branch_df["status"] == "Active") &
    (branch_df["ownership"] == "COCO")
]

branches = coco_branches["branchCode"].tolist()

print("🏪 COCO Branch Count:", len(branches))

# =========================================================
# 📦 FETCH ITEM AVAILABILITY
# =========================================================

def fetch_availability(branch):
    try:
        r = requests.get(
            "https://api.ristaapps.com/v1/inventory/itemAvailability",
            headers=headers(),
            params={"branch": branch},
            timeout=20
        )

        if r.status_code != 200:
            return pd.DataFrame()

        data = r.json().get("data", [])

        if not data:
            return pd.DataFrame()

        df = pd.json_normalize(data)

        df["branchCode"] = branch
        df["timestamp"] = now

        return df

    except Exception as e:
        print(f"❌ Error for branch {branch}: {e}")
        return pd.DataFrame()

# =========================================================
# ⚡ PARALLEL FETCH (FAST)
# =========================================================

from concurrent.futures import ThreadPoolExecutor

frames = []

with ThreadPoolExecutor(max_workers=10) as executor:
    results = executor.map(fetch_availability, branches)

for res in results:
    if not res.empty:
        frames.append(res)

availability_df = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()

# =========================================================
# 🧠 PROCESS DATA
# =========================================================

if availability_df.empty:
    print("❌ No availability data")
    exit()

# Normalize fields (based on API structure)
availability_df["isAvailable"] = availability_df["isAvailable"].fillna(False)

# Summary per store
summary = availability_df.groupby("branchCode").agg(
    Total_Items=("itemName", "count"),
    Available_Items=("isAvailable", "sum")
).reset_index()

summary["Availability %"] = (
    summary["Available_Items"] / summary["Total_Items"] * 100
).round(2)

summary["timestamp"] = now

# =========================================================
# 📊 ITEM LEVEL (OPTIONAL DETAIL)
# =========================================================

item_level = availability_df[[
    "branchCode",
    "itemName",
    "isAvailable"
]].copy()

item_level["timestamp"] = now

# =========================================================
# 📤 PUSH TO GOOGLE SHEETS
# =========================================================

def push(sheet_name, df):
    try:
        try:
            ws = spreadsheet.worksheet(sheet_name)
            ws.clear()
        except:
            ws = spreadsheet.add_worksheet(title=sheet_name, rows="1000", cols="20")

        ws.update([df.columns.tolist()] + df.astype(str).values.tolist())

        print(f"✅ Pushed: {sheet_name}")

    except Exception as e:
        print(f"❌ Push Error ({sheet_name}):", e)

# Push both
push("Summary", summary)
push("Item_Level", item_level)

print("🚀 Availability Report Completed")
