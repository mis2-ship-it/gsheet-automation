from datetime import datetime

print("🚀 MTD START")
print("Current Time:", datetime.now())
# =========================================================
# IMPORTS
# =========================================================

import os
import json
import time
import jwt
import requests
import pandas as pd
import numpy as np
import gspread

from datetime import datetime, timedelta
from concurrent.futures import (
    ThreadPoolExecutor,
    as_completed
)
from google.oauth2.service_account import Credentials


print("🚀 MTD Script Started")


# =========================================================
# GOOGLE AUTH
# =========================================================

scope = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive"
]

creds_dict = json.loads(
    os.environ["GOOGLE_CREDENTIALS"]
)

creds = Credentials.from_service_account_info(
    creds_dict,
    scopes=scope
)

client = gspread.authorize(creds)

print("✅ Connected to Google")


# =========================================================
# API CONFIG
# =========================================================

API_KEY = os.getenv("API_KEY")
SECRET_KEY = os.getenv("SECRET_KEY")

def get_token():
    payload = {"iss": API_KEY, "iat": int(time.time())}
    return jwt.encode(payload, SECRET_KEY, algorithm="HS256")

def headers():
    return {
        "x-api-key": API_KEY,
        "x-api-token": get_token(),
        "content-type": "application/json"
    }


print("API KEY EXISTS:", bool(API_KEY))
print("SECRET KEY EXISTS:", bool(SECRET_KEY))

# =========================================================
# DATE RANGE - LAST 2 DAYS ONLY
# =========================================================

today = datetime.now().date()

start_date = today - timedelta(days=2)
end_date = today - timedelta(days=1)

print(f"📅 Date Range: {start_date} → {end_date}")


# =========================================================
# FETCH BRANCHES
# =========================================================

print("📥 Fetching Branches...")

branch_url = "https://api.ristaapps.com/v1/branch/list"

try:

    branch_resp = requests.get(
        branch_url,
        headers=headers(),
        timeout=60
    )

    print("Branch API Status:", branch_resp.status_code)

    branch_json = branch_resp.json()

    
    print("Branch API Sample:")
    print(str(branch_json)[:500])

    # handle multiple response formats
    if isinstance(branch_json, dict):

        if "data" in branch_json:
            branch_data = branch_json["data"]

        elif "branches" in branch_json:
            branch_data = branch_json["branches"]

        else:
            branch_data = []

    elif isinstance(branch_json, list):
        branch_data = branch_json

    else:
        branch_data = []

    branches = []

    for b in branch_data:

        branch_code = (
            b.get("branchCode")
            or b.get("code")
            or b.get("id")
        )

        if branch_code:
            branches.append(str(branch_code))

    print("🏪 Branch Count:", len(branches))
    print("🏪 Sample Branches:", branches[:10])

except Exception as e:

    print("❌ Branch Fetch Failed:", str(e))
    branches = []

# =========================================================
# FETCH BRANCH SALES
# =========================================================

def fetch_branch_data(branch, day):

    all_data = []
    last_key = None

    while True:

        params = {
            "branch": branch,
            "day": day
        }

        if last_key:
            params["lastKey"] = last_key

        try:

            r = requests.get(
                "https://api.ristaapps.com/v1/sales/summary",
                headers=headers(),
                params=params,
                timeout=20
            )

            if r.status_code != 200:

                print(
                    f"❌ API Failed | "
                    f"{branch} | "
                    f"{day} | "
                    f"{r.status_code}"
                )

                return pd.DataFrame()

            js = r.json()

            data = js.get("data", [])

            if not data:
                break

            all_data.append(
                pd.json_normalize(data)
            )

            last_key = js.get("lastKey")

            if not last_key:
                break

        except Exception as e:

            print(
                f"�� Branch Error | "
                f"{branch} | "
                f"{day} | "
                f"{str(e)}"
            )

            return pd.DataFrame()

    return (
        pd.concat(
            all_data,
            ignore_index=True
        )
        if all_data
        else pd.DataFrame()
    )
#-- Fetch Sales----- #

def fetch_sales(day):

    results = []

    with ThreadPoolExecutor(max_workers=10) as executor:

        futures = [
            executor.submit(
                fetch_branch_data,
                b,
                day
            )
            for b in branches
        ]

        for future in as_completed(futures):

            try:
                df = future.result()

                if df is not None:

                    if not df.empty:
                        results.append(df)

            except Exception as e:
                print("Fetch Error:", e)

    return (
        pd.concat(results, ignore_index=True)
        if results
        else pd.DataFrame()
    )


# =========================================================
# FETCH DATA FOR LAST 2 DAYS
# =========================================================

all_days = []

current_day = start_date

while current_day <= end_date:

    print("📥 Fetching:", current_day)

    df = fetch_sales(current_day)

    if not df.empty:
        all_days.append(df)

    current_day += timedelta(days=1)

# ---------------- CHECK ---------------- #

if not all_days:
    raise Exception(
        "No sales data fetched"
    )

# ---------------- FINAL DF ---------------- #

final_df = pd.concat(
    all_days,
    ignore_index=True
)

print("✅ Rows:", len(final_df))


# =========================================================
# HELP SHEET MAPPING
# =========================================================

sheet = client.open_by_key(
    "1g4vuRZPy7qsUvDzF5yYM60VKWTL2r0VSDvtvNl06hiY"
).worksheet("Region_Help_Sheet")

master = pd.DataFrame(
    sheet.get_all_records()
)

storetype_map = dict(
    zip(master["Branch"], master["Store Type"])
)

region_map = dict(
    zip(master["Branch"], master["Region"])
)

final_df["Store Type"] = (
    final_df["branchName"]
    .map(storetype_map)
)

final_df["Region"] = (
    final_df["branchName"]
    .map(region_map)
)


# =========================================================
# SOURCE MAP
# =========================================================

help_sheet = client.open(
    "Sales Dashboard"
).worksheet("Help Sheet")

source_master = pd.DataFrame(
    help_sheet.get("D:F")[1:],
    columns=help_sheet.get("D:F")[0]
)

source_master["Channel"] = (
    source_master["Channel"]
    .astype(str)
    .str.upper()
    .str.strip()
)

final_df["channel"] = (
    final_df["channel"]
    .astype(str)
    .str.upper()
    .str.strip()
)

source_map = dict(
    zip(
        source_master["Channel"],
        source_master["Source Group"]
    )
)

brand_map = dict(
    zip(
        source_master["Channel"],
        source_master["Brand"]
    )
)

final_df["Source"] = (
    final_df["channel"]
    .map(source_map)
)

final_df["Brand Name"] = (
    final_df["channel"]
    .map(brand_map)
)

final_df["Source"] = final_df["Source"].replace(
    [
        "Magicpin",
        "HOGR",
        "Website"
    ],
    "Others"
)


# =========================================================
# SESSION
# =========================================================

final_df["Session"] = (
    final_df["sessionLabel"]
)

# =========================================================
# SAFE COLUMN CHECK
# =========================================================

required_cols = [
    "netAmount",
    "chargeAmount",
    "status",
    "branchName",
    "channel"
]

for col in required_cols:

    if col not in final_df.columns:
        final_df[col] = 0

# =========================================================
# NUMERIC
# =========================================================

required_numeric = [
    "netAmount",
    "chargeAmount",
    "discountAmount",
    "taxAmount",
    "grossAmount",
    "item_quantity"
]

for col in required_numeric:

    # create column if missing
    if col not in final_df.columns:
        final_df[col] = 0

    # numeric conversion
    final_df[col] = pd.to_numeric(
        final_df[col],
        errors="coerce"
    ).fillna(0)

# ---------------- DISCOUNT POSITIVE ---------------- #

final_df["discountAmount"] = (
    final_df["discountAmount"]
    .abs()
)

# =========================================================
# NET SALES
# =========================================================

final_df["Net Sales"] = (
    final_df["netAmount"]
    +
    final_df["chargeAmount"]
)

# =========================================================
# CLOSED BILL FILTER
# =========================================================

final_df = final_df[
    final_df["status"]
    .astype(str)
    .str.upper()
    == "CLOSED"
]

print(
    "✅ Closed Rows:",
    len(final_df)
)

print(
    "💰 Total Net Sales:"
)

print(
    round(
        final_df["Net Sales"].sum(),
        2
    )
)
# =========================================================
# BUSINESS HOUR LOGIC
# 8:00 AM → NEXT DAY 5:30 AM
# =========================================================

final_df["invoiceDate"] = pd.to_datetime(
    final_df["invoiceDate"],
    errors="coerce"
)

# Hour + minute
final_df["Hour"] = (
    final_df["invoiceDate"]
    .dt.hour
)

final_df["Minute"] = (
    final_df["invoiceDate"]
    .dt.minute
)

# Business Date Logic
final_df["Business Date"] = (
    final_df["invoiceDate"]
    .dt.date
)

# Before 5:30 AM belongs to previous day
mask = (
    (final_df["Hour"] < 5)
    |
    (
        (final_df["Hour"] == 5)
        &
        (final_df["Minute"] <= 30)
    )
)

final_df.loc[
    mask,
    "Business Date"
] = (
    pd.to_datetime(
        final_df.loc[
            mask,
            "Business Date"
        ]
    )
    - pd.Timedelta(days=1)
).dt.date

# Convert for reporting
final_df["Date"] = pd.to_datetime(
    final_df["Business Date"]
)

# =========================================================
# WEEK
# =========================================================

final_df["Week"] = (
    "WK "
    + final_df["Date"]
    .dt.isocalendar()
    .week.astype(str)
)

# =========================================================
# SAFE COLUMN FIX
# =========================================================

if "item_quantity" not in final_df.columns:
    final_df["item_quantity"] = 0
    
# =========================================================
# SUMMARY
# =========================================================

mtd_summary = (
    final_df.groupby(
        [
            "Brand Name",
            "Business Date",
            "Week",
            "branchName",
            "Source",
            "Session",
            "Store Type",
            "Region"
        ],
        dropna=False
    )
    .agg({
        "Net Sales": "sum",
        "discountAmount": "sum",
        "taxAmount": "sum",
        "grossAmount": "sum",
        "item_quantity": "sum",
        "invoiceNumber": "nunique"
    })
    .reset_index()
)

mtd_summary.columns = [
    "Brand Name",
    "Date",
    "Week",
    "Branch",
    "Source",
    "Session",
    "Store Type",
    "Region",
    "Net Sales",
    "Discount",
    "Taxes",
    "Gross Sales",
    "Quantity",
    "Orders"
]


# =========================================================
# AOV + DIS%
# =========================================================

mtd_summary["Dis %"] = (
    mtd_summary["Discount"]
    / mtd_summary["Gross Sales"]
    .replace(0, 1)
) * 100

mtd_summary["AOV"] = (
    mtd_summary["Net Sales"]
    / mtd_summary["Orders"]
    .replace(0, 1)
)


# =========================================================
# BUCKETS
# =========================================================

mtd_summary["AOV Bucket"] = pd.cut(
    mtd_summary["AOV"],
    bins=[0,100,200,300,400,500,600,900,999999],
    labels=[
        "0-100",
        "100-200",
        "200-300",
        "300-400",
        "400-500",
        "500-600",
        "600-900",
        ">900"
    ]
)

mtd_summary["Discount Bucket"] = pd.cut(
    mtd_summary["Dis %"],
    bins=[-1,0,10,20,30,40,50,60,70,80,90,100],
    labels=[
        "0%",
        "1%-10%",
        "10%-20%",
        "20%-30%",
        "30%-40%",
        "40%-50%",
        "50%-60%",
        "60%-70%",
        "70%-80%",
        "80%-90%",
        "90%-100%"
    ]
).astype(str)


# =========================================================
# UPDATE GSHEET
# =========================================================

sheet = client.open_by_key(
    "1g4vuRZPy7qsUvDzF5yYM60VKWTL2r0VSDvtvNl06hiY"
).worksheet("MTD_Data")

# ---------------- FORMAT DATE ---------------- #

mtd_summary["Date"] = pd.to_datetime(
    mtd_summary["Date"]
)

# ---------------- READ EXISTING DATA ---------------- #

existing_data = pd.DataFrame(
    sheet.get_all_records()
)

print(
    "Existing Rows:",
    len(existing_data)
)

# ---------------- KEEP OLD DATA (EXCEPT LAST 2 DAYS) ---------------- #

if len(existing_data) > 0:

    existing_data["Date"] = pd.to_datetime(
        existing_data["Date"]
    )

    refresh_from = (
        mtd_summary["Date"].min()
    )

    # Keep data older than the refresh period
    historical_df = existing_data[
        existing_data["Date"]
        < refresh_from
    ].copy()

else:

    historical_df = pd.DataFrame()

# ---------------- FORMAT NEW DATA ---------------- #

mtd_summary["Date"] = (
    mtd_summary["Date"]
    .dt.strftime("%Y-%m-%d")
)

mtd_summary["AOV Bucket"] = (
    mtd_summary["AOV Bucket"]
    .astype(str)
)

mtd_summary["Discount Bucket"] = (
    mtd_summary["Discount Bucket"]
    .astype(str)
)

# ---------------- MERGE OLD + NEW (LAST 2 DAYS) ---------------- #

final_upload_df = pd.concat(
    [
        historical_df,
        mtd_summary
    ],
    ignore_index=True
)

print(
    "Final Upload Rows:",
    len(final_upload_df)
)

# ---------------- UPDATE SHEET ---------------- #

sheet.clear()

sheet.update(
    [final_upload_df.columns.tolist()]
    +
    final_upload_df.replace(
        [np.nan, "nan"],
        ""
    ).values.tolist()
)

print(
    "✅ Last 2 Days Data Update Completed"
)

print("✅ MTD END")
print("End Time:", datetime.now())
