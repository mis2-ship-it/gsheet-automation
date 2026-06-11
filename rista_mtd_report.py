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
# DATE RANGE
# =========================================================

today = datetime.now().date()

start_date = today.replace(day=1)
end_date = today - timedelta(days=1)

print(f"📅 MTD Range: {start_date} → {end_date}")


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

    try:

        payload = {
            "branchCode": branch,
            "fromDate": str(day),
            "toDate": str(day),
            "page": 1,
            "pageSize": 10000
        }

        r = requests.get(
            "https://api.ristaapps.com/v1/sales/summary",
            headers=headers(),
            params={
                "branch": branch,
                "day": day
            },
            timeout=20
        )

        data = r.json()

        # ---------------- DEBUG ---------------- #

        print(
            f"Branch: {branch} | "
            f"Date: {day} | "
            f"Status: {r.status_code}"
        )

        print(str(data)[:200])

        # ---------------- RESPONSE ---------------- #

        if isinstance(data, dict):

            rows = (
                data.get("data")
                or data.get("orders")
                or []
            )

        elif isinstance(data, list):

            rows = data

        else:
            rows = []

        if not rows:
            return pd.DataFrame()

        df = pd.DataFrame(rows)

        df["businessDate"] = str(day)

        return df

    except Exception as e:

        print(
            f"❌ Error Branch {branch} "
            f"Date {day}: {e}"
        )

        return pd.DataFrame()


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

                    print(
                        f"Fetched rows for {day}:",
                        len(df)
                    )

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
# FETCH MTD DATA
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
# NET SALES
# =========================================================

final_df["netAmount"] = pd.to_numeric(
    final_df["netAmount"],
    errors="coerce"
).fillna(0)

final_df["chargeAmount"] = pd.to_numeric(
    final_df["chargeAmount"],
    errors="coerce"
).fillna(0)

final_df["Net Sales"] = (
    (
        final_df["netAmount"]
        + final_df["chargeAmount"]
    )
    .where(
        final_df["status"] == "Closed",
        0
    )
)
# =========================================================
# NUMERIC
# =========================================================

numeric_cols = [
    "netAmount",
    "discountAmount",
    "taxAmount",
    "grossAmount"
]

for col in numeric_cols:

    if col in final_df.columns:

        final_df[col] = pd.to_numeric(
            final_df[col],
            errors="coerce"
        ).fillna(0)


final_df["discountAmount"] = (
    final_df["discountAmount"]
    .abs()
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
            "businessDate",
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

sheet.clear()

mtd_summary["Date"] = (
    pd.to_datetime(mtd_summary["Date"])
    .dt.strftime("%Y-%m-%d")
)

# Convert categorical columns to string
mtd_summary["AOV Bucket"] = (
    mtd_summary["AOV Bucket"]
    .astype(str)
)

mtd_summary["Discount Bucket"] = (
    mtd_summary["Discount Bucket"]
    .astype(str)
)

sheet.update(
    [mtd_summary.columns.tolist()]
    + mtd_summary.replace(
        [np.nan, "nan"],
        ""
    ).values.tolist()
)

print("✅ MTD Update Completed")
