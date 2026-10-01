# =========================================================
# RISTA HISTORICAL BACKFILL & DAILY INCREMENTAL SYNC
# =========================================================
# Purpose:
#   Fetch historical & daily Rista sales, apply Business Hours
#   rollover (08:00 AM to Next Day 05:30 AM), and create/update
#   standardized CSV files under monthly_data/YYYY/
#
# Output format is the exact 18-column structure:
# Brand Name | Date | Week | Branch | Source | Session |
# Store Type | Region | Net Sales | Discount | Taxes |
# Gross Sales | Quantity | Orders | Dis % | AOV |
# AOV Bucket | Discount Bucket
# =========================================================

from pathlib import Path
from datetime import datetime, date, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
import json
import os
import time

import jwt
import numpy as np
import pandas as pd
import requests
import gspread

from google.oauth2.service_account import Credentials


# =========================================================
# CONFIGURATION
# =========================================================

SPREADSHEET_ID = "1g4vuRZPy7qsUvDzF5yYM60VKWTL2r0VSDvtvNl06hiY"
HELP_SHEET_NAME = "Region_Help_Sheet"

API_KEY = os.getenv("API_KEY")
SECRET_KEY = os.getenv("SECRET_KEY")

if not API_KEY:
    raise RuntimeError("❌ API_KEY environment variable is missing")

if not SECRET_KEY:
    raise RuntimeError("❌ SECRET_KEY environment variable is missing")

# Keep concurrency below the daily script to reduce 429 risk.
MAX_WORKERS = 8

# Retry configuration for 429 / temporary server errors.
MAX_RETRIES = 5
RETRY_BASE_SECONDS = 2


# =========================================================
# START LOGGING
# =========================================================

print("=" * 80)
print("🚀 RISTA HISTORICAL BACKFILL & DAILY SYNC STARTED")
print("=" * 80)

print("API KEY EXISTS   :", bool(API_KEY))
print("SECRET KEY EXISTS:", bool(SECRET_KEY))
print("Workers          :", MAX_WORKERS)


# =========================================================
# RISTA AUTH
# =========================================================

def get_token():
    payload = {
        "iss": API_KEY,
        "iat": int(time.time())
    }

    return jwt.encode(
        payload,
        SECRET_KEY,
        algorithm="HS256"
    )


def get_headers():
    return {
        "x-api-key": API_KEY,
        "x-api-token": get_token(),
        "content-type": "application/json"
    }


# =========================================================
# GOOGLE AUTH
# =========================================================

print("\n📡 Connecting to Google...")

google_credentials = os.environ.get("GOOGLE_CREDENTIALS")

if not google_credentials:
    raise RuntimeError(
        "❌ GOOGLE_CREDENTIALS environment variable is missing"
    )

creds_dict = json.loads(google_credentials)

scope = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive"
]

creds = Credentials.from_service_account_info(
    creds_dict,
    scopes=scope
)

client = gspread.authorize(creds)

print("✅ Connected to Google")


# =========================================================
# HELP SHEET
# =========================================================

print("\n📥 Loading Region_Help_Sheet...")

help_ws = client.open_by_key(
    SPREADSHEET_ID
).worksheet(
    HELP_SHEET_NAME
)

help_values = help_ws.get_all_values()

if not help_values:
    raise RuntimeError(
        "❌ Region_Help_Sheet is empty"
    )

help_headers = help_values[0]
help_rows = help_values[1:]

help_master = pd.DataFrame(
    help_rows,
    columns=help_headers
)

required_help_columns = [
    "Branch",
    "Store Type",
    "Region",
    "Channel",
    "Source Group",
    "Brand"
]

missing_help = [
    col
    for col in required_help_columns
    if col not in help_master.columns
]

if missing_help:
    raise RuntimeError(
        "❌ Missing columns in Region_Help_Sheet: "
        + ", ".join(missing_help)
    )

print("✅ Help sheet loaded")
print("Help Rows:", len(help_master))


# =========================================================
# BRANCH MAPPING
# =========================================================

storetype_map = dict(
    zip(
        help_master["Branch"].astype(str).str.strip(),
        help_master["Store Type"]
    )
)

region_map = dict(
    zip(
        help_master["Branch"].astype(str).str.strip(),
        help_master["Region"]
    )
)


# =========================================================
# SOURCE / BRAND MAPPING
# =========================================================

source_master = help_master[
    ["Channel", "Source Group", "Brand"]
].copy()

source_master["Channel"] = (
    source_master["Channel"]
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


# =========================================================
# FETCH BRANCHES
# =========================================================

print("\n📥 Fetching Rista branches...")

branch_url = "https://api.ristaapps.com/v1/branch/list"

branch_response = requests.get(
    branch_url,
    headers=get_headers(),
    timeout=60
)

print(
    "Branch API Status:",
    branch_response.status_code
)

branch_response.raise_for_status()

branch_json = branch_response.json()

if isinstance(branch_json, dict):

    if isinstance(branch_json.get("data"), list):
        branch_data = branch_json["data"]

    elif isinstance(branch_json.get("branches"), list):
        branch_data = branch_json["branches"]

    else:
        branch_data = []

elif isinstance(branch_json, list):

    branch_data = branch_json

else:

    branch_data = []


branches = []

for branch in branch_data:

    branch_code = (
        branch.get("branchCode")
        or branch.get("code")
        or branch.get("id")
    )

    if branch_code:
        branches.append(
            str(branch_code)
        )


branches = list(
    dict.fromkeys(branches)
)

if not branches:
    raise RuntimeError(
        "❌ No branches returned from Rista"
    )

print("🏪 Branch Count:", len(branches))
print("🏪 Sample:", branches[:10])


# =========================================================
# FETCH ONE BRANCH / ONE DAY
# =========================================================

def fetch_branch_day(branch, day):

    all_data = []
    last_key = None

    for attempt in range(MAX_RETRIES):

        try:

            while True:

                params = {
                    "branch": branch,
                    "day": day
                }

                if last_key:
                    params["lastKey"] = last_key

                response = requests.get(
                    "https://api.ristaapps.com/v1/sales/summary",
                    headers=get_headers(),
                    params=params,
                    timeout=60
                )

                status = response.status_code

                # -----------------------------------------
                # Temporary API throttling / server error
                # -----------------------------------------

                if status == 429 or status >= 500:

                    raise RuntimeError(
                        f"TEMP_API_ERROR:{status}"
                    )

                # -----------------------------------------
                # Other API errors
                # -----------------------------------------

                if status != 200:

                    print(
                        f"⚠️ API Failed | "
                        f"{branch} | {day} | HTTP {status}"
                    )

                    return pd.DataFrame()

                payload = response.json()

                data = payload.get(
                    "data",
                    []
                )

                if not data:
                    break

                all_data.append(
                    pd.json_normalize(data)
                )

                last_key = payload.get(
                    "lastKey"
                )

                if not last_key:
                    break

            if all_data:

                return pd.concat(
                    all_data,
                    ignore_index=True
                )

            return pd.DataFrame()

        except RuntimeError as exc:

            if not str(exc).startswith(
                "TEMP_API_ERROR:"
            ):
                print(
                    f"⚠️ Branch error | "
                    f"{branch} | {day} | {exc}"
                )
                return pd.DataFrame()

            wait_seconds = (
                RETRY_BASE_SECONDS
                * (2 ** attempt)
            )

            print(
                f"⏳ Retry {attempt + 1}/{MAX_RETRIES} | "
                f"{branch} | {day} | "
                f"waiting {wait_seconds}s"
            )

            time.sleep(
                wait_seconds
            )

        except requests.RequestException as exc:

            if attempt >= MAX_RETRIES - 1:

                print(
                    f"❌ Request failed | "
                    f"{branch} | {day} | {exc}"
                )

                return pd.DataFrame()

            wait_seconds = (
                RETRY_BASE_SECONDS
                * (2 ** attempt)
            )

            print(
                f"⏳ Network retry "
                f"{attempt + 1}/{MAX_RETRIES} | "
                f"{branch} | {day} | "
                f"waiting {wait_seconds}s"
            )

            time.sleep(
                wait_seconds
            )

        except Exception as exc:

            print(
                f"❌ Unexpected error | "
                f"{branch} | {day} | {exc}"
            )

            return pd.DataFrame()

    return pd.DataFrame()


# =========================================================
# FETCH ONE DAY
# =========================================================

def fetch_day(day):

    day_string = day.strftime(
        "%Y-%m-%d"
    )

    print(
        f"      📥 Fetching {day_string}"
    )

    results = []

    with ThreadPoolExecutor(
        max_workers=MAX_WORKERS
    ) as executor:

        futures = {
            executor.submit(
                fetch_branch_day,
                branch,
                day_string
            ): branch
            for branch in branches
        }

        completed = 0

        for future in as_completed(
            futures
        ):

            branch = futures[future]
            completed += 1

            try:

                df = future.result()

                if df is not None and not df.empty:
                    results.append(df)

            except Exception as exc:

                print(
                    f"❌ Future failed | "
                    f"{branch} | "
                    f"{day_string} | {exc}"
                )

            if completed % 50 == 0:

                print(
                    f"         Progress: "
                    f"{completed}/{len(branches)}"
                )

    if results:

        day_df = pd.concat(
            results,
            ignore_index=True
        )

        print(
            f"      ✅ {day_string}: "
            f"{len(day_df)} raw rows"
        )

        return day_df

    print(
        f"      ⚠️ {day_string}: "
        f"no rows"
    )

    return pd.DataFrame()


# =========================================================
# BUILD STANDARDIZED DATA
# =========================================================

def standardize_month(raw_df):

    if raw_df.empty:
        return pd.DataFrame()

    df = raw_df.copy()

    # -----------------------------------------------------
    # Basic mappings
    # -----------------------------------------------------

    if "branchName" not in df.columns:
        df["branchName"] = ""

    if "channel" not in df.columns:
        df["channel"] = ""

    if "sessionLabel" not in df.columns:
        df["sessionLabel"] = ""

    if "status" not in df.columns:
        df["status"] = ""

    df["Branch"] = (
        df["branchName"]
        .astype(str)
        .str.strip()
    )

    df["Channel Clean"] = (
        df["channel"]
        .astype(str)
        .str.upper()
        .str.strip()
    )

    df["Store Type"] = (
        df["Branch"]
        .map(storetype_map)
    )

    df["Region"] = (
        df["Branch"]
        .map(region_map)
    )

    df["Source"] = (
        df["Channel Clean"]
        .map(source_map)
    )

    df["Brand Name"] = (
        df["Channel Clean"]
        .map(brand_map)
    )

    df["Source"] = (
        df["Source"]
        .replace(
            [
                "Magicpin",
                "HOGR",
                "Website"
            ],
            "Others"
        )
    )

    df["Session"] = (
        df["sessionLabel"]
    )

    # -----------------------------------------------------
    # Required numeric columns
    # -----------------------------------------------------

    numeric_columns = [
        "netAmount",
        "chargeAmount",
        "discountAmount",
        "taxAmount",
        "grossAmount",
        "item_quantity"
    ]

    for col in numeric_columns:

        if col not in df.columns:
            df[col] = 0

        df[col] = pd.to_numeric(
            df[col],
            errors="coerce"
        ).fillna(0)

    df["discountAmount"] = (
        df["discountAmount"]
        .abs()
    )

    # -----------------------------------------------------
    # Closed bills only
    # -----------------------------------------------------

    df = df[
        df["status"]
        .astype(str)
        .str.upper()
        .eq("CLOSED")
    ].copy()

    if df.empty:
        return pd.DataFrame()

    # -----------------------------------------------------
    # Net Sales
    # -----------------------------------------------------

    df["Net Sales"] = (
        df["netAmount"]
        +
        df["chargeAmount"]
    )

    # -----------------------------------------------------
    # Invoice Date
    # -----------------------------------------------------

    df["invoiceDate"] = pd.to_datetime(
        df["invoiceDate"],
        errors="coerce"
    )

    df = df[
        df["invoiceDate"].notna()
    ].copy()

    # -----------------------------------------------------
    # Business Hours Rollover: 08:00 AM to Next Day 05:30 AM
    #
    # Transactions at or before 5:30 AM belong to the
    # previous calendar day's Business Date.
    # -----------------------------------------------------

    df["Hour"] = (
        df["invoiceDate"]
        .dt.hour
    )

    df["Minute"] = (
        df["invoiceDate"]
        .dt.minute
    )

    df["Business Date"] = (
        df["invoiceDate"]
        .dt.normalize()
    )

    previous_day_mask = (
        (df["Hour"] < 5)
        |
        (
            (df["Hour"] == 5)
            &
            (df["Minute"] <= 30)
        )
    )

    df.loc[
        previous_day_mask,
        "Business Date"
    ] = (
        df.loc[
            previous_day_mask,
            "Business Date"
        ]
        - pd.Timedelta(days=1)
    )

    df["Date"] = pd.to_datetime(
        df["Business Date"]
    )

    # -----------------------------------------------------
    # Week
    # -----------------------------------------------------

    df["Week"] = (
        "WK "
        + df["Date"]
        .dt.isocalendar()
        .week
        .astype(str)
    )

    # -----------------------------------------------------
    # Orders
    # -----------------------------------------------------

    if "invoiceNumber" in df.columns:

        df["Orders"] = (
            df["invoiceNumber"]
            .astype(str)
            .nunique()
        )

    # -----------------------------------------------------
    # Group to standard format
    # -----------------------------------------------------

    group_columns = [
        "Brand Name",
        "Date",
        "Week",
        "Branch",
        "Source",
        "Session",
        "Store Type",
        "Region"
    ]

    if "invoiceNumber" in df.columns:

        summary = (
            df.groupby(
                group_columns,
                dropna=False
            )
            .agg(
                **{
                    "Net Sales": (
                        "Net Sales",
                        "sum"
                    ),
                    "Discount": (
                        "discountAmount",
                        "sum"
                    ),
                    "Taxes": (
                        "taxAmount",
                        "sum"
                    ),
                    "Gross Sales": (
                        "grossAmount",
                        "sum"
                    ),
                    "Quantity": (
                        "item_quantity",
                        "sum"
                    ),
                    "Orders": (
                        "invoiceNumber",
                        "nunique"
                    )
                }
            )
            .reset_index()
        )

    else:

        summary = (
            df.groupby(
                group_columns,
                dropna=False
            )
            .agg(
                **{
                    "Net Sales": (
                        "Net Sales",
                        "sum"
                    ),
                    "Discount": (
                        "discountAmount",
                        "sum"
                    ),
                    "Taxes": (
                        "taxAmount",
                        "sum"
                    ),
                    "Gross Sales": (
                        "grossAmount",
                        "sum"
                    ),
                    "Quantity": (
                        "item_quantity",
                        "sum"
                    ),
                    "Orders": (
                        "Orders",
                        "sum"
                    )
                }
            )
            .reset_index()
        )

    # -----------------------------------------------------
    # Numeric fields
    # -----------------------------------------------------

    for col in [
        "Net Sales",
        "Discount",
        "Taxes",
        "Gross Sales",
        "Quantity",
        "Orders"
    ]:

        summary[col] = pd.to_numeric(
            summary[col],
            errors="coerce"
        ).fillna(0)

    # -----------------------------------------------------
    # AOV / Discount %
    # -----------------------------------------------------

    summary["Dis %"] = (
        summary["Discount"]
        /
        summary["Gross Sales"]
        .replace(0, 1)
    ) * 100

    summary["AOV"] = (
        summary["Net Sales"]
        /
        summary["Orders"]
        .replace(0, 1)
    )

    # -----------------------------------------------------
    # Buckets
    # -----------------------------------------------------

    summary["AOV Bucket"] = pd.cut(
        summary["AOV"],
        bins=[
            0,
            100,
            200,
            300,
            400,
            500,
            600,
            900,
            np.inf
        ],
        labels=[
            "0-100",
            "100-200",
            "200-300",
            "300-400",
            "400-500",
            "500-600",
            "600-900",
            ">900"
        ],
        include_lowest=True
    )

    summary["Discount Bucket"] = pd.cut(
        summary["Dis %"],
        bins=[
            -1,
            0,
            10,
            20,
            30,
            40,
            50,
            60,
            70,
            80,
            90,
            100,
            np.inf
        ],
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
            "90%-100%",
            ">100%"
        ],
        include_lowest=True
    ).astype(str)

    # -----------------------------------------------------
    # Exact column order
    # -----------------------------------------------------

    final_columns = [
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
        "Orders",
        "Dis %",
        "AOV",
        "AOV Bucket",
        "Discount Bucket"
    ]

    summary = summary[
        final_columns
    ].copy()

    # -----------------------------------------------------
    # Final cleanup
    # -----------------------------------------------------

    summary["Date"] = pd.to_datetime(
        summary["Date"]
    ).dt.strftime(
        "%Y-%m-%d"
    )

    summary = summary.sort_values(
        [
            "Date",
            "Branch",
            "Brand Name"
        ]
    ).reset_index(
        drop=True
    )

    return summary


# =========================================================
# INCREMENTAL DAILY / MONTHLY BACKFILL ENGINE
# =========================================================

def process_dynamic_range():
    today = date.today()
    yesterday = today - timedelta(days=1)

    print("\n" + "=" * 80)
    print(f"📅 CHECKING DATES TO UPDATE UP TO YESTERDAY ({yesterday})")
    print("=" * 80)

    # Default start month: September 2026
    start_year = 2026
    start_month = 9
    start_date = date(start_year, start_month, 1)

    # Scan existing monthly data to find max fetched date
    month_dir = Path("monthly_data") / str(start_year)
    month_name = start_date.strftime("%b")
    mtd_file = month_dir / f"MTD_{month_name}_{str(start_year)[-2:]}.csv"

    existing_df = pd.DataFrame()

    if mtd_file.exists():
        try:
            existing_df = pd.read_csv(mtd_file, low_memory=False)
            if not existing_df.empty and "Date" in existing_df.columns:
                existing_df["Date_dt"] = pd.to_datetime(existing_df["Date"])
                max_date = existing_df["Date_dt"].max().date()
                print(f"ℹ️ Found existing MTD file. Max recorded date: {max_date}")
                
                # Fetch starting from last available date (rewind 1 day to ensure full close)
                start_date = max_date - timedelta(days=1)
                existing_df = existing_df.drop(columns=["Date_dt"])
        except Exception as err:
            print(f"⚠️ Could not parse existing CSV: {err}")

    if start_date > yesterday:
        print("✅ All data is up to date! No missing days to fetch.")
        return True

    # Generate daily list
    days_to_fetch = []
    curr = start_date
    while curr <= yesterday:
        days_to_fetch.append(curr)
        curr += timedelta(days=1)

    print(f"📥 Dates to Fetch ({len(days_to_fetch)} days): {days_to_fetch[0]} → {days_to_fetch[-1]}")

    monthly_raw = []
    for day in days_to_fetch:
        day_df = fetch_day(day)
        if day_df is not None and not day_df.empty:
            monthly_raw.append(day_df)

    if not monthly_raw:
        print("⚠️ No raw data returned for selected date range.")
        return False

    raw_combined = pd.concat(monthly_raw, ignore_index=True)
    new_summary = standardize_month(raw_combined)

    if new_summary.empty:
        print("⚠️ No CLOSED orders found for selected date range.")
        return False

    # Merge with existing file if available
    if not existing_df.empty:
        combined_df = pd.concat([existing_df, new_summary], ignore_index=True)
        dedup_cols = ["Brand Name", "Date", "Branch", "Source", "Session"]
        final_df = combined_df.drop_duplicates(subset=dedup_cols, keep="last")
    else:
        final_df = new_summary

    final_df = final_df.sort_values(["Date", "Branch", "Brand Name"]).reset_index(drop=True)

    # Ensure output directory exists
    month_dir.mkdir(parents=True, exist_ok=True)
    final_df.to_csv(mtd_file, index=False)

    print("\n" + "=" * 80)
    print("✅ FILE UPDATED SUCCESSFULLY:", mtd_file)
    print("Total Rows:", len(final_df))
    print("Date Range:", final_df["Date"].min(), "→", final_df["Date"].max())
    print("Total Net Sales:", round(pd.to_numeric(final_df["Net Sales"], errors="coerce").sum(), 2))
    print("=" * 80)

    # -----------------------------------------------------
    # ALSO UPDATE LIGHTWEIGHT SUMMARY CSV
    # -----------------------------------------------------
    summary_path = Path("historical_data/historical_sales_summary.csv")
    summary_path.parent.mkdir(parents=True, exist_ok=True)

    summary_cols = ["Branch", "Date", "Source", "Brand Name"]
    new_summary_light = final_df.groupby(summary_cols, as_index=False)["Net Sales"].sum()

    if summary_path.exists():
        try:
            old_summary = pd.read_csv(summary_path)
            full_summary = pd.concat([old_summary, new_summary_light], ignore_index=True)
            full_summary = full_summary.drop_duplicates(subset=summary_cols, keep="last")
        except Exception:
            full_summary = new_summary_light
    else:
        full_summary = new_summary_light

    full_summary.to_csv(summary_path, index=False)
    print(f"✅ Updated lightweight summary CSV at: {summary_path}")

    return True


# =========================================================
# MAIN EXECUTION
# =========================================================

if __name__ == "__main__":
    try:
        success = process_dynamic_range()
        print("\n" + "=" * 80)
        print("🏁 PROCESS COMPLETED SUCCESSFULLY")
        print("=" * 80)
    except Exception as exc:
        print("\n❌ EXECUTION FAILED:", repr(exc))
