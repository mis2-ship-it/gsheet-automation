# =========================================================
# RISTA HISTORICAL BACKFILL
# =========================================================
# Purpose:
#   Fetch historical Rista sales and create standardized monthly
#   CSV files for:
#       2025-01 through 2025-12
#       2026-01 through 2026-06
#
# Existing files such as:
#       MTD_July_26.csv
#       MTD_Aug_26.csv
# are NOT touched.
#
# Output format is the same 14-column structure used by the
# corrected rista_mtd_report.py:
#
# Brand Name | Date | Week | Branch | Source | Session |
# Store Type | Region | Net Sales | Discount | Taxes |
# Gross Sales | Quantity | Orders
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
# MONTHS TO FETCH
# =========================================================
#
# Add only the months that you want to fetch/rebuild.
#
# Examples:
#
# (2026, 7)       = July 2026
# (2026, 8)       = August 2026
# (2026, 9)       = September 2026
#
# Multiple months:
# [(2026, 7), (2026, 8), (2026, 9)]
#
# Full 2025:
# [(2025, month) for month in range(1, 13)]
#
# =========================================================

BACKFILL_MONTHS = [
    (2026, 7),
]


# =========================================================
# START
# =========================================================

print("=" * 80)
print("🚀 RISTA HISTORICAL BACKFILL STARTED")
print("=" * 80)

print("API KEY EXISTS   :", bool(API_KEY))
print("SECRET KEY EXISTS:", bool(SECRET_KEY))
print("Workers          :", MAX_WORKERS)

print("\nMonths to create:")

for year, month in BACKFILL_MONTHS:
    print(f"  - {year}-{month:02d}")


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
# BUILD STANDARDIZED MONTH DATA
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
    # Business Date
    #
    # Same business-date logic as current MTD script:
    # before / at 5:30 AM belongs to previous business day.
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
    # Group to same standard format
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

    # invoiceNumber may not exist in unusual API responses.
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
# MONTH DATE RANGE
# =========================================================

def month_dates(year, month):

    first_day = date(
        year,
        month,
        1
    )

    if month == 12:

        next_month = date(
            year + 1,
            1,
            1
        )

    else:

        next_month = date(
            year,
            month + 1,
            1
        )

    last_day = (
        next_month
        - timedelta(days=1)
    )

    current = first_day

    while current <= last_day:

        yield current

        current += timedelta(
            days=1
        )


# =========================================================
# PROCESS ONE MONTH
# =========================================================

def process_month(year, month):

    month_name = datetime(
        year,
        month,
        1
    ).strftime("%b")

    output_dir = (
        Path("monthly_data")
        / str(year)
    )

    output_dir.mkdir(
        parents=True,
        exist_ok=True
    )

    output_file = (
        output_dir
        / f"MTD_{month_name}_{str(year)[-2:]}.csv"
    )

    print("\n" + "=" * 80)
    print(
        f"📅 PROCESSING {month_name.upper()} {year}"
    )
    print(
        f"📄 OUTPUT: {output_file}"
    )
    print("=" * 80)

    # -----------------------------------------------------
    # Fetch a one-day buffer on both sides.
    #
    # Business day is:
    #   08:00 AM -> next day 05:30 AM
    #
    # Therefore:
    #   - Month-start business data can contain
    #     transactions from the previous calendar day
    #     between 00:00 and 05:30.
    #   - Month-end business data can contain
    #     transactions from the next calendar day
    #     between 00:00 and 05:30.
    #
    # Example:
    #   Aug-31 business day requires Sep-01 00:00-05:30.
    # -----------------------------------------------------

    monthly_raw = []

    first_day = date(
        year,
        month,
        1
    )

    if month == 12:
        next_month_first = date(
            year + 1,
            1,
            1
        )
    else:
        next_month_first = date(
            year,
            month + 1,
            1
        )

    last_day = (
        next_month_first
        - timedelta(days=1)
    )

    fetch_start = (
        first_day
        - timedelta(days=1)
    )

    fetch_end = (
        last_day
        + timedelta(days=1)
    )

    days = []

    current = fetch_start

    while current <= fetch_end:
        days.append(current)
        current += timedelta(days=1)

    print(
        "Business Month :",
        first_day,
        "→",
        last_day
    )

    print(
        "API Fetch Range:",
        fetch_start,
        "→",
        fetch_end
    )

    print(
        "Days to fetch:",
        len(days)
    )

    for day_index, day in enumerate(
        days,
        start=1
    ):

        print(
            f"\n[{day_index}/{len(days)}] "
            f"{day}"
        )

        day_df = fetch_day(
            day
        )

        if (
            day_df is not None
            and not day_df.empty
        ):

            monthly_raw.append(
                day_df
            )

    if not monthly_raw:

        print(
            f"⚠️ No data returned for "
            f"{month_name} {year}"
        )

        return False

    raw_month_df = pd.concat(
        monthly_raw,
        ignore_index=True
    )

    print(
        "\n📦 Raw monthly rows:",
        len(raw_month_df)
    )

    # -----------------------------------------------------
    # Standardize
    # -----------------------------------------------------

    monthly_df = standardize_month(
        raw_month_df
    )

    if monthly_df.empty:

        print(
            f"⚠️ No CLOSED data for "
            f"{month_name} {year}"
        )

        return False

    # -----------------------------------------------------
    # Keep only business dates belonging to this month.
    #
    # This prevents the 00:00–05:30 business-date rollover
    # from placing a transaction into the previous month file
    # incorrectly.
    # -----------------------------------------------------

    monthly_df["_Date"] = pd.to_datetime(
        monthly_df["Date"]
    )

    monthly_df = monthly_df[
        (
            monthly_df["_Date"].dt.year
            == year
        )
        &
        (
            monthly_df["_Date"].dt.month
            == month
        )
    ].copy()

    monthly_df = monthly_df.drop(
        columns="_Date"
    )

    # -----------------------------------------------------
    # Remove duplicates
    # -----------------------------------------------------

    monthly_df = (
        monthly_df
        .drop_duplicates()
        .reset_index(drop=True)
    )

    # -----------------------------------------------------
    # Save
    # -----------------------------------------------------

    monthly_df.to_csv(
        output_file,
        index=False
    )

    # -----------------------------------------------------
    # Verification
    # -----------------------------------------------------

    check_df = pd.read_csv(
        output_file,
        low_memory=False
    )

    expected_columns = [
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

    if check_df.columns.tolist() != expected_columns:

        raise RuntimeError(
            f"❌ Column structure mismatch in "
            f"{output_file}\n"
            f"Expected: {expected_columns}\n"
            f"Actual: {check_df.columns.tolist()}"
        )

    print(
        "\n✅ MONTH SAVED:",
        output_file
    )

    print(
        "Rows:",
        len(check_df)
    )

    print(
        "Date:",
        check_df["Date"].min(),
        "→",
        check_df["Date"].max()
    )

    print(
        "Net Sales:",
        round(
            pd.to_numeric(
                check_df["Net Sales"],
                errors="coerce"
            ).sum(),
            2
        )
    )

    print(
        "Quantity:",
        round(
            pd.to_numeric(
                check_df["Quantity"],
                errors="coerce"
            ).sum(),
            2
        )
    )

    print(
        "Orders:",
        round(
            pd.to_numeric(
                check_df["Orders"],
                errors="coerce"
            ).sum(),
            2
        )
    )

    return True


# =========================================================
# MAIN
# =========================================================

success = []
failed = []

for year, month in BACKFILL_MONTHS:

    try:

        result = process_month(
            year,
            month
        )

        if result:
            success.append(
                f"{year}-{month:02d}"
            )
        else:
            failed.append(
                f"{year}-{month:02d}"
            )

    except Exception as exc:

        print(
            "\n❌ MONTH FAILED:",
            f"{year}-{month:02d}"
        )

        print(
            "Reason:",
            repr(exc)
        )

        failed.append(
            f"{year}-{month:02d}"
        )

        # Continue to the next month rather than losing
        # the complete backfill because of one month.
        continue


# =========================================================
# FINAL REPORT
# =========================================================

print("\n" + "=" * 80)
print("🏁 HISTORICAL BACKFILL COMPLETED")
print("=" * 80)

print("\n✅ Successful Months:")

for month in success:
    print(
        "   ",
        month
    )

print("\n❌ Failed / Empty Months:")

if failed:

    for month in failed:
        print(
            "   ",
            month
        )

else:

    print(
        "    None"
    )

print("\n📂 CSV folders created under:")
print("    monthly_data/2025/")
print("    monthly_data/2026/")

print("\n📌 Requested months were fetched/rebuilt:")
for year, month in BACKFILL_MONTHS:
    print(f"    {year}-{month:02d}")
print("=" * 80)

if failed:

    print(
        "\n⚠️ Backfill finished with failed months."
        " Review the logs and rerun only those months."
    )
else:

    print(
        "\n🎉 All requested historical months completed."
    )
