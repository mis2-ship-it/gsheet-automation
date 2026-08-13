# =========================================================
# RISTA DSR / MTD DASHBOARD
# =========================================================
#
# Purpose:
#   Read historical monthly Rista CSV files and send
#   one complete FTD + MTD dashboard by email.
#
# Comparisons:
#   FTD vs LW same day
#   FTD vs LM same day
#   FTD vs LY same day
#   MTD vs LM MTD
#   MTD vs LY MTD
#
# Google Sheets:
#   NOT USED
#
# CSV SOURCE:
#   monthly_data/2025/
#   monthly_data/2026/
#
# =========================================================


# =========================================================
# IMPORTS
# =========================================================

from pathlib import Path
from datetime import datetime, timedelta
import os
import re
import smtplib

import pandas as pd

from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from zoneinfo import ZoneInfo


print("=" * 80)
print("🚀 RISTA DSR / MTD DASHBOARD STARTED")
print("=" * 80)


# =========================================================
# CONFIGURATION
# =========================================================

BASE_FOLDER = Path("monthly_data")

SMTP_SERVER = "smtp.gmail.com"
SMTP_PORT = 587

EMAIL = os.environ.get("EMAIL_USER")
PASSWORD = os.environ.get("EMAIL_PASS")

# ---------------------------------------------------------
# TEST RECIPIENT
# ---------------------------------------------------------
# Later you can change this to the team distribution list.
# No EMAIL_TO / EMAIL_CC environment variables required.
# ---------------------------------------------------------

TO = [
    "mis2@frozenbottle.in"
]

CC = []


# =========================================================
# EMAIL VALIDATION
# =========================================================

if not EMAIL:
    raise RuntimeError(
        "❌ EMAIL_USER environment variable is missing"
    )

if not PASSWORD:
    raise RuntimeError(
        "❌ EMAIL_PASS environment variable is missing"
    )

TO = [
    x.strip()
    for x in TO
    if x and x.strip()
]

CC = [
    x.strip()
    for x in CC
    if x and x.strip()
]

if not TO:
    raise RuntimeError(
        "❌ No email recipient configured"
    )


print("📧 Email From :", EMAIL)
print("📧 Email To   :", TO)


# =========================================================
# FIND ALL MONTHLY CSV FILES
# =========================================================

if not BASE_FOLDER.exists():

    raise RuntimeError(
        f"❌ Folder not found: {BASE_FOLDER}"
    )


csv_files = sorted(
    BASE_FOLDER.rglob("MTD_*.csv")
)


print("=" * 80)
print("CSV FILE CHECK")
print("=" * 80)

print(
    "📂 CSV Files Found:",
    len(csv_files)
)


if not csv_files:

    raise RuntimeError(
        "❌ No MTD CSV files found under monthly_data/"
    )


for file in csv_files:

    print(
        "   ",
        file
    )


# =========================================================
# MONTH / YEAR FROM FILE NAME
# =========================================================

MONTH_MAP = {
    "Jan": 1,
    "Feb": 2,
    "Mar": 3,
    "Apr": 4,
    "May": 5,
    "Jun": 6,
    "Jul": 7,
    "Aug": 8,
    "Sep": 9,
    "Oct": 10,
    "Nov": 11,
    "Dec": 12
}


def extract_month_year(path):

    match = re.search(
        r"MTD_([A-Za-z]{3})_(\d{2})\.csv",
        path.name
    )

    if not match:

        return datetime(
            1900,
            1,
            1
        )

    month_name = match.group(1)

    year_short = int(
        match.group(2)
    )

    month = MONTH_MAP.get(
        month_name
    )

    if month is None:

        return datetime(
            1900,
            1,
            1
        )

    year = 2000 + year_short

    return datetime(
        year,
        month,
        1
    )


# =========================================================
# LOAD ALL CSV FILES
# =========================================================

all_data = []

print("=" * 80)
print("READING MONTHLY CSV FILES")
print("=" * 80)


for csv_file in csv_files:

    print(
        f"📥 Reading: {csv_file}"
    )

    try:

        df = pd.read_csv(
            csv_file,
            low_memory=False
        )

        if df.empty:

            print(
                "   ⚠️ Empty file - skipped"
            )

            continue

        df["__source_file"] = str(
            csv_file
        )

        all_data.append(
            df
        )

        print(
            f"   ✅ Rows: {len(df):,}"
        )

    except Exception as exc:

        print(
            f"   ❌ Failed: {exc}"
        )


if not all_data:

    raise RuntimeError(
        "❌ No usable CSV data found"
    )


final_df = pd.concat(
    all_data,
    ignore_index=True
)


print("=" * 80)
print("COMBINED DATA")
print("=" * 80)

print(
    "Total Rows   :",
    f"{len(final_df):,}"
)

print(
    "Total Columns:",
    len(final_df.columns)
)


# =========================================================
# REQUIRED COLUMNS
# =========================================================

REQUIRED_COLUMNS = [
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
    "Orders"
]


missing_columns = [
    col
    for col in REQUIRED_COLUMNS
    if col not in final_df.columns
]


if missing_columns:

    raise RuntimeError(
        "❌ Required columns missing:\n"
        + "\n".join(
            f"   - {col}"
            for col in missing_columns
        )
    )


# =========================================================
# CLEAN DATA
# =========================================================

final_df["Date"] = pd.to_datetime(
    final_df["Date"],
    errors="coerce"
)


final_df = final_df[
    final_df["Date"].notna()
].copy()


# ---------------------------------------------------------
# Numeric columns
# ---------------------------------------------------------

NUMERIC_COLUMNS = [
    "Net Sales",
    "Discount",
    "Taxes",
    "Gross Sales",
    "Orders"
]


for col in NUMERIC_COLUMNS:

    final_df[col] = pd.to_numeric(
        final_df[col],
        errors="coerce"
    ).fillna(0)


# ---------------------------------------------------------
# Text columns
# ---------------------------------------------------------

TEXT_COLUMNS = [
    "Brand Name",
    "Branch",
    "Source",
    "Session",
    "Store Type",
    "Region"
]


for col in TEXT_COLUMNS:

    final_df[col] = (
        final_df[col]
        .fillna("")
        .astype(str)
        .str.strip()
    )


# ---------------------------------------------------------
# Session standardization for dashboard
# Breakfast > Lunch > Snacks > Dinner > Post Dinner
# Late Night and Closing are treated as Post Dinner
# ---------------------------------------------------------
SESSION_MAP = {
    "breakfast": "Breakfast",
    "lunch": "Lunch",
    "snacks": "Snacks",
    "snack": "Snacks",
    "dinner": "Dinner",
    "late night": "Post Dinner",
    "latenight": "Post Dinner",
    "closing": "Post Dinner",
    "post dinner": "Post Dinner",
    "post-dinner": "Post Dinner",
    "": "Post Dinner"
}

final_df["Session"] = (
    final_df["Session"]
    .str.lower()
    .map(SESSION_MAP)
    .fillna(final_df["Session"].str.title())
)

SESSION_ORDER = {
    "Breakfast": 1,
    "Lunch": 2,
    "Snacks": 3,
    "Dinner": 4,
    "Post Dinner": 5
}


final_df["Store Type"] = (
    final_df["Store Type"]
    .str.upper()
)


# =========================================================
# REMOVE INTERNAL COLUMN
# =========================================================

if "__source_file" in final_df.columns:

    final_df.drop(
        columns="__source_file",
        inplace=True
    )


# =========================================================
# DATE RANGE
# =========================================================

available_dates = sorted(
    final_df["Date"]
    .dt.normalize()
    .unique()
)


if not available_dates:

    raise RuntimeError(
        "❌ No valid dates available"
    )


latest_date = pd.Timestamp(
    available_dates[-1]
)


print("=" * 80)
print("AVAILABLE DATE RANGE")
print("=" * 80)

print(
    "Minimum Date:",
    final_df["Date"].min().date()
)

print(
    "Maximum Date:",
    final_df["Date"].max().date()
)


# =========================================================
# DASHBOARD BUSINESS DATE
# =========================================================
#
# FTD = Yesterday
#
# Example:
# Today      : 13-Aug-2026
# FTD        : 12-Aug-2026
# LW Same Day: 05-Aug-2026
# LM Same Day: 12-Jul-2026
# LY Same Day: 12-Aug-2025
#
# =========================================================

india_today = datetime.now(
    ZoneInfo("Asia/Kolkata")
).date()

ftd_date = (
    india_today
    - timedelta(days=1)
)


# =========================================================
# COMPARISON DATES
# =========================================================

# ---------------------------------------------------------
# LAST WEEK - SAME WEEKDAY
# ---------------------------------------------------------

lw_date = (
    ftd_date
    - timedelta(days=7)
)


# ---------------------------------------------------------
# LAST MONTH - SAME WEEKDAY
# ---------------------------------------------------------

def same_weekday_last_month(current_date):

    current_date = pd.Timestamp(current_date)

    target = (
        current_date
        - pd.DateOffset(months=1)
    )

    while target.weekday() != current_date.weekday():
        target -= pd.Timedelta(days=1)

    return target.date()


lm_date = same_weekday_last_month(
    ftd_date
)


# ---------------------------------------------------------
# LAST YEAR - SAME WEEKDAY
# ---------------------------------------------------------

def same_weekday_last_year(current_date):

    current_date = pd.Timestamp(current_date)

    target = (
        current_date
        - pd.DateOffset(years=1)
    )

    while target.weekday() != current_date.weekday():
        target -= pd.Timedelta(days=1)

    return target.date()


ly_date = same_weekday_last_year(
    ftd_date
)


print("=" * 80)
print("DASHBOARD BUSINESS DATE CHECK")
print("=" * 80)

print(
    "India Today:",
    india_today
)

print(
    "FTD:",
    ftd_date
)

print(
    "LW :",
    lw_date
)

print(
    "LM :",
    lm_date
)

print(
    "LY :",
    ly_date
)


# =========================================================
# MONTH START DATES
# =========================================================

# Current MTD
current_month_start = pd.Timestamp(
    year=ftd_date.year,
    month=ftd_date.month,
    day=1
).date()


# Last Month MTD
lm_month_start = pd.Timestamp(
    year=lm_date.year,
    month=lm_date.month,
    day=1
).date()


# Last Year MTD
ly_month_start = pd.Timestamp(
    year=ly_date.year,
    month=ly_date.month,
    day=1
).date()


print("=" * 80)
print("MTD PERIODS")
print("=" * 80)

print(
    "Current MTD:",
    current_month_start,
    "→",
    ftd_date
)

print(
    "LM MTD     :",
    lm_month_start,
    "→",
    lm_date
)

print(
    "LY MTD     :",
    ly_month_start,
    "→",
    ly_date
)

# =========================================================
# DATE FILTER HELPER
# =========================================================

def date_filter(
    start_date,
    end_date
):

    start = pd.Timestamp(
        start_date
    )

    end = pd.Timestamp(
        end_date
    )

    return final_df.loc[
        (
            final_df["Date"]
            .dt.normalize()
            >= start
        )
        &
        (
            final_df["Date"]
            .dt.normalize()
            <= end
        )
    ].copy()


def single_date_filter(
    target_date
):

    target = pd.Timestamp(
        target_date
    )

    return final_df.loc[
        final_df["Date"]
        .dt.normalize()
        .eq(target)
    ].copy()


# =========================================================
# FTD DATASETS
# =========================================================

ftd_df = single_date_filter(
    ftd_date
)


lw_df = single_date_filter(
    lw_date
)


lm_df = single_date_filter(
    lm_date
)


ly_df = single_date_filter(
    ly_date
)


# =========================================================
# MTD DATASETS
# =========================================================

mtd_df = date_filter(
    current_month_start,
    ftd_date
)


lm_mtd_df = date_filter(
    lm_month_start,
    lm_date
)


ly_mtd_df = date_filter(
    ly_month_start,
    ly_date
)


# =========================================================
# DATA CHECK
# =========================================================

print("=" * 80)
print("FILTER CHECK")
print("=" * 80)

print(
    "FTD Rows    :",
    f"{len(ftd_df):,}"
)

print(
    "LW Rows     :",
    f"{len(lw_df):,}"
)

print(
    "LM Rows     :",
    f"{len(lm_df):,}"
)

print(
    "LY Rows     :",
    f"{len(ly_df):,}"
)

print(
    "MTD Rows    :",
    f"{len(mtd_df):,}"
)

print(
    "LM MTD Rows :",
    f"{len(lm_mtd_df):,}"
)

print(
    "LY MTD Rows :",
    f"{len(ly_mtd_df):,}"
)


# =========================================================
# VALIDATION
# =========================================================

if ftd_df.empty:

    raise RuntimeError(
        f"❌ No FTD data found for {ftd_date}"
    )


if lw_df.empty:

    print(
        f"⚠️ No LW data found for {lw_date}"
    )


if lm_df.empty:

    print(
        f"⚠️ No LM data found for {lm_date}"
    )


if ly_df.empty:

    print(
        f"⚠️ No LY data found for {ly_date}"
    )


# =========================================================
# KPI BUILDER
# =========================================================

def get_kpi(df):

    if df.empty:

        return {
            "Gross": 0,
            "Net": 0,
            "Discount": 0,
            "Orders": 0,
            "AOV": 0,
            "Dis %": 0
        }


    gross = df[
        "Gross Sales"
    ].sum()


    net = df[
        "Net Sales"
    ].sum()


    discount = df[
        "Discount"
    ].sum()


    orders = df[
        "Orders"
    ].sum()

    aov = (
        net / orders
        if orders
        else 0
    )


    dis_pct = (
        discount / gross * 100
        if gross
        else 0
    )


    return {

        "Gross": round(
            gross,
            2
        ),

        "Net": round(
            net,
            2
        ),

        "Discount": round(
            discount,
            2
        ),

        "Orders": int(
            orders
        ),


        "AOV": round(
            aov,
            2
        ),

        "Dis %": round(
            dis_pct,
            2
        )
    }


# =========================================================
# KPI OBJECTS
# =========================================================

ftd_kpi = get_kpi(
    ftd_df
)

lw_kpi = get_kpi(
    lw_df
)

lm_kpi = get_kpi(
    lm_df
)

ly_kpi = get_kpi(
    ly_df
)

mtd_kpi = get_kpi(
    mtd_df
)

lm_mtd_kpi = get_kpi(
    lm_mtd_df
)

ly_mtd_kpi = get_kpi(
    ly_mtd_df
)


# =========================================================
# GROWTH %
# =========================================================

def growth_pct(
    current,
    previous
):

    if previous == 0:
        return 0

    return round(
        (
            (current - previous)
            / previous
        ) * 100,
        1
    )


def build_growth_kpi(
    current,
    previous
):

    return {

        "Gross %":
            growth_pct(
                current["Gross"],
                previous["Gross"]
            ),

        "Net %":
            growth_pct(
                current["Net"],
                previous["Net"]
            ),

        "Orders %":
            growth_pct(
                current["Orders"],
                previous["Orders"]
            )
    }


# =========================================================
# GROWTH KPI
# =========================================================

# =========================================================
# FTD vs LW
# =========================================================

ftd_lw_growth = build_growth_kpi(
    ftd_kpi,
    lw_kpi
)


# =========================================================
# FTD vs LM
# =========================================================

ftd_lm_growth = build_growth_kpi(
    ftd_kpi,
    lm_kpi
)


# =========================================================
# FTD vs LY
# =========================================================

ftd_ly_growth = build_growth_kpi(
    ftd_kpi,
    ly_kpi
)


# =========================================================
# MTD vs LM MTD
# =========================================================

mtd_lm_growth = build_growth_kpi(
    mtd_kpi,
    lm_mtd_kpi
)


# =========================================================
# MTD vs LY MTD
# =========================================================

mtd_ly_growth = build_growth_kpi(
    mtd_kpi,
    ly_mtd_kpi
)


# =========================================================
# STORE TYPE KPI HELPER
# =========================================================

def store_type_kpi(df, store_type):

    return get_kpi(
        df[
            df["Store Type"]
            .astype(str)
            .str.strip()
            .str.upper()
            == store_type.upper()
        ].copy()
    )


# =========================================================
# FTD STORE TYPE KPI
# =========================================================

ftd_coco_kpi = store_type_kpi(
    ftd_df,
    "COCO"
)

ftd_fofo_kpi = store_type_kpi(
    ftd_df,
    "FOFO"
)


# =========================================================
# LW STORE TYPE KPI
# =========================================================

lw_coco_kpi = store_type_kpi(
    lw_df,
    "COCO"
)

lw_fofo_kpi = store_type_kpi(
    lw_df,
    "FOFO"
)


# =========================================================
# LM STORE TYPE KPI
# =========================================================

lm_coco_kpi = store_type_kpi(
    lm_df,
    "COCO"
)

lm_fofo_kpi = store_type_kpi(
    lm_df,
    "FOFO"
)


# =========================================================
# LY STORE TYPE KPI
# =========================================================

ly_coco_kpi = store_type_kpi(
    ly_df,
    "COCO"
)

ly_fofo_kpi = store_type_kpi(
    ly_df,
    "FOFO"
)


# =========================================================
# MTD STORE TYPE KPI
# =========================================================

mtd_coco_kpi = store_type_kpi(
    mtd_df,
    "COCO"
)

mtd_fofo_kpi = store_type_kpi(
    mtd_df,
    "FOFO"
)


# =========================================================
# LM MTD STORE TYPE KPI
# =========================================================

lm_mtd_coco_kpi = store_type_kpi(
    lm_mtd_df,
    "COCO"
)

lm_mtd_fofo_kpi = store_type_kpi(
    lm_mtd_df,
    "FOFO"
)


# =========================================================
# LY MTD STORE TYPE KPI
# =========================================================

ly_mtd_coco_kpi = store_type_kpi(
    ly_mtd_df,
    "COCO"
)

ly_mtd_fofo_kpi = store_type_kpi(
    ly_mtd_df,
    "FOFO"
)


# =========================================================
# COCO GROWTH KPI
# =========================================================

ftd_coco_lw_growth = build_growth_kpi(
    ftd_coco_kpi,
    lw_coco_kpi
)

ftd_coco_lm_growth = build_growth_kpi(
    ftd_coco_kpi,
    lm_coco_kpi
)

ftd_coco_ly_growth = build_growth_kpi(
    ftd_coco_kpi,
    ly_coco_kpi
)

mtd_coco_lm_growth = build_growth_kpi(
    mtd_coco_kpi,
    lm_mtd_coco_kpi
)

mtd_coco_ly_growth = build_growth_kpi(
    mtd_coco_kpi,
    ly_mtd_coco_kpi
)

# =========================================================
# SUMMARY BUILDER
# =========================================================

def build_summary(
    df,
    column
):

    if df.empty:

        return pd.DataFrame(
            columns=[
                column,
                "Gross",
                "Net",
                "Discount",
                "Orders",
                "AOV",
                "Dis %",
                "Contribution %"
            ]
        )


    work = df.copy()


    work[column] = (
        work[column]
        .fillna("Others")
        .astype(str)
        .replace("", "Others")
    )


    summary = (
        work
        .groupby(
            column,
            dropna=False
        )
        .agg(
            Gross=(
                "Gross Sales",
                "sum"
            ),
            Net=(
                "Net Sales",
                "sum"
            ),
            Discount=(
                "Discount",
                "sum"
            ),
            Orders=(
                "Orders",
                "sum"
            )
        )
        .reset_index()
    )


    summary["AOV"] = (
        summary["Net"]
        /
        summary["Orders"]
        .replace(0, 1)
    )


    summary["Dis %"] = (
        summary["Discount"]
        /
        summary["Gross"]
        .replace(0, 1)
    ) * 100


    total_net = (
        summary["Net"].sum()
    )


    if total_net:

        summary["Contribution %"] = (
            summary["Net"]
            / total_net
        ) * 100

    else:

        summary["Contribution %"] = 0


    summary = (
        summary
        .sort_values(
            "Net",
            ascending=False
        )
        .reset_index(
            drop=True
        )
    )


    return summary.round(2)


# =========================================================
# COCO DATA
# =========================================================

ftd_coco_df = ftd_df[
    ftd_df["Store Type"]
    == "COCO"
].copy()


mtd_coco_df = mtd_df[
    mtd_df["Store Type"]
    == "COCO"
].copy()


lw_coco_df = lw_df[
    lw_df["Store Type"]
    == "COCO"
].copy()


lm_coco_df = lm_df[
    lm_df["Store Type"]
    == "COCO"
].copy()


ly_coco_df = ly_df[
    ly_df["Store Type"]
    == "COCO"
].copy()


lm_mtd_coco_df = lm_mtd_df[
    lm_mtd_df["Store Type"]
    == "COCO"
].copy()


ly_mtd_coco_df = ly_mtd_df[
    ly_mtd_df["Store Type"]
    == "COCO"
].copy()


# =========================================================
# GENERIC PERFORMANCE SUMMARY
# =========================================================

def performance_summary(
    current_df,
    previous_df,
    group_column
):

    current = build_summary(
        current_df,
        group_column
    )


    previous = build_summary(
        previous_df,
        group_column
    )


    if current.empty:

        return current


    previous_net = (
        previous[
            [
                group_column,
                "Net",
                "Orders",
                "Dis %"
            ]
        ]
        .rename(
            columns={
                "Net":
                    "Previous Net",
                "Orders":
                    "Previous Orders",
                "Dis %":
                    "Previous Dis %"
            }
        )
    )


    result = current.merge(
        previous_net,
        on=group_column,
        how="left"
    )


    result[
        "Previous Net"
    ] = (
        result[
            "Previous Net"
        ]
        .fillna(0)
    )


    result[
        "Previous Orders"
    ] = (
        result[
            "Previous Orders"
        ]
        .fillna(0)
    )

    result[
        "Previous Dis %"
    ] = (
        result[
            "Previous Dis %"
        ]
        .fillna(0)
    )


    result["Net Growth %"] = (
        (
            result["Net"]
            -
            result["Previous Net"]
        )
        /
        result[
            "Previous Net"
        ].replace(
            0,
            pd.NA
        )
    ) * 100


    result["Orders Growth %"] = (
        (
            result["Orders"]
            -
            result["Previous Orders"]
        )
        /
        result[
            "Previous Orders"
        ].replace(
            0,
            pd.NA
        )
    ) * 100


    result[
        "Net Growth %"
    ] = (
        result[
            "Net Growth %"
        ]
        .fillna(0)
        .round(1)
    )


    # Negative = discount reduced (good); positive = discount increased (bad).
    result["Dis % Change"] = (
        result["Dis %"] - result["Previous Dis %"]
    ).round(1)


    result[
        "Orders Growth %"
    ] = (
        result[
            "Orders Growth %"
        ]
        .fillna(0)
        .round(1)
    )


    result.drop(
        columns=[
            "Previous Net",
            "Previous Orders",
            "Previous Dis %"
        ],
        inplace=True
    )


    return result.round(2)


# =========================================================
# FTD PERFORMANCE TABLES
# =========================================================

brand_ftd_lw = performance_summary(
    ftd_coco_df,
    lw_coco_df,
    "Brand Name"
)


brand_ftd_lm = performance_summary(
    ftd_coco_df,
    lm_coco_df,
    "Brand Name"
)


brand_ftd_ly = performance_summary(
    ftd_coco_df,
    ly_coco_df,
    "Brand Name"
)


source_ftd_lw = performance_summary(
    ftd_coco_df,
    lw_coco_df,
    "Source"
)


source_ftd_lm = performance_summary(
    ftd_coco_df,
    lm_coco_df,
    "Source"
)


source_ftd_ly = performance_summary(
    ftd_coco_df,
    ly_coco_df,
    "Source"
)


region_ftd_lw = performance_summary(
    ftd_coco_df,
    lw_coco_df,
    "Region"
)


region_ftd_lm = performance_summary(
    ftd_coco_df,
    lm_coco_df,
    "Region"
)


region_ftd_ly = performance_summary(
    ftd_coco_df,
    ly_coco_df,
    "Region"
)


session_ftd_lw = performance_summary(
    ftd_coco_df,
    lw_coco_df,
    "Session"
)


session_ftd_lm = performance_summary(
    ftd_coco_df,
    lm_coco_df,
    "Session"
)


session_ftd_ly = performance_summary(
    ftd_coco_df,
    ly_coco_df,
    "Session"
)

# =========================================================
# COCO DATA FILTERS
# =========================================================

ftd_coco_df = ftd_df[
    ftd_df["Store Type"].astype(str).str.strip().str.upper() == "COCO"
].copy()

lw_coco_df = lw_df[
    lw_df["Store Type"].astype(str).str.strip().str.upper() == "COCO"
].copy()

lm_coco_df = lm_df[
    lm_df["Store Type"].astype(str).str.strip().str.upper() == "COCO"
].copy()

ly_coco_df = ly_df[
    ly_df["Store Type"].astype(str).str.strip().str.upper() == "COCO"
].copy()

mtd_coco_df = mtd_df[
    mtd_df["Store Type"].astype(str).str.strip().str.upper() == "COCO"
].copy()

lm_mtd_coco_df = lm_mtd_df[
    lm_mtd_df["Store Type"].astype(str).str.strip().str.upper() == "COCO"
].copy()

ly_mtd_coco_df = ly_mtd_df[
    ly_mtd_df["Store Type"].astype(str).str.strip().str.upper() == "COCO"
].copy()


print("=" * 80)
print("COCO FILTER CHECK")
print("=" * 80)

print("FTD COCO       :", len(ftd_coco_df))
print("LW COCO        :", len(lw_coco_df))
print("LM COCO        :", len(lm_coco_df))
print("LY COCO        :", len(ly_coco_df))
print("MTD COCO       :", len(mtd_coco_df))
print("LM MTD COCO    :", len(lm_mtd_coco_df))
print("LY MTD COCO    :", len(ly_mtd_coco_df))

# =========================================================
# MTD PERFORMANCE TABLES
# =========================================================

brand_mtd_lm = performance_summary(
    mtd_coco_df,
    lm_mtd_coco_df,
    "Brand Name"
)

brand_mtd_ly = performance_summary(
    mtd_coco_df,
    ly_mtd_coco_df,
    "Brand Name"
)

source_mtd_lm = performance_summary(
    mtd_coco_df,
    lm_mtd_coco_df,
    "Source"
)

source_mtd_ly = performance_summary(
    mtd_coco_df,
    ly_mtd_coco_df,
    "Source"
)

region_mtd_lm = performance_summary(
    mtd_coco_df,
    lm_mtd_coco_df,
    "Region"
)

region_mtd_ly = performance_summary(
    mtd_coco_df,
    ly_mtd_coco_df,
    "Region"
)

session_mtd_lm = performance_summary(
    mtd_coco_df,
    lm_mtd_coco_df,
    "Session"
)

session_mtd_ly = performance_summary(
    mtd_coco_df,
    ly_mtd_coco_df,
    "Session"
)

# =========================================================
# TOP 10 BRANCHES
# =========================================================

top_branch_ftd_lw = performance_summary(
    ftd_coco_df,
    lw_coco_df,
    "Branch"
).head(10)

top_branch_ftd_lm = performance_summary(
    ftd_coco_df,
    lm_coco_df,
    "Branch"
).head(10)

top_branch_mtd_lm = performance_summary(
    mtd_coco_df,
    lm_mtd_coco_df,
    "Branch"
).head(10)

# =========================================================
# DAY-LEVEL COCO METRICS
# =========================================================

day_coco_summary = (
    mtd_coco_df
    .groupby("Date")
    .agg(
        Gross=("Gross Sales", "sum"),
        Net=("Net Sales", "sum"),
        Discount=("Discount", "sum"),
        Orders=("Orders", "sum")
    )
    .reset_index()
)

day_coco_summary["AOV"] = (
    day_coco_summary["Net"]
    /
    day_coco_summary["Orders"].replace(0, 1)
)

day_coco_summary["Dis %"] = (
    day_coco_summary["Discount"]
    /
    day_coco_summary["Gross"].replace(0, 1)
) * 100

day_coco_summary = (
    day_coco_summary
    .sort_values("Date", ascending=False)
    .reset_index(drop=True)
)

day_coco_summary["Gross"] = day_coco_summary["Gross"].round(2)
day_coco_summary["Net"] = day_coco_summary["Net"].round(2)
day_coco_summary["Discount"] = day_coco_summary["Discount"].round(2)
day_coco_summary["Orders"] = day_coco_summary["Orders"].round(0).astype(int)
day_coco_summary["AOV"] = day_coco_summary["AOV"].round(2)
day_coco_summary["Dis %"] = day_coco_summary["Dis %"].round(2)

# =========================================================
# DAY-LEVEL COMPARISON TABLES - COCO
# =========================================================

def build_day_level_comparison(current_df, previous_df):
    """
    Compare each available day in current_df against the
    corresponding comparison date in previous_df.

    Used for:
        FTD vs LW
        FTD vs LM
        FTD vs LY
    """

    current = (
        current_df
        .groupby("Date")
        .agg(
            Gross=("Gross Sales", "sum"),
            Net=("Net Sales", "sum"),
            Orders=("Orders", "sum"),
            Discount=("Discount", "sum")
        )
        .reset_index()
    )

    previous = (
        previous_df
        .groupby("Date")
        .agg(
            Prev_Gross=("Gross Sales", "sum"),
            Prev_Net=("Net Sales", "sum"),
            Prev_Orders=("Orders", "sum"),
            Prev_Discount=("Discount", "sum")
        )
        .reset_index()
    )

    if current.empty or previous.empty:
        return pd.DataFrame()

    # Use the first/only comparison date
    previous_row = previous.iloc[0]

    current["Prev Gross"] = previous_row["Prev_Gross"]
    current["Prev Net"] = previous_row["Prev_Net"]
    current["Prev Orders"] = previous_row["Prev_Orders"]

    current["Gross Growth %"] = (
        (
            current["Gross"]
            - current["Prev Gross"]
        )
        / current["Prev Gross"].replace(0, 1)
        * 100
    )

    current["Net Growth %"] = (
        (
            current["Net"]
            - current["Prev Net"]
        )
        / current["Prev Net"].replace(0, 1)
        * 100
    )

    current["Orders Growth %"] = (
        (
            current["Orders"]
            - current["Prev Orders"]
        )
        / current["Prev Orders"].replace(0, 1)
        * 100
    )


    current["AOV"] = (
        current["Net"]
        / current["Orders"].replace(0, 1)
    )

    current = current.round(2)

    return current


# =========================================================
# FTD vs LW - COCO
# =========================================================

day_level_ftd_lw = build_day_level_comparison(
    ftd_coco_df,
    lw_coco_df
)


# =========================================================
# FTD vs LM - COCO
# =========================================================

day_level_ftd_lm = build_day_level_comparison(
    ftd_coco_df,
    lm_coco_df
)


# =========================================================
# FTD vs LY - COCO
# =========================================================

day_level_ftd_ly = build_day_level_comparison(
    ftd_coco_df,
    ly_coco_df
)

# =========================================================
# MTD vs LM MTD - DAY LEVEL COCO
# =========================================================

day_level_mtd_lm = build_day_level_comparison(
    mtd_coco_df,
    lm_mtd_coco_df
)


# =========================================================
# MTD vs LY MTD - DAY LEVEL COCO
# =========================================================

day_level_mtd_ly = build_day_level_comparison(
    mtd_coco_df,
    ly_mtd_coco_df
)
# =========================================================
# KPI TABLE
# =========================================================

kpi_table = pd.DataFrame({

    "Metric": [
        "Gross Revenue",
        "Net Revenue",
        "Discount",
        "Orders",
        "AOV",
        "Discount %"
    ],

    "FTD": [
        ftd_coco_kpi["Gross"],
        ftd_coco_kpi["Net"],
        ftd_coco_kpi["Discount"],
        ftd_coco_kpi["Orders"],
        ftd_coco_kpi["AOV"],
        ftd_coco_kpi["Dis %"]
    ],

    "MTD": [
        mtd_coco_kpi["Gross"],
        mtd_coco_kpi["Net"],
        mtd_coco_kpi["Discount"],
        mtd_coco_kpi["Orders"],
        mtd_coco_kpi["AOV"],
        mtd_coco_kpi["Dis %"]
    ]

})


# =========================================================
# STORE TYPE TABLE
# =========================================================

store_type_table = pd.DataFrame({

    "Metric": [
        "Gross Revenue",
        "Net Revenue",
        "Discount",
        "Orders",
        "AOV",
        "Discount %"
    ],

    "FTD COCO": [
        ftd_coco_kpi["Gross"],
        ftd_coco_kpi["Net"],
        ftd_coco_kpi["Discount"],
        ftd_coco_kpi["Orders"],
        ftd_coco_kpi["AOV"],
        ftd_coco_kpi["Dis %"]
    ],

    "MTD COCO": [
        mtd_coco_kpi["Gross"],
        mtd_coco_kpi["Net"],
        mtd_coco_kpi["Discount"],
        mtd_coco_kpi["Orders"],
        mtd_coco_kpi["AOV"],
        mtd_coco_kpi["Dis %"]
    ],

    "FTD FOFO": [
        ftd_fofo_kpi["Gross"],
        ftd_fofo_kpi["Net"],
        ftd_fofo_kpi["Discount"],
        ftd_fofo_kpi["Orders"],
        ftd_fofo_kpi["AOV"],
        ftd_fofo_kpi["Dis %"]
    ],

    "MTD FOFO": [
        mtd_fofo_kpi["Gross"],
        mtd_fofo_kpi["Net"],
        mtd_fofo_kpi["Discount"],
        mtd_fofo_kpi["Orders"],
        mtd_fofo_kpi["AOV"],
        mtd_fofo_kpi["Dis %"]
    ]

})


# =========================================================
# COMPARISON KPI TABLE
# =========================================================

comparison_table = pd.DataFrame({

    "Metric": [
        "Gross Revenue",
        "Net Revenue",
        "Orders",
        "Discount %"
    ],

    "FTD": [
        ftd_coco_kpi["Gross"],
        ftd_coco_kpi["Net"],
        ftd_coco_kpi["Orders"],
        ftd_coco_kpi["Dis %"]
    ],

    "LW Same Day": [
        store_type_kpi(lw_df, "COCO")["Gross"],
        store_type_kpi(lw_df, "COCO")["Net"],
        store_type_kpi(lw_df, "COCO")["Orders"],
        store_type_kpi(lw_df, "COCO")["Dis %"]
    ],

    "FTD vs LW %": [
        ftd_lw_growth["Gross %"],
        ftd_lw_growth["Net %"],
        ftd_lw_growth["Orders %"],
        ftd_coco_kpi["Dis %"] - store_type_kpi(lw_df, "COCO")["Dis %"]
    ],

    "LM Same Day": [
        store_type_kpi(lm_df, "COCO")["Gross"],
        store_type_kpi(lm_df, "COCO")["Net"],
        store_type_kpi(lm_df, "COCO")["Orders"],
        store_type_kpi(lm_df, "COCO")["Dis %"]
    ],

    "FTD vs LM %": [
        ftd_lm_growth["Gross %"],
        ftd_lm_growth["Net %"],
        ftd_lm_growth["Orders %"],
        ftd_coco_kpi["Dis %"] - store_type_kpi(lm_df, "COCO")["Dis %"]
    ],

    "LY Same Day": [
        store_type_kpi(ly_df, "COCO")["Gross"],
        store_type_kpi(ly_df, "COCO")["Net"],
        store_type_kpi(ly_df, "COCO")["Orders"],
        store_type_kpi(ly_df, "COCO")["Dis %"]
    ],

    "FTD vs LY %": [
        ftd_ly_growth["Gross %"],
        ftd_ly_growth["Net %"],
        ftd_ly_growth["Orders %"],
        ftd_coco_kpi["Dis %"] - store_type_kpi(ly_df, "COCO")["Dis %"]
    ]

})


# =========================================================
# MTD COMPARISON TABLE
# =========================================================

mtd_comparison_table = pd.DataFrame(
    [
        {
            "Metric": "Gross Revenue",
            "Current MTD": mtd_kpi["Gross"],
            "LM MTD": lm_mtd_kpi["Gross"],
            "MTD vs LM %": mtd_lm_growth["Gross %"],
            "LY MTD": ly_mtd_kpi["Gross"],
            "MTD vs LY %": mtd_ly_growth["Gross %"]
        },
        {
            "Metric": "Net Revenue",
            "Current MTD": mtd_kpi["Net"],
            "LM MTD": lm_mtd_kpi["Net"],
            "MTD vs LM %": mtd_lm_growth["Net %"],
            "LY MTD": ly_mtd_kpi["Net"],
            "MTD vs LY %": mtd_ly_growth["Net %"]
        },
        {
            "Metric": "Orders",
            "Current MTD": mtd_kpi["Orders"],
            "LM MTD": lm_mtd_kpi["Orders"],
            "MTD vs LM %": mtd_lm_growth["Orders %"],
            "LY MTD": ly_mtd_kpi["Orders"],
            "MTD vs LY %": mtd_ly_growth["Orders %"]
        }
    ]
)

# =========================================================
# HTML FORMATTERS
# =========================================================

def format_number(value):

    if pd.isna(value):

        return ""


    if isinstance(
        value,
        (int, float)
    ):

        return f"{value:,.2f}"


    return value


def html_table(
    df,
    percent_columns=None
):

    if df is None:
        return ""

    if df.empty:
        return ""

    work = df.copy()
    percent_columns = percent_columns or []

    for col in work.columns:
        if col.lower() in ("dis %", "discount %", "dis % change") and col not in percent_columns:
            percent_columns.append(col)

    headers = list(work.columns)

    html = ['<table class="data-table"><thead><tr>']
    for col in headers:
        html.append(f'<th>{col}</th>')
    html.append('</tr></thead><tbody>')

    for _, row in work.iterrows():
        html.append('<tr>')
        for col in headers:
            value = row[col]

            if pd.isna(value):
                display = ""
            elif col in percent_columns:
                display = f"{float(value):,.1f}%"
            elif isinstance(value, (int, float)) or pd.api.types.is_number(value):
                display = f"{float(value):,.2f}"
            else:
                display = str(value)

            cell_style = ""
            if col in percent_columns and pd.notna(value):
                number = float(value)
                lower_col = col.lower()

                # Growth: positive = green, negative = red
                if "growth" in lower_col or "vs" in lower_col:
                    if number > 0:
                        cell_style = ' style="background:#E8F5E9;color:#2E7D32;font-weight:bold;"'
                    elif number < 0:
                        cell_style = ' style="background:#FFEBEE;color:#C62828;font-weight:bold;"'

                # Discount %: lower is good, higher is bad
                elif "dis %" in lower_col or "discount %" in lower_col:
                    if number < 0:
                        cell_style = ' style="background:#E8F5E9;color:#2E7D32;font-weight:bold;"'
                    elif number > 0:
                        cell_style = ' style="background:#FFEBEE;color:#C62828;font-weight:bold;"'

            html.append(f'<td{cell_style}>{display}</td>')
        html.append('</tr>')

    html.append('</tbody></table>')
    return ''.join(html)


def insight_block(label, df, comparison_name="FTD vs LW"):
    if df is None or df.empty or "Net Growth %" not in df.columns:
        return ""

    work = df.copy()
    work = work[pd.to_numeric(work["Net"], errors="coerce").fillna(0) > 0]
    work["Net Growth %"] = pd.to_numeric(work["Net Growth %"], errors="coerce").fillna(0)

    if work.empty:
        return ""

    positives = work[work["Net Growth %"] > 0].sort_values("Net Growth %", ascending=False).head(3)
    negatives = work[work["Net Growth %"] < 0].sort_values("Net Growth %", ascending=True).head(3)

    parts = []
    if not positives.empty:
        items = []
        for _, r in positives.iterrows():
            items.append(f"<b>{r.iloc[0]}</b> (+{r['Net Growth %']:.1f}%, ₹{r['Net']:,.0f})")
        parts.append(f"<li><span class='insight-good'>🟢 {label} growth:</span> {', '.join(items)}</li>")

    if not negatives.empty:
        items = []
        for _, r in negatives.iterrows():
            items.append(f"<b>{r.iloc[0]}</b> ({r['Net Growth %']:.1f}%, ₹{r['Net']:,.0f})")
        parts.append(f"<li><span class='insight-bad'>🔴 {label} needs improvement:</span> {', '.join(items)}</li>")

    return ''.join(parts)


def build_insights():
    sections = []

    sections.append(insight_block("Source", source_ftd_lw))
    sections.append(insight_block("Session", session_ftd_lw))
    sections.append(insight_block("Region", region_ftd_lw))

    content = ''.join(x for x in sections if x)
    if not content:
        return "<p style='color:#777;'>No comparison data available for insights.</p>"

    return f"<ul class='insight-list'>{content}</ul>"


insights_html = build_insights()


# =========================================================
# KPI CARD
# =========================================================

def kpi_card(
    title,
    ftd_value,
    mtd_value,
    prefix="",
    suffix=""
):

    return f"""
    <div class="kpi-card">

        <div class="kpi-title">
            {title}
        </div>

        <div class="period-label">
            FTD
        </div>

        <div class="kpi-value">
            {prefix}{ftd_value:,.0f}{suffix}
        </div>

        <div class="divider"></div>

        <div class="period-label">
            MTD
        </div>

        <div class="kpi-value">
            {prefix}{mtd_value:,.0f}{suffix}
        </div>

    </div>
    """


# =========================================================
# EMAIL HTML
# =========================================================

body = f"""
<!DOCTYPE html>

<html>

<head>

<meta charset="UTF-8">

<style>

body {{
    font-family: Calibri, Arial, sans-serif;
    background: #F4F6F8;
    color: #222;
    margin: 0;
    padding: 20px;
}}

.container {{
    max-width: 1400px;
    margin: auto;
    background: white;
    padding: 20px;
}}

.header {{
    background: #243447;
    color: white;
    padding: 18px;
    border-radius: 8px;
    margin-bottom: 20px;
}}

.header-title {{
    font-size: 24px;
    font-weight: bold;
}}

.header-subtitle {{
    margin-top: 6px;
    font-size: 14px;
}}

.section-title {{
    background: #2E8B57;
    color: white;
    padding: 9px 12px;
    margin-top: 25px;
    margin-bottom: 10px;
    border-radius: 4px;
    font-size: 16px;
    font-weight: bold;
}}

.period-title {{
    background: #EAF2F8;
    color: #243447;
    padding: 8px 12px;
    margin-top: 18px;
    margin-bottom: 10px;
    border-left: 5px solid #243447;
    font-weight: bold;
}}

.kpi-row {{
    display: flex;
    gap: 12px;
    flex-wrap: wrap;
    margin-bottom: 20px;
}}

.kpi-card {{
    background: #FFFFFF;
    border: 1px solid #D9DEE3;
    border-radius: 8px;
    padding: 12px;
    width: 165px;
    text-align: center;
}}

.kpi-title {{
    font-size: 14px;
    font-weight: bold;
    margin-bottom: 10px;
}}

.period-label {{
    font-size: 11px;
    color: #777;
    text-transform: uppercase;
}}

.kpi-value {{
    font-size: 21px;
    font-weight: bold;
    color: #0A7D32;
    margin-top: 3px;
}}

.divider {{
    border-top: 1px solid #DDD;
    margin: 9px 0;
}}

.data-table {{
    border-collapse: collapse;
    width: 100%;
    margin-bottom: 20px;
    font-size: 12px;
}}

.data-table th {{
    background: #243447;
    color: white;
    padding: 7px;
    border: 1px solid #243447;
    text-align: center;
}}

.data-table td {{
    padding: 6px;
    border: 1px solid #D9DEE3;
    text-align: center;
}}

.data-table tr:nth-child(even) {{
    background: #F7F8F9;
}}

.insights {{
    background: #F8FAFC;
    border: 1px solid #D9DEE3;
    border-left: 5px solid #2E8B57;
    padding: 12px 16px;
    margin-bottom: 20px;
}}

.insight-list {{
    margin: 6px 0 0 18px;
    padding: 0;
}}

.insight-list li {{
    margin: 7px 0;
}}

.insight-good {{ color: #2E7D32; font-weight: bold; }}
.insight-bad {{ color: #C62828; font-weight: bold; }}

.note {{
    font-size: 12px;
    color: #666;
    margin-bottom: 10px;
}}

.footer {{
    margin-top: 30px;
    padding-top: 12px;
    border-top: 1px solid #DDD;
    color: #777;
    font-size: 11px;
}}

</style>

</head>


<body>

<div class="container">


<div class="header">

    <div class="header-title">
        📊 Frozen Bottle DSR Dashboard
    </div>

    <div class="header-subtitle">
        FTD: {ftd_date.strftime("%d-%b-%Y")}
        &nbsp; | &nbsp;
        MTD: {current_month_start.strftime("%d-%b-%Y")}
        → {ftd_date.strftime("%d-%b-%Y")}
    </div>

</div>


<!-- =====================================================
     YESTERDAY / FTD INSIGHTS
     ===================================================== -->

<div class="section-title">
    💡 Yesterday COCO Sales Insights
</div>

<div class="insights">
    <div class="note">
        Based on FTD {ftd_date.strftime("%d-%b-%Y")} vs LW same day.
        Green = growth opportunity / positive movement. Red = needs improvement.
    </div>
    {insights_html}
</div>

<!-- =====================================================
     KPI CARDS
     ===================================================== -->

<div class="section-title">
    📌 FTD | MTD KPI
</div>


<div class="kpi-row">

    {kpi_card(
        "Gross Revenue",
        ftd_coco_kpi["Gross"],
        mtd_coco_kpi["Gross"],
        "₹"
    )}

    {kpi_card(
        "Net Revenue",
        ftd_coco_kpi["Net"],
        mtd_coco_kpi["Net"],
        "₹"
    )}

    {kpi_card(
        "Discount",
        ftd_coco_kpi["Discount"],
        mtd_coco_kpi["Discount"],
        "₹"
    )}

    {kpi_card(
        "Orders",
        ftd_coco_kpi["Orders"],
        mtd_coco_kpi["Orders"]
    )}

    {kpi_card(
        "AOV",
        ftd_coco_kpi["AOV"],
        mtd_coco_kpi["AOV"],
        "₹"
    )}

    {kpi_card(
        "Discount %",
        ftd_coco_kpi["Dis %"],
        mtd_coco_kpi["Dis %"],
        "",
        "%"
    )}

</div>


<!-- =====================================================
     KPI SUMMARY
     ===================================================== -->

<div class="section-title">
    📊 KPI Summary
</div>

{html_table(
    kpi_table
)}


<!-- =====================================================
     STORE TYPE
     ===================================================== -->

<div class="section-title">
    🏢 COCO vs FOFO
</div>

{html_table(
    store_type_table
)}


<!-- =====================================================
     FTD COMPARISON
     ===================================================== -->

<div class="section-title">
    📅 FTD Comparison
</div>

<div class="note">
    FTD: {ftd_date.strftime("%d-%b-%Y")}
    &nbsp; | &nbsp;
    LW: {lw_date.strftime("%d-%b-%Y")}
    &nbsp; | &nbsp;
    LM: {lm_date.strftime("%d-%b-%Y")}
    &nbsp; | &nbsp;
    LY: {ly_date.strftime("%d-%b-%Y")}
</div>

{html_table(
    comparison_table,
    percent_columns=[
        "FTD vs LW %",
        "FTD vs LM %",
        "FTD vs LY %",
        "Discount %"
    ]
)}


<!-- =====================================================
     MTD COMPARISON
     ===================================================== -->

<div class="section-title">
    📈 MTD Comparison
</div>

<div class="note">
    Current MTD:
    {current_month_start.strftime("%d-%b-%Y")}
    →
    {ftd_date.strftime("%d-%b-%Y")}
    <br>

    LM MTD:
    {lm_month_start.strftime("%d-%b-%Y")}
    →
    {lm_date.strftime("%d-%b-%Y")}
    <br>

    LY MTD:
    {ly_month_start.strftime("%d-%b-%Y")}
    →
    {ly_date.strftime("%d-%b-%Y")}
</div>

{html_table(
    mtd_comparison_table,
    percent_columns=[
        "MTD vs LM %",
        "MTD vs LY %",
        "Discount %"
    ]
)}


<!-- =====================================================
     BRAND
     ===================================================== -->

<div class="section-title">
    🏷 Brand Performance - FTD vs LW
</div>

{html_table(
    brand_ftd_lw,
    percent_columns=[
        "Net Growth %",
        "Orders Growth %",
        "Dis % Change"
    ]
)}


<div class="period-title">
    Brand - FTD vs LM
</div>

{html_table(
    brand_ftd_lm,
    percent_columns=[
        "Net Growth %",
        "Orders Growth %",
        "Dis % Change"
    ]
)}


<div class="period-title">
    Brand - FTD vs LY
</div>

{html_table(
    brand_ftd_ly,
    percent_columns=[
        "Net Growth %",
        "Orders Growth %",
        "Dis % Change"
    ]
)}


<div class="period-title">
    Brand - MTD vs LM MTD
</div>

{html_table(
    brand_mtd_lm,
    percent_columns=[
        "Net Growth %",
        "Orders Growth %",
        "Dis % Change"
    ]
)}


<div class="period-title">
    Brand - MTD vs LY MTD
</div>

{html_table(
    brand_mtd_ly,
    percent_columns=[
        "Net Growth %",
        "Orders Growth %",
        "Dis % Change"
    ]
)}


<!-- =====================================================
     SOURCE
     ===================================================== -->

<div class="section-title">
    🛒 Source Performance
</div>


<div class="period-title">
    FTD vs LW
</div>

{html_table(
    source_ftd_lw,
    percent_columns=[
        "Net Growth %",
        "Orders Growth %",
        "Dis % Change"
    ]
)}


<div class="period-title">
    FTD vs LM
</div>

{html_table(
    source_ftd_lm,
    percent_columns=[
        "Net Growth %",
        "Orders Growth %",
        "Dis % Change"
    ]
)}


<div class="period-title">
    FTD vs LY
</div>

{html_table(
    source_ftd_ly,
    percent_columns=[
        "Net Growth %",
        "Orders Growth %",
        "Dis % Change"
    ]
)}


<div class="period-title">
    MTD vs LM MTD
</div>

{html_table(
    source_mtd_lm,
    percent_columns=[
        "Net Growth %",
        "Orders Growth %",
        "Dis % Change"
    ]
)}


<div class="period-title">
    MTD vs LY MTD
</div>

{html_table(
    source_mtd_ly,
    percent_columns=[
        "Net Growth %",
        "Orders Growth %",
        "Dis % Change"
    ]
)}


<!-- =====================================================
     REGION
     ===================================================== -->

<div class="section-title">
    🌎 Region Performance
</div>


<div class="period-title">
    FTD vs LW
</div>

{html_table(
    region_ftd_lw,
    percent_columns=[
        "Net Growth %",
        "Orders Growth %",
        "Dis % Change"
    ]
)}


<div class="period-title">
    FTD vs LM
</div>

{html_table(
    region_ftd_lm,
    percent_columns=[
        "Net Growth %",
        "Orders Growth %",
        "Dis % Change"
    ]
)}


<div class="period-title">
    FTD vs LY
</div>

{html_table(
    region_ftd_ly,
    percent_columns=[
        "Net Growth %",
        "Orders Growth %",
        "Dis % Change"
    ]
)}


<div class="period-title">
    MTD vs LM MTD
</div>

{html_table(
    region_mtd_lm,
    percent_columns=[
        "Net Growth %",
        "Orders Growth %",
        "Dis % Change"
    ]
)}


<div class="period-title">
    MTD vs LY MTD
</div>

{html_table(
    region_mtd_ly,
    percent_columns=[
        "Net Growth %",
        "Orders Growth %",
        "Dis % Change"
    ]
)}


<!-- =====================================================
     SESSION
     ===================================================== -->

<div class="section-title">
    🕒 Session Performance
</div>


<div class="period-title">
    FTD vs LW
</div>

{html_table(
    session_ftd_lw,
    percent_columns=[
        "Net Growth %",
        "Orders Growth %",
        "Dis % Change"
    ]
)}


<div class="period-title">
    FTD vs LM
</div>

{html_table(
    session_ftd_lm,
    percent_columns=[
        "Net Growth %",
        "Orders Growth %",
        "Dis % Change"
    ]
)}


<div class="period-title">
    FTD vs LY
</div>

{html_table(
    session_ftd_ly,
    percent_columns=[
        "Net Growth %",
        "Orders Growth %",
        "Dis % Change"
    ]
)}


<div class="period-title">
    MTD vs LM MTD
</div>

{html_table(
    session_mtd_lm,
    percent_columns=[
        "Net Growth %",
        "Orders Growth %",
        "Dis % Change"
    ]
)}


<div class="period-title">
    MTD vs LY MTD
</div>

{html_table(
    session_mtd_ly,
    percent_columns=[
        "Net Growth %",
        "Orders Growth %",
        "Dis % Change"
    ]
)}


<!-- =====================================================
     TOP BRANCHES
     ===================================================== -->

<div class="section-title">
    🏪 Top 10 Branches - FTD vs LW
</div>

{html_table(
    top_branch_ftd_lw,
    percent_columns=[
        "Net Growth %",
        "Orders Growth %",
        "Dis % Change"
    ]
)}


<div class="period-title">
    Top 10 Branches - FTD vs LM
</div>

{html_table(
    top_branch_ftd_lm,
    percent_columns=[
        "Net Growth %",
        "Orders Growth %",
        "Dis % Change"
    ]
)}


<div class="period-title">
    Top 10 Branches - MTD vs LM MTD
</div>

{html_table(
    top_branch_mtd_lm,
    percent_columns=[
        "Net Growth %",
        "Orders Growth %",
        "Dis % Change"
    ]
)}

<!-- =====================================================
     DAY LEVEL PERFORMANCE
     ===================================================== -->

<div class="section-title">
    📅 Day Level Performance - FTD vs LW
</div>

{html_table(
    day_level_ftd_lw,
    percent_columns=[
        "Net Growth %",
        "Orders Growth %",
        "Dis % Change"
    ]
)}


<div class="period-title">
    Day Level Performance - FTD vs LM
</div>

{html_table(
    day_level_ftd_lm,
    percent_columns=[
        "Net Growth %",
        "Orders Growth %",
        "Dis % Change"
    ]
)}


<div class="period-title">
    Day Level Performance - MTD vs LM MTD
</div>

{html_table(
    day_level_mtd_lm,
    percent_columns=[
        "Net Growth %",
        "Orders Growth %",
        "Dis % Change"
    ]
)}

<!-- =====================================================
     FOOTER
     ===================================================== -->

<div class="footer">

    Generated automatically from Rista monthly CSV data.

    <br>

    FTD Date:
    {ftd_date.strftime("%d-%b-%Y")}

    <br>

    Data through:
    {ftd_date.strftime("%d-%b-%Y")}

</div>


</div>

</body>

</html>
"""


# =========================================================
# SEND EMAIL
# =========================================================

def send_mail(
    subject,
    html_body
):

    msg = MIMEMultipart(
        "alternative"
    )


    msg["From"] = EMAIL

    msg["To"] = ", ".join(
        TO
    )


    if CC:

        msg["Cc"] = ", ".join(
            CC
        )


    msg["Subject"] = subject


    msg.attach(
        MIMEText(
            html_body,
            "html",
            "utf-8"
        )
    )


    recipients = (
        TO + CC
    )


    print("=" * 80)
    print("SENDING EMAIL")
    print("=" * 80)

    print(
        "To:",
        recipients
    )


    with smtplib.SMTP(
        SMTP_SERVER,
        SMTP_PORT,
        timeout=60
    ) as server:

        server.ehlo()

        server.starttls()

        server.ehlo()

        server.login(
            EMAIL,
            PASSWORD
        )

        server.sendmail(
            EMAIL,
            recipients,
            msg.as_string()
        )


    print(
        "✅ Dashboard Mail Sent"
    )


# =========================================================
# SEND
# =========================================================

subject = (
    "📊 DSR Dashboard | "
    f"FTD & MTD | "
    f"{ftd_date.strftime('%d-%b-%Y')}"
)


send_mail(
    subject,
    body
)


# =========================================================
# FINAL LOG
# =========================================================

print("=" * 80)
print("🏁 DSR DASHBOARD COMPLETED")
print("=" * 80)

print(
    "FTD:",
    ftd_date
)

print(
    "MTD:",
    current_month_start,
    "→",
    ftd_date
)

print(
    "LW:",
    lw_date
)

print(
    "LM:",
    lm_date
)

print(
    "LY:",
    ly_date
)

print("=" * 80)
