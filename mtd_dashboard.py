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
# SESSION NORMALIZATION
# Breakfast > Lunch > Snacks > Dinner > Post Dinner
# Late Night and Closing -> Post Dinner
# =========================================================

def normalize_session(value):
    s = str(value).strip().lower()
    if "breakfast" in s: return "Breakfast"
    if "lunch" in s: return "Lunch"
    if "snack" in s: return "Snacks"
    if "late night" in s or "latenight" in s or "closing" in s or "post dinner" in s: return "Post Dinner"
    if "dinner" in s: return "Dinner"
    return str(value).strip() if str(value).strip() else "Others"

final_df["Session"] = final_df["Session"].apply(normalize_session)

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

# Last Week - Same Day
lw_date = (
    ftd_date
    - timedelta(days=7)
)


# Last Month - Same Day
lm_date = (
    pd.Timestamp(ftd_date)
    - pd.DateOffset(months=1)
).date()


# Last Year - Same Day
ly_date = (
    pd.Timestamp(ftd_date)
    - pd.DateOffset(years=1)
).date()


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

# Current MTD = 1st -> yesterday
current_month_start = pd.Timestamp(
    year=ftd_date.year,
    month=ftd_date.month,
    day=1
).date()


def same_weekday_previous_month(current_date):
    current_date = pd.Timestamp(current_date)
    target = current_date - pd.DateOffset(months=1)
    while target.weekday() != current_date.weekday():
        target -= pd.Timedelta(days=1)
    return target.date()


# FTD LM = same weekday in previous month
lm_date = same_weekday_previous_month(ftd_date)

# FTD LY = same weekday last year
ly_date = (
    pd.Timestamp(ftd_date)
    - pd.Timedelta(days=364)
).date()

lm_month_start = pd.Timestamp(
    year=lm_date.year,
    month=lm_date.month,
    day=1
).date()

ly_month_start = pd.Timestamp(
    year=ly_date.year,
    month=ly_date.month,
    day=1
).date()


def safe_month_end(year, month, day):
    first = pd.Timestamp(year=year, month=month, day=1)
    next_month = first + pd.DateOffset(months=1)
    last_day = (next_month - pd.Timedelta(days=1)).day
    return pd.Timestamp(
        year=year,
        month=month,
        day=min(day, last_day)
    ).date()


lm_mtd_end = safe_month_end(
    lm_date.year,
    lm_date.month,
    ftd_date.day
)

ly_mtd_end = safe_month_end(
    ly_date.year,
    ly_date.month,
    ftd_date.day
)

print("=" * 80)
print("MTD PERIODS")
print("=" * 80)
print("Current MTD:", current_month_start, "→", ftd_date)
print("LM MTD     :", lm_month_start, "→", lm_mtd_end)
print("LY MTD     :", ly_month_start, "→", ly_mtd_end)


# =========================================================
# DATE FILTER HELPER
# =========================================================

def date_filter(start_date, end_date):
    start = pd.Timestamp(start_date)
    end = pd.Timestamp(end_date)
    return final_df.loc[
        (final_df["Date"].dt.normalize() >= start)
        &
        (final_df["Date"].dt.normalize() <= end)
    ].copy()


def single_date_filter(target_date):
    target = pd.Timestamp(target_date)
    return final_df.loc[
        final_df["Date"].dt.normalize().eq(target)
    ].copy()


# =========================================================
# FTD DATASETS
# =========================================================

ftd_df = single_date_filter(ftd_date)
lw_df = single_date_filter(lw_date)
lm_df = single_date_filter(lm_date)
ly_df = single_date_filter(ly_date)


# =========================================================
# MTD DATASETS - 1ST TO YESTERDAY / SAME DAY
# =========================================================

mtd_df = date_filter(current_month_start, ftd_date)
lm_mtd_df = date_filter(lm_month_start, lm_mtd_end)
ly_mtd_df = date_filter(ly_month_start, ly_mtd_end)

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
# STORE TYPE KPI
# =========================================================

def store_type_kpi(
    df,
    store_type
):

    return get_kpi(
        df[
            df["Store Type"]
            == store_type
        ].copy()
    )


# =========================================================
# FTD STORE TYPE
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
# MTD STORE TYPE
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

def filter_coco(df):
    return df[
        df["Store Type"]
        .astype(str)
        .str.strip()
        .str.upper()
        .eq("COCO")
    ].copy()


ftd_coco_df = filter_coco(ftd_df)
lw_coco_df = filter_coco(lw_df)
lm_coco_df = filter_coco(lm_df)
ly_coco_df = filter_coco(ly_df)

mtd_coco_df = filter_coco(mtd_df)
lm_mtd_coco_df = filter_coco(lm_mtd_df)
ly_mtd_coco_df = filter_coco(ly_mtd_df)

# =========================================================
# FULL PREVIOUS MONTHS FOR DAY-LEVEL COMPARISONS
# =========================================================

lm_full_month_coco_df = filter_coco(
    date_filter(
        lm_month_start,
        (
            pd.Timestamp(
                lm_month_start
            )
            + pd.offsets.MonthEnd(1)
        ).date()
    )
)


ly_full_month_coco_df = filter_coco(
    date_filter(
        ly_month_start,
        (
            pd.Timestamp(
                ly_month_start
            )
            + pd.offsets.MonthEnd(1)
        ).date()
    )
)

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
# SESSION DISPLAY NORMALIZATION
# Breakfast > Lunch > Snacks > Dinner > Post Dinner
# Late Night and Closing -> Post Dinner
# =========================================================

def normalize_session(value):
    s = str(value).strip().lower()
    if "breakfast" in s:
        return "Breakfast"
    if "lunch" in s:
        return "Lunch"
    if "snack" in s:
        return "Snacks"
    if "late night" in s or "latenight" in s or "closing" in s or "post dinner" in s:
        return "Post Dinner"
    if "dinner" in s:
        return "Dinner"
    return str(value).strip() if str(value).strip() else "Others"

final_df["Session"] = final_df["Session"].apply(normalize_session)




# =========================================================
# DAY-LEVEL COMPARISON
# =========================================================

def build_day_level_comparison(current_df, previous_df, days_back):

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

    current["Date"] = pd.to_datetime(current["Date"]).dt.normalize()
    previous["Date"] = pd.to_datetime(previous["Date"]).dt.normalize()

    current["Prev Date"] = current["Date"] - pd.to_timedelta(days_back, unit="D")
    previous = previous.rename(columns={"Date":"Prev Date"})

    result = current.merge(previous, on="Prev Date", how="left")

    for col in ["Prev_Gross","Prev_Net","Prev_Orders","Prev_Discount"]:
        result[col] = pd.to_numeric(result[col], errors="coerce").fillna(0)

    result["Gross Growth %"] = (
        (result["Gross"] - result["Prev_Gross"])
        / result["Prev_Gross"].replace(0, pd.NA)
    ) * 100

    result["Net Growth %"] = (
        (result["Net"] - result["Prev_Net"])
        / result["Prev_Net"].replace(0, pd.NA)
    ) * 100

    result["Orders Growth %"] = (
        (result["Orders"] - result["Prev_Orders"])
        / result["Prev_Orders"].replace(0, pd.NA)
    ) * 100

    result["Dis % Change"] = (
        result["Discount"] / result["Gross"].replace(0, 1) * 100
    ) - (
        result["Prev_Discount"] / result["Prev_Gross"].replace(0, 1) * 100
    )

    result["AOV"] = result["Net"] / result["Orders"].replace(0, 1)

    result = result[
        [
            "Date", "Gross", "Net", "Orders", "Discount", "Prev Date",
            "Prev_Gross", "Prev_Net", "Prev_Orders",
            "Gross Growth %", "Net Growth %", "Orders Growth %",
            "Dis % Change", "AOV"
        ]
    ].rename(
        columns={
            "Prev_Gross":"Prev Gross",
            "Prev_Net":"Prev Net",
            "Prev_Orders":"Prev Orders"
        }
    )

    result["Date"] = result["Date"].dt.strftime("%-d-%b")
    result["Prev Date"] = result["Prev Date"].dt.strftime("%-d-%b")

    return result.sort_values("Date", ascending=False).reset_index(drop=True)


# FTD day-level
day_level_ftd_lw = build_day_level_comparison(ftd_coco_df, lw_coco_df, 7)
day_level_ftd_lm = build_day_level_comparison(ftd_coco_df, lm_full_month_coco_df, 28)
day_level_ftd_ly = build_day_level_comparison(ftd_coco_df, ly_full_month_coco_df, 364)

# MTD day-level
day_level_mtd_lm = build_day_level_comparison(mtd_coco_df, lm_full_month_coco_df, 28)
day_level_mtd_ly = build_day_level_comparison(mtd_coco_df, ly_full_month_coco_df, 364)


# =========================================================
# INSIGHTS
# =========================================================

def insight_block(title, df):
    if df is None or df.empty or "Net Growth %" not in df.columns:
        return ""

    work = df.copy()
    work["Net Growth %"] = pd.to_numeric(work["Net Growth %"], errors="coerce").fillna(0)

    positive = work[work["Net Growth %"] > 0].sort_values("Net Growth %", ascending=False).head(2)
    negative = work[work["Net Growth %"] < 0].sort_values("Net Growth %").head(2)

    html = []
    for _, row in positive.iterrows():
        html.append(
            f'<li>{title}: <span class="insight-good">{row[title]} +{row["Net Growth %"]:.1f}%</span> vs LW</li>'
        )
    for _, row in negative.iterrows():
        html.append(
            f'<li>{title}: <span class="insight-bad">{row[title]} {row["Net Growth %"]:.1f}%</span> vs LW - improvement needed</li>'
        )
    return "".join(html)


def build_insights():
    blocks = [
        insight_block("Source", source_ftd_lw),
        insight_block("Session", session_ftd_lw),
        insight_block("Region", region_ftd_lw)
    ]
    blocks = [x for x in blocks if x]
    if not blocks:
        return "<p style='color:#777;'>No comparison data available for insights.</p>"
    return '<ul class="insight-list">' + ''.join(blocks) + '</ul>'


# =========================================================
# DASHBOARD CHART HELPERS
# EMAIL-SAFE VERSION
# =========================================================

def _safe_num(value):

    try:

        if pd.isna(value):
            return 0.0

        return float(value)

    except Exception:

        return 0.0


def _short_value(value):

    value = _safe_num(value)

    if abs(value) >= 10000000:
        return f"₹{value / 10000000:.1f}Cr"

    if abs(value) >= 100000:
        return f"₹{value / 100000:.1f}L"

    if abs(value) >= 1000:
        return f"₹{value / 1000:.1f}K"

    return f"₹{value:,.0f}"


# =========================================================
# EMAIL-SAFE HORIZONTAL BAR CHART
# =========================================================

def horizontal_bar_chart(
    df,
    category_col,
    value_col,
    title,
    max_rows=6
):

    if df is None or df.empty:
        return ""

    if category_col not in df.columns:
        return ""

    if value_col not in df.columns:
        return ""

    work = df.copy()

    work[value_col] = pd.to_numeric(
        work[value_col],
        errors="coerce"
    ).fillna(0)

    work = (
        work[
            [category_col, value_col]
        ]
        .groupby(
            category_col,
            as_index=False
        )[value_col]
        .sum()
        .sort_values(
            value_col,
            ascending=False
        )
        .head(max_rows)
        .reset_index(drop=True)
    )

    if work.empty:
        return ""

    maximum = max(
        float(work[value_col].max()),
        1
    )

    rows = []

    for _, row in work.iterrows():

        name = str(
            row[category_col]
        )

        value = _safe_num(
            row[value_col]
        )

        percentage = (
            value / maximum
        ) * 100

        percentage = max(
            0,
            min(
                percentage,
                100
            )
        )

        # -------------------------------------------------
        # EMAIL-SAFE BAR
        # -------------------------------------------------

        rows.append(
            f"""
            <tr>

                <td
                    width="105"
                    style="
                        width:105px;
                        padding:7px 8px 7px 0;
                        font-family:Arial, sans-serif;
                        font-size:11px;
                        color:#3f4650;
                        white-space:nowrap;
                    "
                >
                    {name}
                </td>

                <td
                    style="
                        padding:7px 5px;
                    "
                >

                    <table
                        role="presentation"
                        width="100%"
                        cellpadding="0"
                        cellspacing="0"
                        border="0"
                        style="
                            width:100%;
                        "
                    >

                        <tr>

                            <td
                                style="
                                    background:#EEF1F4;
                                    height:18px;
                                    padding:0;
                                    font-size:0;
                                    line-height:0;
                                "
                            >

                                <table
                                    role="presentation"
                                    width="{percentage:.0f}%"
                                    cellpadding="0"
                                    cellspacing="0"
                                    border="0"
                                    style="
                                        width:{percentage:.0f}%;
                                    "
                                >

                                    <tr>

                                        <td
                                            style="
                                                background:#2E8B57;
                                                height:18px;
                                                padding:0;
                                                font-size:0;
                                                line-height:0;
                                            "
                                        >
                                            &nbsp;
                                        </td>

                                    </tr>

                                </table>

                            </td>

                        </tr>

                    </table>

                </td>

                <td
                    width="70"
                    style="
                        width:70px;
                        padding:7px 0 7px 8px;
                        font-family:Arial, sans-serif;
                        font-size:11px;
                        font-weight:bold;
                        color:#333333;
                        text-align:right;
                        white-space:nowrap;
                    "
                >
                    {_short_value(value)}
                </td>

            </tr>
            """
        )

    return f"""
    <table
        role="presentation"
        width="100%"
        cellpadding="0"
        cellspacing="0"
        border="0"
        style="
            width:100%;
            background:#FFFFFF;
            border:1px solid #D9DEE3;
            border-radius:8px;
        "
    >

        <tr>

            <td
                style="
                    padding:16px;
                "
            >

                <div
                    style="
                        font-family:Arial,sans-serif;
                        font-size:15px;
                        font-weight:bold;
                        color:#243447;
                        margin-bottom:5px;
                    "
                >
                    {title}
                </div>

                <div
                    style="
                        font-family:Arial,sans-serif;
                        font-size:11px;
                        color:#777777;
                        padding-bottom:10px;
                    "
                >
                    Net Revenue
                </div>

                <table
                    role="presentation"
                    width="100%"
                    cellpadding="0"
                    cellspacing="0"
                    border="0"
                    style="
                        width:100%;
                    "
                >

                    {''.join(rows)}

                </table>

            </td>

        </tr>

    </table>
    """


# =========================================================
# EMAIL-SAFE DAILY MTD BAR CHART
# =========================================================

def daily_mtd_chart(
    df,
    title="MTD Net Revenue Trend"
):

    if df is None or df.empty:
        return ""

    if "Date" not in df.columns:
        return ""

    if "Net Sales" not in df.columns:
        return ""

    work = df.copy()

    work["Date"] = pd.to_datetime(
        work["Date"],
        errors="coerce"
    )

    work["Net Sales"] = pd.to_numeric(
        work["Net Sales"],
        errors="coerce"
    ).fillna(0)

    work = (
        work
        .dropna(
            subset=["Date"]
        )
        .groupby(
            "Date",
            as_index=False
        )["Net Sales"]
        .sum()
        .sort_values("Date")
        .tail(14)
        .reset_index(drop=True)
    )

    if work.empty:
        return ""

    maximum = max(
        float(
            work["Net Sales"].max()
        ),
        1
    )

    columns = []

    for _, row in work.iterrows():

        value = _safe_num(
            row["Net Sales"]
        )

        percentage = (
            value / maximum
        ) * 100

        percentage = max(
            3,
            min(
                percentage,
                100
            )
        )

        date_label = row[
            "Date"
        ].strftime(
            "%-d-%b"
        )

        # -------------------------------------------------
        # EMAIL-SAFE VERTICAL BAR
        # -------------------------------------------------

        columns.append(
            f"""
            <td
                width="7%"
                valign="bottom"
                align="center"
                style="
                    width:7%;
                    padding:0 2px;
                    vertical-align:bottom;
                "
            >

                <table
                    role="presentation"
                    width="100%"
                    cellpadding="0"
                    cellspacing="0"
                    border="0"
                    style="
                        width:100%;
                    "
                >

                    <tr>

                        <td
                            valign="bottom"
                            align="center"
                            height="125"
                            style="
                                height:125px;
                                vertical-align:bottom;
                                padding:0;
                            "
                        >

                            <table
                                role="presentation"
                                width="100%"
                                cellpadding="0"
                                cellspacing="0"
                                border="0"
                                style="
                                    width:100%;
                                "
                            >

                                <tr>

                                    <td
                                        height="{max(int(115 * percentage / 100), 5)}"
                                        style="
                                            height:{max(int(115 * percentage / 100), 5)}px;
                                            background:#6CC9CE;
                                            font-size:0;
                                            line-height:0;
                                            border-radius:3px 3px 0 0;
                                        "
                                    >
                                        &nbsp;
                                    </td>

                                </tr>

                            </table>

                        </td>

                    </tr>

                    <tr>

                        <td
                            style="
                                padding-top:5px;
                                font-family:Arial,sans-serif;
                                font-size:9px;
                                color:#666666;
                                text-align:center;
                                white-space:nowrap;
                            "
                        >
                            {date_label}
                        </td>

                    </tr>

                    <tr>

                        <td
                            style="
                                padding-top:3px;
                                font-family:Arial,sans-serif;
                                font-size:8px;
                                color:#444444;
                                text-align:center;
                                white-space:nowrap;
                            "
                        >
                            {_short_value(value)}
                        </td>

                    </tr>

                </table>

            </td>
            """
        )

    return f"""
    <table
        role="presentation"
        width="100%"
        cellpadding="0"
        cellspacing="0"
        border="0"
        style="
            width:100%;
            background:#FFFFFF;
            border:1px solid #D9DEE3;
            border-radius:8px;
        "
    >

        <tr>

            <td
                style="
                    padding:16px;
                "
            >

                <div
                    style="
                        font-family:Arial,sans-serif;
                        font-size:15px;
                        font-weight:bold;
                        color:#243447;
                        padding-bottom:5px;
                    "
                >
                    {title}
                </div>

                <div
                    style="
                        font-family:Arial,sans-serif;
                        font-size:11px;
                        color:#777777;
                        padding-bottom:12px;
                    "
                >
                    Daily Net Revenue
                </div>

                <table
                    role="presentation"
                    width="100%"
                    cellpadding="0"
                    cellspacing="0"
                    border="0"
                    style="
                        width:100%;
                    "
                >

                    <tr>

                        {''.join(columns)}

                    </tr>

                </table>

            </td>

        </tr>

    </table>
    """


# =========================================================
# PREPARE CHART DATA
# =========================================================

chart_mtd_daily = (
    mtd_coco_df.copy()
)


chart_source_mtd = (
    mtd_coco_df
    .groupby(
        "Source",
        as_index=False
    )["Net Sales"]
    .sum()
)


chart_brand_mtd = (
    mtd_coco_df
    .groupby(
        "Brand Name",
        as_index=False
    )["Net Sales"]
    .sum()
)


chart_region_mtd = (
    mtd_coco_df
    .groupby(
        "Region",
        as_index=False
    )["Net Sales"]
    .sum()
)


chart_session_mtd = (
    mtd_coco_df
    .groupby(
        "Session",
        as_index=False
    )["Net Sales"]
    .sum()
)


# =========================================================
# BUILD CHART HTML
# =========================================================

chart_daily_html = daily_mtd_chart(
    chart_mtd_daily,
    "MTD Net Revenue Trend"
)


chart_source_html = horizontal_bar_chart(
    chart_source_mtd,
    "Source",
    "Net Sales",
    "MTD Revenue by Source",
    6
)


chart_brand_html = horizontal_bar_chart(
    chart_brand_mtd,
    "Brand Name",
    "Net Sales",
    "MTD Revenue by Brand",
    6
)


chart_region_html = horizontal_bar_chart(
    chart_region_mtd,
    "Region",
    "Net Sales",
    "MTD Revenue by Region",
    6
)


chart_session_html = horizontal_bar_chart(
    chart_session_mtd,
    "Session",
    "Net Sales",
    "MTD Revenue by Session",
    6
)

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
                "Orders"
            ]
        ]
        .rename(
            columns={
                "Net":
                    "Previous Net",
                "Orders":
                    "Previous Orders"
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
            "Previous Orders"
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
# INSIGHT BLOCK
# =========================================================

def insight_block(
    title,
    df
):

    if df is None or df.empty:
        return ""

    work = df.copy()

    if "Net Growth %" not in work.columns:
        return ""

    # -----------------------------------------------------
    # Sort by growth
    # -----------------------------------------------------

    work["Net Growth %"] = pd.to_numeric(
        work["Net Growth %"],
        errors="coerce"
    ).fillna(0)

    # -----------------------------------------------------
    # Best performers
    # -----------------------------------------------------

    best = (
        work
        .sort_values(
            "Net Growth %",
            ascending=False
        )
        .head(3)
    )

    # -----------------------------------------------------
    # Areas needing improvement
    # -----------------------------------------------------

    weak = (
        work
        .sort_values(
            "Net Growth %",
            ascending=True
        )
        .head(3)
    )

    html = []

    html.append(
        f"""
        <li class="insight-heading">
            {title}
        </li>
        """
    )

    # -----------------------------------------------------
    # Growth
    # -----------------------------------------------------

    for _, row in best.iterrows():

        growth = float(
            row["Net Growth %"]
        )

        if growth <= 0:
            continue

        # Determine dimension name
        dimension = ""

        for col in [
            "Source",
            "Session",
            "Region",
            "Brand Name",
            "Branch"
        ]:

            if col in work.columns:
                dimension = str(
                    row[col]
                )
                break

        html.append(
            f"""
            <li>
                <span class="insight-good">
                    ✅ {dimension}
                </span>

                grew
                <strong>
                    {growth:+.1f}%
                </strong>
                vs LW.
            </li>
            """
        )

    # -----------------------------------------------------
    # Improvement
    # -----------------------------------------------------

    for _, row in weak.iterrows():

        growth = float(
            row["Net Growth %"]
        )

        if growth >= 0:
            continue

        dimension = ""

        for col in [
            "Source",
            "Session",
            "Region",
            "Brand Name",
            "Branch"
        ]:

            if col in work.columns:
                dimension = str(
                    row[col]
                )
                break

        html.append(
            f"""
            <li>
                <span class="insight-bad">
                    ⚠️ {dimension}
                </span>

                declined
                <strong>
                    {growth:+.1f}%
                </strong>
                vs LW —
                improvement required.
            </li>
            """
        )

    return "".join(
        html
    )


# =========================================================
# BUILD INSIGHTS
# =========================================================

def build_insights():

    sections = []

    sections.append(
        insight_block(
            "Source",
            source_ftd_lw
        )
    )

    sections.append(
        insight_block(
            "Session",
            session_ftd_lw
        )
    )

    sections.append(
        insight_block(
            "Region",
            region_ftd_lw
        )
    )

    content = "".join(
        x
        for x in sections
        if x
    )

    if not content:

        return """
        <p style="color:#777;">
            No comparison data available for insights.
        </p>
        """

    return (
        "<ul class='insight-list'>"
        f"{content}"
        "</ul>"
    )


# =========================================================
# GENERATE INSIGHTS HTML
# =========================================================

insights_html = build_insights()


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
        ftd_kpi["Gross"],
        ftd_kpi["Net"],
        ftd_kpi["Discount"],
        ftd_kpi["Orders"],
        ftd_kpi["AOV"],
        ftd_kpi["Dis %"]
    ],

    "MTD": [
        mtd_kpi["Gross"],
        mtd_kpi["Net"],
        mtd_kpi["Discount"],
        mtd_kpi["Orders"],
        mtd_kpi["AOV"],
        mtd_kpi["Dis %"]
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
        "Orders"
    ],

    "FTD": [
        ftd_kpi["Gross"],
        ftd_kpi["Net"],
        ftd_kpi["Orders"]
    ],

    "LW Same Day": [
        lw_kpi["Gross"],
        lw_kpi["Net"],
        lw_kpi["Orders"]
    ],

    "FTD vs LW %": [
        ftd_lw_growth["Gross %"],
        ftd_lw_growth["Net %"],
        ftd_lw_growth["Orders %"]
    ],

    "LM Same Day": [
        lm_kpi["Gross"],
        lm_kpi["Net"],
        lm_kpi["Orders"]
    ],

    "FTD vs LM %": [
        ftd_lm_growth["Gross %"],
        ftd_lm_growth["Net %"],
        ftd_lm_growth["Orders %"]
    ],

    "LY Same Day": [
        ly_kpi["Gross"],
        ly_kpi["Net"],
        ly_kpi["Orders"]
    ],

    "FTD vs LY %": [
        ftd_ly_growth["Gross %"],
        ftd_ly_growth["Net %"],
        ftd_ly_growth["Orders %"]
    ]

})


# =========================================================
# MTD COMPARISON TABLE
# =========================================================

mtd_comparison_table = pd.DataFrame({

    "Metric": [
        "Gross Revenue",
        "Net Revenue",
        "Orders"
    ],

    "Current MTD": [
        mtd_kpi["Gross"],
        mtd_kpi["Net"],
        mtd_kpi["Orders"]
    ],

    "LM MTD": [
        lm_mtd_kpi["Gross"],
        lm_mtd_kpi["Net"],
        lm_mtd_kpi["Orders"]
    ],

    "MTD vs LM %": [
        mtd_lm_growth["Gross %"],
        mtd_lm_growth["Net %"],
        mtd_lm_growth["Orders %"]
    ],

    "LY MTD": [
        ly_mtd_kpi["Gross"],
        ly_mtd_kpi["Net"],
        ly_mtd_kpi["Orders"]
    ],

    "MTD vs LY %": [
        mtd_ly_growth["Gross %"],
        mtd_ly_growth["Net %"],
        mtd_ly_growth["Orders %"]
    ]

})


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

    percent_columns = (
        percent_columns or []
    )

    headers = list(df.columns)

    html = [
        '<table class="data-table">',
        '<thead><tr>'
    ]

    for col in headers:
        html.append(
            f"<th>{col}</th>"
        )

    html.append(
        "</tr></thead><tbody>"
    )

    for _, row in df.iterrows():

        html.append("<tr>")

        for col in headers:

            value = row[col]

            # -------------------------------------------------
            # DATE
            # -------------------------------------------------

            if col == "Date":

                try:
                    display = pd.to_datetime(
                        value
                    ).strftime("%-d-%b")

                except Exception:
                    display = str(value)

                html.append(
                    f"<td>{display}</td>"
                )

                continue

            # -------------------------------------------------
            # EMPTY
            # -------------------------------------------------

            if pd.isna(value):

                html.append(
                    "<td></td>"
                )

                continue

            # -------------------------------------------------
            # PERCENT
            # -------------------------------------------------

            if col in percent_columns:

                number = _safe_num(value)

                display = (
                    f"{number:,.1f}%"
                )

                lower_col = (
                    col.lower()
                )

                style = ""

                # -------------------------------------------------
                # GROWTH %
                # Positive = Green
                # Negative = Red
                # -------------------------------------------------

                if (
                    "growth" in lower_col
                ):

                    if number > 0:

                        style = (
                            ' style="'
                            'background:#E8F5E9;'
                            'color:#2E7D32;'
                            'font-weight:bold;'
                            '"'
                        )

                    elif number < 0:

                        style = (
                            ' style="'
                            'background:#FFEBEE;'
                            'color:#C62828;'
                            'font-weight:bold;'
                            '"'
                        )

                # -------------------------------------------------
                # DISCOUNT CHANGE
                # Lower discount = Green
                # Higher discount = Red
                # -------------------------------------------------

                elif (
                    "dis %" in lower_col
                    or
                    "discount" in lower_col
                ):

                    if number < 0:

                        style = (
                            ' style="'
                            'background:#E8F5E9;'
                            'color:#2E7D32;'
                            'font-weight:bold;'
                            '"'
                        )

                    elif number > 0:

                        style = (
                            ' style="'
                            'background:#FFEBEE;'
                            'color:#C62828;'
                            'font-weight:bold;'
                            '"'
                        )

                html.append(
                    f"<td{style}>{display}</td>"
                )

                continue

            # -------------------------------------------------
            # NUMERIC
            # -------------------------------------------------

            if (
                isinstance(
                    value,
                    (int, float)
                )
                or
                pd.api.types.is_number(value)
            ):

                display = (
                    f"{float(value):,.2f}"
                )

            else:

                display = str(value)

            html.append(
                f"<td>{display}</td>"
            )

        html.append("</tr>")

    html.append(
        "</tbody></table>"
    )

    return "".join(html)

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
# INSIGHTS
# =========================================================

def insight_block(title, df):
    if df is None or df.empty or "Net Growth %" not in df.columns:
        return ""

    work = df.copy()
    work["Net Growth %"] = pd.to_numeric(work["Net Growth %"], errors="coerce").fillna(0)
    positive = work[work["Net Growth %"] > 0].sort_values("Net Growth %", ascending=False).head(2)
    negative = work[work["Net Growth %"] < 0].sort_values("Net Growth %").head(2)

    parts = []
    for _, row in positive.iterrows():
        parts.append(f'<li>{title}: <span class="insight-good">{row[title]} +{row["Net Growth %"]:.1f}%</span> vs LW</li>')
    for _, row in negative.iterrows():
        parts.append(f'<li>{title}: <span class="insight-bad">{row[title]} {row["Net Growth %"]:.1f}%</span> vs LW - improvement needed</li>')
    return "".join(parts)


def build_insights():
    blocks = [
        insight_block("Source", source_ftd_lw),
        insight_block("Session", session_ftd_lw),
        insight_block("Region", region_ftd_lw)
    ]
    blocks = [x for x in blocks if x]
    if not blocks:
        return "<p style='color:#777;'>No comparison data available for insights.</p>"
    return '<ul class="insight-list">' + ''.join(blocks) + '</ul>'


insights_html = build_insights()


# =========================================================
# CHART HELPERS
# =========================================================

def _safe_num(value):
    try:
        if pd.isna(value):
            return 0.0
        return float(value)
    except Exception:
        return 0.0


def _short_value(value):
    value = _safe_num(value)
    if abs(value) >= 10_000_000:
        return f"₹{value/10_000_000:.1f}Cr"
    if abs(value) >= 100_000:
        return f"₹{value/100_000:.1f}L"
    if abs(value) >= 1_000:
        return f"₹{value/1_000:.1f}K"
    return f"₹{value:,.0f}"


def horizontal_bar_chart(df, category_col, value_col, title, max_rows=6):
    if df is None or df.empty:
        return ""
    if category_col not in df.columns or value_col not in df.columns:
        return ""

    work = df[[category_col, value_col]].copy()
    work[value_col] = pd.to_numeric(work[value_col], errors="coerce").fillna(0)
    work = work.sort_values(value_col, ascending=False).head(max_rows)
    maximum = max(work[value_col].max(), 1)

    rows = []
    for _, row in work.iterrows():
        label = str(row[category_col])
        value = _safe_num(row[value_col])
        width = max(0, min(100, value / maximum * 100))
        rows.append(
            f'<div class="bar-row"><div class="bar-label">{label}</div>'
            f'<div class="bar-track"><div class="bar-fill" style="width:{width:.1f}%"></div></div>'
            f'<div class="bar-value">{_short_value(value)}</div></div>'
        )

    return (
        f'<div class="chart-card"><div class="chart-title">{title}</div>'
        f'<div class="chart-body">{"".join(rows)}</div></div>'
    )


def daily_mtd_chart(df, title="MTD Net Revenue Trend"):
    if df is None or df.empty or "Date" not in df.columns or "Net Sales" not in df.columns:
        return ""

    work = df.copy()
    work["Date"] = pd.to_datetime(work["Date"], errors="coerce")
    work["Net Sales"] = pd.to_numeric(work["Net Sales"], errors="coerce").fillna(0)
    work = (
        work.dropna(subset=["Date"])
        .groupby("Date", as_index=False)["Net Sales"]
        .sum()
        .sort_values("Date")
        .tail(14)
        .reset_index(drop=True)
    )
    if work.empty:
        return ""

    width, height = 640, 250
    left, right, top, bottom = 40, 20, 30, 45
    chart_width = width - left - right
    chart_height = height - top - bottom
    maximum = max(work["Net Sales"].max(), 1)

    points = []
    for i, row in work.iterrows():
        x = left + (i / max(len(work)-1, 1)) * chart_width
        y = top + chart_height - (row["Net Sales"] / maximum) * chart_height
        points.append((x, y, row["Date"]))

    polyline = " ".join(f"{x:.1f},{y:.1f}" for x, y, _ in points)
    circles = ''.join(
        f'<circle cx="{x:.1f}" cy="{y:.1f}" r="4" fill="#2E8B57" />'
        for x, y, _ in points
    )
    labels = ''.join(
        f'<text x="{x:.1f}" y="{height-16}" text-anchor="middle" font-size="10" fill="#666">{d.strftime("%-d-%b")}</text>'
        for x, _, d in points
    )

    return (
        f'<div class="chart-card chart-wide"><div class="chart-title">{title}</div>'
        f'<svg viewBox="0 0 {width} {height}" width="100%" height="{height}">'
        f'<line x1="{left}" y1="{top}" x2="{width-right}" y2="{top}" stroke="#E5E7EB" />'
        f'<line x1="{left}" y1="{top+chart_height/2}" x2="{width-right}" y2="{top+chart_height/2}" stroke="#E5E7EB" />'
        f'<line x1="{left}" y1="{top+chart_height}" x2="{width-right}" y2="{top+chart_height}" stroke="#E5E7EB" />'
        f'<polyline points="{polyline}" fill="none" stroke="#2E8B57" stroke-width="3" />'
        f'{circles}{labels}</svg></div>'
    )


chart_mtd_daily = mtd_coco_df.copy()
chart_source_mtd = mtd_coco_df.groupby("Source", as_index=False)["Net Sales"].sum()
chart_brand_mtd = mtd_coco_df.groupby("Brand Name", as_index=False)["Net Sales"].sum()
chart_region_mtd = mtd_coco_df.groupby("Region", as_index=False)["Net Sales"].sum()
chart_session_mtd = mtd_coco_df.groupby("Session", as_index=False)["Net Sales"].sum()

chart_daily_html = daily_mtd_chart(chart_mtd_daily, "MTD Net Revenue Trend")
chart_source_html = horizontal_bar_chart(chart_source_mtd, "Source", "Net Sales", "MTD Revenue by Source", 6)
chart_brand_html = horizontal_bar_chart(chart_brand_mtd, "Brand Name", "Net Sales", "MTD Revenue by Brand", 6)
chart_region_html = horizontal_bar_chart(chart_region_mtd, "Region", "Net Sales", "MTD Revenue by Region", 6)
chart_session_html = horizontal_bar_chart(chart_session_mtd, "Session", "Net Sales", "MTD Revenue by Session", 6)

# =========================================================
# EMAIL HTML
# =========================================================

body = f"""
<!DOCTYPE html>
<html>
<head>
<meta charset="UTF-8">
<style>
body {{ font-family: Calibri, Arial, sans-serif; background:#F4F6F8; color:#222; margin:0; padding:20px; }}
.container {{ max-width:1400px; margin:auto; background:#fff; padding:20px; }}
.header {{ background:#243447; color:#fff; padding:18px; border-radius:8px; margin-bottom:20px; }}
.header-title {{ font-size:24px; font-weight:bold; }}
.header-subtitle {{ margin-top:6px; font-size:14px; }}
.section-title {{ background:#EAF2F8; color:#243447; padding:9px 12px; margin-top:22px; margin-bottom:10px; border-left:5px solid #243447; font-size:15px; font-weight:bold; }}
.period-title {{ background:#F4F6F8; color:#243447; padding:8px 12px; margin-top:16px; margin-bottom:10px; border-left:4px solid #6C757D; font-weight:bold; }}
.kpi-row {{ display:table; width:100%; table-layout:fixed; margin-bottom:18px; }}
.kpi-card {{ display:table-cell; width:16.66%; background:#fff; border:1px solid #D9DEE3; padding:12px; text-align:center; box-sizing:border-box; }}
.kpi-card + .kpi-card {{ border-left:8px solid #F4F6F8; }}
.kpi-title {{ font-size:12px; font-weight:bold; color:#555; }}
.period-label {{ font-size:10px; color:#777; margin-top:7px; }}
.kpi-value {{ font-size:19px; font-weight:bold; color:#243447; margin-top:3px; }}
.divider {{ border-top:1px solid #E5E7EB; margin:8px 0; }}
.insights {{ background:#F8FAFC; border:1px solid #D9DEE3; border-left:5px solid #2E8B57; padding:12px 16px; margin-bottom:18px; }}
.insight-list {{ margin:4px 0 0 18px; padding:0; }}
.insight-list li {{ margin:6px 0; font-size:12px; }}
.insight-good {{ color:#2E7D32; font-weight:bold; }}
.insight-bad {{ color:#C62828; font-weight:bold; }}
.note {{ font-size:11px; color:#666; margin-bottom:8px; }}
.data-table {{ border-collapse:collapse; width:100%; margin-bottom:18px; font-size:11px; }}
.data-table th {{ background:#243447; color:#fff; padding:7px; border:1px solid #243447; text-align:center; }}
.data-table td {{ padding:6px; border:1px solid #D9DEE3; text-align:center; }}
.data-table tr:nth-child(even) {{ background:#F7F8F9; }}
.chart-grid {{ display:table; width:100%; table-layout:fixed; border-spacing:12px 0; margin:0 -12px 18px -12px; width:calc(100% + 24px); }}
.chart-card {{ display:table-cell; width:50%; vertical-align:top; background:#fff; border:1px solid #D9DEE3; border-radius:10px; padding:16px; box-sizing:border-box; min-height:220px; }}
.chart-grid .chart-card + .chart-card {{ border-left:8px solid #F4F6F8; }}
.chart-wide {{ display:block; width:100%; }}
.chart-title {{ font-size:14px; font-weight:bold; color:#243447; margin-bottom:12px; }}
.chart-body {{ width:100%; }}
.bar-row {{ display:flex; align-items:center; gap:8px; margin:12px 0; }}
.bar-label {{ width:95px; min-width:95px; font-size:11px; color:#444; white-space:nowrap; overflow:hidden; text-overflow:ellipsis; }}
.bar-track {{ flex:1; height:18px; background:#EEF1F3; border-radius:3px; overflow:hidden; }}
.bar-fill {{ height:18px; background:#6CC9CE; border-radius:3px; }}
.bar-value {{ width:65px; min-width:65px; font-size:11px; font-weight:bold; text-align:right; color:#333; }}
.footer {{ margin-top:25px; padding-top:12px; border-top:1px solid #DDD; color:#777; font-size:10px; }}
@media only screen and (max-width:900px) {{
    .chart-grid {{ display:block; width:100%; margin:0 0 18px 0; }}
    .chart-card {{ display:block; width:100%; margin-bottom:14px; }}
    .chart-grid .chart-card + .chart-card {{ border-left:1px solid #D9DEE3; }}
}}
</style>
</head>
<body>
<div class="container">

<div class="header">
    <div class="header-title">🧊 Frozen Bottle DSR Dashboard</div>
    <div class="header-subtitle">
        FTD: {ftd_date.strftime("%d-%b-%Y")} &nbsp; | &nbsp;
        MTD: {current_month_start.strftime("%d-%b-%Y")} → {ftd_date.strftime("%d-%b-%Y")}
    </div>
</div>

<div class="section-title">💡 Yesterday COCO Sales Insights</div>
<div class="insights">
    <div class="note">Based on FTD {ftd_date.strftime("%d-%b-%Y")} vs LW same day.</div>
    {insights_html}
</div>

<div class="section-title">📌 FTD | MTD KPI</div>
<div class="kpi-row">
    {kpi_card("Gross Revenue", ftd_coco_kpi["Gross"], mtd_coco_kpi["Gross"], "₹")}
    {kpi_card("Net Revenue", ftd_coco_kpi["Net"], mtd_coco_kpi["Net"], "₹")}
    {kpi_card("Discount", ftd_coco_kpi["Discount"], mtd_coco_kpi["Discount"], "₹")}
    {kpi_card("Orders", ftd_coco_kpi["Orders"], mtd_coco_kpi["Orders"])}
    {kpi_card("AOV", ftd_coco_kpi["AOV"], mtd_coco_kpi["AOV"], "₹")}
    {kpi_card("Discount %", ftd_coco_kpi["Dis %"], mtd_coco_kpi["Dis %"], "", "%")}
</div>

<!-- =====================================================
     REVENUE DASHBOARD
     ===================================================== -->

<div class="section-title">
    📊 Revenue Dashboard
</div>


<!-- DAILY MTD TREND -->

<table
    role="presentation"
    width="100%"
    cellpadding="0"
    cellspacing="0"
    border="0"
>

    <tr>

        <td
            style="padding-bottom:14px;"
        >

            {chart_daily_html}

        </td>

    </tr>

</table>


<!-- SOURCE + BRAND -->

<table
    role="presentation"
    width="100%"
    cellpadding="0"
    cellspacing="0"
    border="0"
>

    <tr>

        <td
            width="50%"
            valign="top"
            style="
                width:50%;
                padding:0 6px 14px 0;
            "
        >

            {chart_source_html}

        </td>


        <td
            width="50%"
            valign="top"
            style="
                width:50%;
                padding:0 0 14px 6px;
            "
        >

            {chart_brand_html}

        </td>

    </tr>

</table>


<!-- REGION + SESSION -->

<table
    role="presentation"
    width="100%"
    cellpadding="0"
    cellspacing="0"
    border="0"
>

    <tr>

        <td
            width="50%"
            valign="top"
            style="
                width:50%;
                padding:0 6px 14px 0;
            "
        >

            {chart_region_html}

        </td>


        <td
            width="50%"
            valign="top"
            style="
                width:50%;
                padding:0 0 14px 6px;
            "
        >

            {chart_session_html}

        </td>

    </tr>

</table>


<div class="section-title">📊 KPI Summary</div>
{html_table(kpi_table, percent_columns=["Discount %"])}

<div class="section-title">🏢 COCO vs FOFO</div>
{html_table(store_type_table, percent_columns=["Discount %"])}

<div class="section-title">📅 FTD Comparison</div>
<div class="note">FTD: {ftd_date.strftime("%d-%b-%Y")} | LW: {lw_date.strftime("%d-%b-%Y")} | LM: {lm_date.strftime("%d-%b-%Y")} | LY: {ly_date.strftime("%d-%b-%Y")}</div>
{html_table(comparison_table, percent_columns=["FTD vs LW %", "FTD vs LM %", "FTD vs LY %"])}

<div class="section-title">📈 MTD Comparison</div>
<div class="note">
Current MTD: {current_month_start.strftime("%d-%b-%Y")} → {ftd_date.strftime("%d-%b-%Y")}<br>
LM MTD: {lm_month_start.strftime("%d-%b-%Y")} → {lm_mtd_end.strftime("%d-%b-%Y")}<br>
LY MTD: {ly_month_start.strftime("%d-%b-%Y")} → {ly_mtd_end.strftime("%d-%b-%Y")}
</div>
{html_table(mtd_comparison_table, percent_columns=["MTD vs LM %", "MTD vs LY %"])}

<div class="section-title">🏷 Brand Performance</div>
<div class="period-title">FTD vs LW</div>{html_table(brand_ftd_lw, percent_columns=["Net Growth %", "Orders Growth %"])}
<div class="period-title">FTD vs LM</div>{html_table(brand_ftd_lm, percent_columns=["Net Growth %", "Orders Growth %"])}
<div class="period-title">FTD vs LY</div>{html_table(brand_ftd_ly, percent_columns=["Net Growth %", "Orders Growth %"])}
<div class="period-title">MTD vs LM MTD</div>{html_table(brand_mtd_lm, percent_columns=["Net Growth %", "Orders Growth %"])}
<div class="period-title">MTD vs LY MTD</div>{html_table(brand_mtd_ly, percent_columns=["Net Growth %", "Orders Growth %"])}

<div class="section-title">🛒 Source Performance</div>
<div class="period-title">FTD vs LW</div>{html_table(source_ftd_lw, percent_columns=["Net Growth %", "Orders Growth %"])}
<div class="period-title">FTD vs LM</div>{html_table(source_ftd_lm, percent_columns=["Net Growth %", "Orders Growth %"])}
<div class="period-title">FTD vs LY</div>{html_table(source_ftd_ly, percent_columns=["Net Growth %", "Orders Growth %"])}
<div class="period-title">MTD vs LM MTD</div>{html_table(source_mtd_lm, percent_columns=["Net Growth %", "Orders Growth %"])}
<div class="period-title">MTD vs LY MTD</div>{html_table(source_mtd_ly, percent_columns=["Net Growth %", "Orders Growth %"])}

<div class="section-title">🌎 Region Performance</div>
<div class="period-title">FTD vs LW</div>{html_table(region_ftd_lw, percent_columns=["Net Growth %", "Orders Growth %"])}
<div class="period-title">FTD vs LM</div>{html_table(region_ftd_lm, percent_columns=["Net Growth %", "Orders Growth %"])}
<div class="period-title">FTD vs LY</div>{html_table(region_ftd_ly, percent_columns=["Net Growth %", "Orders Growth %"])}
<div class="period-title">MTD vs LM MTD</div>{html_table(region_mtd_lm, percent_columns=["Net Growth %", "Orders Growth %"])}
<div class="period-title">MTD vs LY MTD</div>{html_table(region_mtd_ly, percent_columns=["Net Growth %", "Orders Growth %"])}

<div class="section-title">🕒 Session Performance</div>
<div class="period-title">FTD vs LW</div>{html_table(session_ftd_lw, percent_columns=["Net Growth %", "Orders Growth %"])}
<div class="period-title">FTD vs LM</div>{html_table(session_ftd_lm, percent_columns=["Net Growth %", "Orders Growth %"])}
<div class="period-title">FTD vs LY</div>{html_table(session_ftd_ly, percent_columns=["Net Growth %", "Orders Growth %"])}
<div class="period-title">MTD vs LM MTD</div>{html_table(session_mtd_lm, percent_columns=["Net Growth %", "Orders Growth %"])}
<div class="period-title">MTD vs LY MTD</div>{html_table(session_mtd_ly, percent_columns=["Net Growth %", "Orders Growth %"])}

<div class="section-title">🏪 Top 10 Branches</div>
<div class="period-title">FTD vs LW</div>{html_table(top_branch_ftd_lw, percent_columns=["Net Growth %", "Orders Growth %"])}
<div class="period-title">FTD vs LM</div>{html_table(top_branch_ftd_lm, percent_columns=["Net Growth %", "Orders Growth %"])}
<div class="period-title">MTD vs LM MTD</div>{html_table(top_branch_mtd_lm, percent_columns=["Net Growth %", "Orders Growth %"])}

<div class="section-title">📅 Day Level Performance</div>
<div class="period-title">FTD vs LW</div>{html_table(day_level_ftd_lw, percent_columns=["Gross Growth %", "Net Growth %", "Orders Growth %", "Dis % Change"])}
<div class="period-title">FTD vs LM</div>{html_table(day_level_ftd_lm, percent_columns=["Gross Growth %", "Net Growth %", "Orders Growth %", "Dis % Change"])}
<div class="period-title">FTD vs LY</div>{html_table(day_level_ftd_ly, percent_columns=["Gross Growth %", "Net Growth %", "Orders Growth %", "Dis % Change"])}
<div class="period-title">MTD vs LM MTD</div>{html_table(day_level_mtd_lm, percent_columns=["Gross Growth %", "Net Growth %", "Orders Growth %", "Dis % Change"])}
<div class="period-title">MTD vs LY MTD</div>{html_table(day_level_mtd_ly, percent_columns=["Gross Growth %", "Net Growth %", "Orders Growth %", "Dis % Change"])}

<div class="footer">Generated automatically from Rista monthly CSV data.<br>FTD: {ftd_date.strftime("%d-%b-%Y")} | MTD: {current_month_start.strftime("%d-%b-%Y")} → {ftd_date.strftime("%d-%b-%Y")}</div>

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
