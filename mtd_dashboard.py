# =========================================================
# IMPORTS
# =========================================================

from datetime import datetime, timedelta
from pathlib import Path
import pandas as pd
import gspread
import json
import os
import re
from google.oauth2.service_account import Credentials

print("=" * 60)
print("🚀 MTD DASHBOARD STARTED")
print("=" * 60)


# ---------------- GOOGLE ---------------- #


creds = Credentials.from_service_account_info(
    json.loads(
        os.environ["GOOGLE_CREDENTIALS"]
    ),
    scopes=[
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive"
    ]
)

client = gspread.authorize(creds)

sheet_url = "https://docs.google.com/spreadsheets/d/1g4vuRZPy7qsUvDzF5yYM60VKWTL2r0VSDvtvNl06hiY/edit"

spreadsheet = client.open_by_url(sheet_url)

print("✅ Connected to Google Sheet")

# =========================================================
# PUSH DATAFRAME TO GOOGLE SHEET
# =========================================================

import time

def push(sheet_name, df):

    try:
        ws = spreadsheet.worksheet(sheet_name)

    except Exception:

        ws = spreadsheet.add_worksheet(
            title=sheet_name,
            rows="5000",
            cols="30"
        )

    # Prepare Data
    data = [
        df.columns.tolist()
    ] + (
        df.fillna("")
        .values
        .tolist()
    )

    # Clear existing data
    ws.batch_clear(["A:Z"])

    # Update from A1
    ws.update(
        "A1",
        data,
        value_input_option="USER_ENTERED"
    )

    # Prevent Google API quota error
    time.sleep(2)

    print(f"✅ Updated : {sheet_name}")

# =========================================================
# FIND LATEST MONTHLY CSV
# =========================================================

BASE_FOLDER = Path("monthly_data")

csv_files = list(
    BASE_FOLDER.rglob("*.csv")
)

print(f"📂 CSV Files Found : {len(csv_files)}")

if not csv_files:
    raise Exception("❌ No Monthly CSV Files Found")

# =========================================================
# PICK LATEST FILE BY FILE NAME
# =========================================================

month_map = {
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
    "Dec": 12,
}

def extract_month_year(path):

    m = re.search(r"MTD_(\w{3})_(\d{2})\.csv", path.name)

    if not m:
        return datetime(1900, 1, 1)

    month = month_map[m.group(1)]
    year = 2000 + int(m.group(2))

    return datetime(year, month, 1)


latest_csv = max(
    csv_files,
    key=extract_month_year
)

print(f"📄 Latest File : {latest_csv}")

# =========================================================
# READ CSV
# =========================================================

final_df = pd.read_csv(
    latest_csv,
    low_memory=False
)

# =========================================================
# CLEAN DATA
# =========================================================

final_df["Date"] = pd.to_datetime(final_df["Date"])

final_df["Store Type"] = (
    final_df["Store Type"]
    .fillna("")
    .astype(str)
    .str.strip()
    .str.upper()
)

print("=" * 60)
print("TOTAL ROWS :", len(final_df))
print("TOTAL COLUMNS :", len(final_df.columns))
print("=" * 60)

print(final_df.head())

print("=" * 60)
print("DATE RANGE CHECK")
print("=" * 60)

print("Min Date :", final_df["Date"].min())
print("Max Date :", final_df["Date"].max())

print()

print(final_df["Date"].value_counts().sort_index())

print("=" * 60)
print("STORE TYPE VALUES (FULL DATA)")
print("=" * 60)

print(final_df["Store Type"].value_counts(dropna=False))

# =========================================================
# BUSINESS DATE
# =========================================================

available_dates = sorted(
    final_df["Date"]
    .dropna()
    .unique()
)

# Latest available date in CSV (FTD)
ftd_date = available_dates[-1]

# Month Start for MTD
mtd_start = pd.Timestamp(
    year=ftd_date.year,
    month=ftd_date.month,
    day=1
)

# Helper function
def nearest_available(target_date):
    valid = [d for d in available_dates if d <= target_date]
    return valid[-1] if valid else None

last_week = nearest_available(ftd_date - timedelta(days=7))
last_2_week = nearest_available(ftd_date - timedelta(days=14))
last_month = nearest_available(ftd_date - pd.DateOffset(months=1))
last_year = nearest_available(ftd_date - pd.DateOffset(years=1))

print("=" * 60)
print("DATE CHECK")
print("=" * 60)

print("FTD Date   :", ftd_date.date())
print("MTD Start  :", mtd_start.date())
print("Last Week  :", last_week.date() if last_week is not None else "NA")
print("Last 2 Week:", last_2_week.date() if last_2_week is not None else "NA")
print("Last Month :", last_month.date() if last_month is not None else "NA")
print("Last Year  :", last_year.date() if last_year is not None else "NA")


# =========================================================
# FILTER DATA
# =========================================================

# ---------- FTD ----------

ftd_df = final_df.loc[
    final_df["Date"].eq(ftd_date)
].copy()

# ---------- MTD ----------

mtd_df = final_df.loc[
    (final_df["Date"] >= mtd_start) &
    (final_df["Date"] <= ftd_date)
].copy()

# ---------- Comparison ----------

lw_df = final_df.loc[
    final_df["Date"].eq(last_week)
].copy()

l2w_df = final_df.loc[
    final_df["Date"].eq(last_2_week)
].copy()

mom_df = final_df.loc[
    final_df["Date"].eq(last_month)
].copy()

ly_df = final_df.loc[
    final_df["Date"].eq(last_year)
].copy()

print("=" * 60)
print("FILTER CHECK")
print("=" * 60)

print("FTD Rows   :", len(ftd_df))
print("MTD Rows   :", len(mtd_df))
print("LW Rows    :", len(lw_df))
print("L2W Rows   :", len(l2w_df))
print("MoM Rows   :", len(mom_df))
print("LY Rows    :", len(ly_df))


# =========================================================
# STORE TYPE SPLIT
# =========================================================

# ---------- FTD ----------

ftd_pan_df = ftd_df.copy()

ftd_coco_df = ftd_df[
    ftd_df["Store Type"] == "COCO"
].copy()

ftd_fofo_df = ftd_df[
    ftd_df["Store Type"] == "FOFO"
].copy()


# ---------- MTD ----------

mtd_pan_df = mtd_df.copy()

mtd_coco_df = mtd_df[
    mtd_df["Store Type"] == "COCO"
].copy()

mtd_fofo_df = mtd_df[
    mtd_df["Store Type"] == "FOFO"
].copy()


print("=" * 60)
print("STORE TYPE SPLIT")
print("=" * 60)

print("FTD PAN  :", len(ftd_pan_df))
print("FTD COCO :", len(ftd_coco_df))
print("FTD FOFO :", len(ftd_fofo_df))

print("-" * 60)

print("MTD PAN  :", len(mtd_pan_df))
print("MTD COCO :", len(mtd_coco_df))
print("MTD FOFO :", len(mtd_fofo_df))

# =========================================================
# STORE TYPE SPLIT
# =========================================================

# ===========================
# FTD
# ===========================

ftd_pan_df = ftd_df.copy()

ftd_coco_df = ftd_df[
    ftd_df["Store Type"] == "COCO"
].copy()

ftd_fofo_df = ftd_df[
    ftd_df["Store Type"] == "FOFO"
].copy()

# ===========================
# MTD
# ===========================

mtd_pan_df = mtd_df.copy()

mtd_coco_df = mtd_df[
    mtd_df["Store Type"] == "COCO"
].copy()

mtd_fofo_df = mtd_df[
    mtd_df["Store Type"] == "FOFO"
].copy()


print("=" * 60)
print("STORE TYPE VALUES (FTD)")
print("=" * 60)

print(
    ftd_df["Store Type"].value_counts(dropna=False)
)

print()

print("=" * 60)
print("STORE TYPE VALUES (MTD)")
print("=" * 60)

print(
    mtd_df["Store Type"].value_counts(dropna=False)
)

print()

print("=" * 60)
print("STORE TYPE SPLIT")
print("=" * 60)

print("--------------- FTD ----------------")

print("PAN INDIA :", len(ftd_pan_df))
print("COCO      :", len(ftd_coco_df))
print("FOFO      :", len(ftd_fofo_df))

print()

print("--------------- MTD ----------------")

print("PAN INDIA :", len(mtd_pan_df))
print("COCO      :", len(mtd_coco_df))
print("FOFO      :", len(mtd_fofo_df))

# =========================================================
# UNIVERSAL SUMMARY BUILDER
# =========================================================

def build_summary(df, column):

    summary = (
        df.groupby(column)
        .agg(
            Gross=("Gross Sales", "sum"),
            Net=("Net Sales", "sum"),
            Discount=("Discount", "sum"),
            Orders=("Orders", "sum")
        )
        .reset_index()
    )

    summary["AOV"] = (
        summary["Net"]
        /
        summary["Orders"].replace(0, 1)
    )

    summary["Dis %"] = (
        summary["Discount"]
        /
        summary["Gross"].replace(0, 1)
    ) * 100

    summary["Contribution %"] = (
        summary["Net"]
        /
        summary["Net"].sum()
    ) * 100

    summary = summary.sort_values(
        "Net",
        ascending=False
    ).reset_index(drop=True)

    return summary

# =========================================================
# KPI SUMMARY
# =========================================================

def get_kpi(df):

    if df.empty:

        return {
            "Gross": 0,
            "Net": 0,
            "Discount": 0,
            "Orders": 0,
            "Qty": 0,
            "AOV": 0,
            "Dis %": 0
        }

    gross = df["Gross Sales"].sum()
    net = df["Net Sales"].sum()
    discount = df["Discount"].sum()
    orders = df["Orders"].sum()
    qty = df["Quantity"].sum()

    aov = net / orders if orders else 0
    dis_pct = (discount / gross * 100) if gross else 0

    return {
        "Gross": round(gross, 2),
        "Net": round(net, 2),
        "Discount": round(discount, 2),
        "Orders": int(orders),
        "Qty": round(qty, 2),
        "AOV": round(aov, 2),
        "Dis %": round(dis_pct, 2)
    }


# =========================================================
# KPI SUMMARY
# =========================================================

# ---------- FTD ----------

ftd_pan_kpi = get_kpi(ftd_pan_df)
ftd_coco_kpi = get_kpi(ftd_coco_df)
ftd_fofo_kpi = get_kpi(ftd_fofo_df)

# ---------- MTD ----------

mtd_pan_kpi = get_kpi(mtd_pan_df)
mtd_coco_kpi = get_kpi(mtd_coco_df)
mtd_fofo_kpi = get_kpi(mtd_fofo_df)

# ---------- Comparison ----------

lw_kpi = get_kpi(lw_df)
l2w_kpi = get_kpi(l2w_df)
mom_kpi = get_kpi(mom_df)
ly_kpi = get_kpi(ly_df)

print("=" * 100)
print("FTD KPI vs MTD KPI")
print("=" * 100)

# =========================================================
# EMAIL DATA
# =========================================================

compare_df = pd.DataFrame({

    "Metric":[
        "Gross",
        "Net",
        "Discount",
        "Orders",
        "Qty",
        "AOV",
        "Dis %"
    ],

    # ---------------- FTD ----------------

    "FTD PAN":[
        ftd_pan_kpi["Gross"],
        ftd_pan_kpi["Net"],
        ftd_pan_kpi["Discount"],
        ftd_pan_kpi["Orders"],
        ftd_pan_kpi["Qty"],
        ftd_pan_kpi["AOV"],
        ftd_pan_kpi["Dis %"]
    ],

    "FTD COCO":[
        ftd_coco_kpi["Gross"],
        ftd_coco_kpi["Net"],
        ftd_coco_kpi["Discount"],
        ftd_coco_kpi["Orders"],
        ftd_coco_kpi["Qty"],
        ftd_coco_kpi["AOV"],
        ftd_coco_kpi["Dis %"]
    ],

    "FTD FOFO":[
        ftd_fofo_kpi["Gross"],
        ftd_fofo_kpi["Net"],
        ftd_fofo_kpi["Discount"],
        ftd_fofo_kpi["Orders"],
        ftd_fofo_kpi["Qty"],
        ftd_fofo_kpi["AOV"],
        ftd_fofo_kpi["Dis %"]
    ],

    # ---------------- MTD ----------------

    "MTD PAN":[
        mtd_pan_kpi["Gross"],
        mtd_pan_kpi["Net"],
        mtd_pan_kpi["Discount"],
        mtd_pan_kpi["Orders"],
        mtd_pan_kpi["Qty"],
        mtd_pan_kpi["AOV"],
        mtd_pan_kpi["Dis %"]
    ],

    "MTD COCO":[
        mtd_coco_kpi["Gross"],
        mtd_coco_kpi["Net"],
        mtd_coco_kpi["Discount"],
        mtd_coco_kpi["Orders"],
        mtd_coco_kpi["Qty"],
        mtd_coco_kpi["AOV"],
        mtd_coco_kpi["Dis %"]
    ],

    "MTD FOFO":[
        mtd_fofo_kpi["Gross"],
        mtd_fofo_kpi["Net"],
        mtd_fofo_kpi["Discount"],
        mtd_fofo_kpi["Orders"],
        mtd_fofo_kpi["Qty"],
        mtd_fofo_kpi["AOV"],
        mtd_fofo_kpi["Dis %"]
    ]

}).round(2)

print("=" * 100)
print("FTD vs MTD KPI")
print("=" * 100)

print(compare_df)

# =========================================================
# GENERIC GROWTH SUMMARY
# =========================================================

def growth_summary(current_df, previous_df, group_col, period_name):

    curr = (
        current_df
        .groupby(group_col)
        .agg(
            Gross=("Gross Sales","sum"),
            Net=("Net Sales","sum"),
            Orders=("Orders","sum")
        )
        .reset_index()
    )

    prev = (
        previous_df
        .groupby(group_col)
        .agg(
            Gross_Previous=("Gross Sales","sum"),
            Net_Previous=("Net Sales","sum"),
            Orders_Previous=("Orders","sum")
        )
        .reset_index()
    )

    df = curr.merge(
        prev,
        on=group_col,
        how="left"
    ).fillna(0)

    df[f"{period_name} Gross %"] = (
        (df["Gross"] - df["Gross_Previous"])
        /
        df["Gross_Previous"].replace(0,1)
    ) * 100

    df[f"{period_name} Net %"] = (
        (df["Net"] - df["Net_Previous"])
        /
        df["Net_Previous"].replace(0,1)
    ) * 100

    df[f"{period_name} Orders %"] = (
        (df["Orders"] - df["Orders_Previous"])
        /
        df["Orders_Previous"].replace(0,1)
    ) * 100

    return df.round(2)

# =========================================================
# UNIVERSAL GROWTH CALCULATOR
# =========================================================

def add_growth(current_df, compare_df, column, suffix):

    compare = (
        compare_df.groupby(column)
        .agg(
            Compare_Net=("Net Sales", "sum")
        )
        .reset_index()
    )

    current_df = current_df.merge(
        compare,
        on=column,
        how="left"
    )

    current_df["Compare_Net"] = (
        current_df["Compare_Net"]
        .fillna(0)
    )

    current_df[f"{suffix} Growth %"] = (
        (
            current_df["Net"]
            - current_df["Compare_Net"]
        )
        /
        current_df["Compare_Net"].replace(0, pd.NA)
    ) * 100

    current_df[f"{suffix} Growth %"] = (
        current_df[f"{suffix} Growth %"]
        .fillna(0)
        .round(1)
    )

    current_df.drop(
        columns="Compare_Net",
        inplace=True
    )

    return current_df

print("="*60)
print("LW Dates")
print("="*60)
print(lw_df["Date"].value_counts())

print("="*60)
print("L2W Dates")
print("="*60)
print(l2w_df["Date"].value_counts())

print("LW Net :", lw_df["Net Sales"].sum())
print("L2W Net:", l2w_df["Net Sales"].sum())

# =========================================================
# FTD BRAND SUMMARY
# =========================================================

ftd_brand_summary = build_summary(
    ftd_coco_df,
    "Brand Name"
)

print("=" * 60)
print("FTD BRAND SUMMARY")
print("=" * 60)

print(ftd_brand_summary.round(2))

push(
    "Dashboard_Brand_FTD",
    ftd_brand_summary.round(2)
)

# =========================================================
# MTD BRAND SUMMARY
# =========================================================

mtd_brand_summary = build_summary(
    mtd_coco_df,
    "Brand Name"
)

print("=" * 60)
print("MTD BRAND SUMMARY")
print("=" * 60)

print(mtd_brand_summary.round(2))

push(
    "Dashboard_Brand_MTD",
    mtd_brand_summary.round(2)

)

# =========================================================
# FTD BRAND LW
# =========================================================

ftd_brand_lw = add_growth(
    ftd_brand_summary.copy(),
    lw_df,
    "Brand Name",
    "LW"
)

print("=" * 60)
print("FTD BRAND LW")
print("=" * 60)

print(ftd_brand_lw.round(2))

push(
    "Dashboard_Brand_FTD_LW",
    ftd_brand_lw.round(2)
)

# =========================================================
# MTD BRAND LW
# =========================================================

mtd_brand_lw = add_growth(
    mtd_brand_summary.copy(),
    lw_df,
    "Brand Name",
    "LW"
)

push(
    "Dashboard_Brand_MTD_LW",
    mtd_brand_lw.round(2)
)

# =========================================================
# FTD BRAND L2W
# =========================================================

ftd_brand_l2w = add_growth(
    ftd_brand_summary.copy(),
    l2w_df,
    "Brand Name",
    "L2W"
)

push(
    "Dashboard_Brand_FTD_L2W",
    ftd_brand_l2w.round(2)
)

# =========================================================
# MTD BRAND L2W
# =========================================================

mtd_brand_l2w = add_growth(
    mtd_brand_summary.copy(),
    l2w_df,
    "Brand Name",
    "L2W"
)

push(
    "Dashboard_Brand_MTD_L2W",
    mtd_brand_l2w.round(2)
)

# =========================================================
# FTD SOURCE SUMMARY
# =========================================================

ftd_source_summary = build_summary(
    ftd_coco_df,
    "Source"
)

push(
    "Dashboard_Source_FTD",
    ftd_source_summary.round(2)
)

# =========================================================
# MTD SOURCE SUMMARY
# =========================================================

mtd_source_summary = build_summary(
    mtd_coco_df,
    "Source"
)

push(
    "Dashboard_Source_MTD",
    mtd_source_summary.round(2)
)

# =========================================================
# FTD SOURCE LW
# =========================================================

ftd_source_lw = add_growth(
    ftd_source_summary.copy(),
    lw_df,
    "Source",
    "LW"
)

push(
    "Dashboard_Source_FTD_LW",
    ftd_source_lw.round(2)
)

# =========================================================
# MTD SOURCE LW
# =========================================================

mtd_source_lw = add_growth(
    mtd_source_summary.copy(),
    lw_df,
    "Source",
    "LW"
)

push(
    "Dashboard_Source_MTD_LW",
    mtd_source_lw.round(2)
)

# =========================================================
# FTD SOURCE L2W
# =========================================================

ftd_source_l2w = add_growth(
    ftd_source_summary.copy(),
    l2w_df,
    "Source",
    "L2W"
)

push(
    "Dashboard_Source_FTD_L2W",
    ftd_source_l2w.round(2)
)

# =========================================================
# MTD SOURCE L2W
# =========================================================

mtd_source_l2w = add_growth(
    mtd_source_summary.copy(),
    l2w_df,
    "Source",
    "L2W"
)

push(
    "Dashboard_Source_MTD_L2W",
    mtd_source_l2w.round(2)
)

# =========================================================
# FTD BRANCH SUMMARY
# =========================================================

ftd_branch_summary = build_summary(
    ftd_coco_df,
    "Branch"
)

push(
    "Dashboard_Branch_FTD",
    ftd_branch_summary.round(2)
)

# =========================================================
# MTD BRANCH SUMMARY
# =========================================================

mtd_branch_summary = build_summary(
    mtd_coco_df,
    "Branch"
)

push(
    "Dashboard_Branch_MTD",
    mtd_branch_summary.round(2)
)

# =========================================================
# TOP 10 BRANCHES (EMAIL)
# =========================================================

top_branch_df = (
    ftd_branch_summary
    .sort_values("Net", ascending=False)
    .head(10)
)

# =========================================================
# FTD BRANCH LW
# =========================================================

ftd_branch_lw = add_growth(
    ftd_branch_summary.copy(),
    lw_df,
    "Branch",
    "LW"
)

push(
    "Dashboard_Branch_FTD_LW",
    ftd_branch_lw.round(2)
)

# =========================================================
# MTD BRANCH LW
# =========================================================

mtd_branch_lw = add_growth(
    mtd_branch_summary.copy(),
    lw_df,
    "Branch",
    "LW"
)

push(
    "Dashboard_Branch_MTD_LW",
    mtd_branch_lw.round(2)
)

# =========================================================
# FTD BRANCH L2W
# =========================================================

ftd_branch_l2w = add_growth(
    ftd_branch_summary.copy(),
    l2w_df,
    "Branch",
    "L2W"
)

push(
    "Dashboard_Branch_FTD_L2W",
    ftd_branch_l2w.round(2)
)

# =========================================================
# MTD BRANCH L2W
# =========================================================

mtd_branch_l2w = add_growth(
    mtd_branch_summary.copy(),
    l2w_df,
    "Branch",
    "L2W"
)

push(
    "Dashboard_Branch_MTD_L2W",
    mtd_branch_l2w.round(2)
)

# =========================================================
# FTD SESSION SUMMARY
# =========================================================

ftd_session_summary = build_summary(
    ftd_coco_df,
    "Session"
)

ftd_session_summary["Session"] = (
    ftd_session_summary["Session"]
    .fillna("Others")
    .replace("", "Others")
)

session_order = {
    "Breakfast": 1,
    "Lunch": 2,
    "Snacks": 3,
    "Dinner": 4,
    "Late Night": 5,
    "Closing": 6,
    "Others": 99
}

ftd_session_summary["Sort"] = (
    ftd_session_summary["Session"]
    .map(session_order)
    .fillna(99)
)

ftd_session_summary = (
    ftd_session_summary
    .sort_values("Sort")
    .drop(columns="Sort")
    .reset_index(drop=True)
)

push(
    "Dashboard_Session_FTD",
    ftd_session_summary.round(2)
)

# =========================================================
# MTD SESSION SUMMARY
# =========================================================

mtd_session_summary = build_summary(
    mtd_coco_df,
    "Session"
)

mtd_session_summary["Session"] = (
    mtd_session_summary["Session"]
    .fillna("Others")
    .replace("", "Others")
)

mtd_session_summary["Sort"] = (
    mtd_session_summary["Session"]
    .map(session_order)
    .fillna(99)
)

mtd_session_summary = (
    mtd_session_summary
    .sort_values("Sort")
    .drop(columns="Sort")
    .reset_index(drop=True)
)

push(
    "Dashboard_Session_MTD",
    mtd_session_summary.round(2)
)

# =========================================================
# FTD SESSION LW
# =========================================================

ftd_session_lw = add_growth(
    ftd_session_summary.copy(),
    lw_df,
    "Session",
    "LW"
)

push(
    "Dashboard_Session_FTD_LW",
    ftd_session_lw.round(2)
)

# =========================================================
# MTD SESSION LW
# =========================================================

mtd_session_lw = add_growth(
    mtd_session_summary.copy(),
    lw_df,
    "Session",
    "LW"
)

push(
    "Dashboard_Session_MTD_LW",
    mtd_session_lw.round(2)
)

# =========================================================
# FTD SESSION L2W
# =========================================================

ftd_session_l2w = add_growth(
    ftd_session_summary.copy(),
    l2w_df,
    "Session",
    "L2W"
)

push(
    "Dashboard_Session_FTD_L2W",
    ftd_session_l2w.round(2)
)

# =========================================================
# MTD SESSION L2W
# =========================================================

mtd_session_l2w = add_growth(
    mtd_session_summary.copy(),
    l2w_df,
    "Session",
    "L2W"
)

push(
    "Dashboard_Session_MTD_L2W",
    mtd_session_l2w.round(2)
)

# =========================================================
# FTD REGION SUMMARY
# =========================================================

ftd_region_summary = build_summary(
    ftd_coco_df,
    "Region"
)

push(
    "Dashboard_Region_FTD",
    ftd_region_summary.round(2)
)

# =========================================================
# MTD REGION SUMMARY
# =========================================================

mtd_region_summary = build_summary(
    mtd_coco_df,
    "Region"
)

push(
    "Dashboard_Region_MTD",
    mtd_region_summary.round(2)
)

# =========================================================
# FTD REGION LW
# =========================================================

ftd_region_lw = add_growth(
    ftd_region_summary.copy(),
    lw_df,
    "Region",
    "LW"
)

push(
    "Dashboard_Region_FTD_LW",
    ftd_region_lw.round(2)
)

# =========================================================
# MTD REGION LW
# =========================================================

mtd_region_lw = add_growth(
    mtd_region_summary.copy(),
    lw_df,
    "Region",
    "LW"
)

push(
    "Dashboard_Region_MTD_LW",
    mtd_region_lw.round(2)
)

# =========================================================
# FTD REGION L2W
# =========================================================

ftd_region_l2w = add_growth(
    ftd_region_summary.copy(),
    l2w_df,
    "Region",
    "L2W"
)

push(
    "Dashboard_Region_FTD_L2W",
    ftd_region_l2w.round(2)
)

# =========================================================
# MTD REGION L2W
# =========================================================

mtd_region_l2w = add_growth(
    mtd_region_summary.copy(),
    l2w_df,
    "Region",
    "L2W"
)

push(
    "Dashboard_Region_MTD_L2W",
    mtd_region_l2w.round(2)
)


# =========================================================
# EMAIL KPI (FTD vs MTD)
# =========================================================

# ensure a 'today' variable for email headers (use latest available date)
today = ftd_date if 'ftd_date' in globals() else datetime.now()

# EMAIL KPI (FTD vs MTD) — fixed to use mtd_pan_kpi
kpi_df = pd.DataFrame({

    "KPI":[
        "Gross Revenue",
        "Net Revenue",
        "Discount",
        "Orders",
        "Qty",
        "AOV",
        "Discount %"
    ],

    "FTD":[
        ftd_pan_kpi["Gross"],
        ftd_pan_kpi["Net"],
        ftd_pan_kpi["Discount"],
        ftd_pan_kpi["Orders"],
        ftd_pan_kpi["Qty"],
        ftd_pan_kpi["AOV"],
        f'{ftd_pan_kpi["Dis %"]:.2f}%'
    ],

    "MTD":[
        mtd_pan_kpi["Gross"],
        mtd_pan_kpi["Net"],
        mtd_pan_kpi["Discount"],
        mtd_pan_kpi["Orders"],
        mtd_pan_kpi["Qty"],
        mtd_pan_kpi["AOV"],
        f'{mtd_pan_kpi["Dis %"]:.2f}%'
    ]

}).round(2)

# Create combined summaries used by the email body (simple FTD vs MTD concat)
# This prevents NameError for brand_summary, source_summary, region_summary, session_summary
brand_summary = pd.concat([
    ftd_brand_summary.assign(Period="FTD"),
    mtd_brand_summary.assign(Period="MTD")
], ignore_index=True)

source_summary = pd.concat([
    ftd_source_summary.assign(Period="FTD"),
    mtd_source_summary.assign(Period="MTD")
], ignore_index=True)

region_summary = pd.concat([
    ftd_region_summary.assign(Period="FTD"),
    mtd_region_summary.assign(Period="MTD")
], ignore_index=True)

session_summary = pd.concat([
    ftd_session_summary.assign(Period="FTD"),
    mtd_session_summary.assign(Period="MTD")
], ignore_index=True)

# Send Mail
import smtplib

from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

# =========================================================
# EMAIL CONFIGURATION
# =========================================================

SMTP_SERVER = "smtp.gmail.com"
SMTP_PORT = 587

import os
import smtplib

SMTP_SERVER = "smtp.gmail.com"
SMTP_PORT = 587

EMAIL = os.environ.get("EMAIL_USER")
PASSWORD = os.environ.get("EMAIL_PASS")

# Fixed recipient for testing
TO = ["mis2@frozenbottle.in"]

# No CC
CC = []

TO = [x.strip() for x in TO if x.strip()]
CC = [x.strip() for x in CC if x.strip()]

def html_table(df):

    return (
        df.round(2)
        .to_html(
            index=False,
            classes="table",
            border=0
        )
    )

body = f"""

<html>

<head>

<style>

body{{
font-family:Calibri;
font-size:14px;
background:#F5F7FA;
}}

h2{{
background:#243447;
color:white;
padding:10px;
}}

h3{{
background:#2E8B57;
color:white;
padding:6px;
}}

.table{{
border-collapse:collapse;
width:80%;
margin-bottom:25px;
}}

.table th{{
background:#243447;
color:white;
padding:8px;
text-align:center;
}}

.table td{{
padding:6px;
text-align:center;
border:1px solid #dddddd;
}}

.table tr:nth-child(even){{
background:#f5f5f5;
}}

.kpi{{
display:flex;
gap:20px;
margin-bottom:20px;
}}

.card{{
background:white;
padding:12px;
border-radius:8px;
box-shadow:0 2px 5px #ccc;
width:180px;
text-align:center;
}}

.value{{
font-size:22px;
font-weight:bold;
color:#0A7D32;
}}

</style>

</head>

<body>

<h2>
📊 MTD Dashboard
<br>
{today.strftime("%d-%b-%Y")}
</h2>

<div class="kpi">

    <!-- Gross -->
    <div class="card">
        <b>Gross Revenue</b><br><br>

        <span style="color:#777;">FTD</span>
        <div class="value">
            ₹{ftd_pan_kpi["Gross"]:,.0f}
        </div>

        <hr>

        <span style="color:#777;">MTD</span>
        <div class="value">
            ₹{mtd_pan_kpi["Gross"]:,.0f}
        </div>
    </div>

    <!-- Net -->
    <div class="card">
        <b>Net Revenue</b><br><br>

        <span style="color:#777;">FTD</span>
        <div class="value">
            ₹{ftd_pan_kpi["Net"]:,.0f}
        </div>

        <hr>

        <span style="color:#777;">MTD</span>
        <div class="value">
            ₹{mtd_pan_kpi["Net"]:,.0f}
        </div>
    </div>

    <!-- Orders -->
    <div class="card">
        <b>Orders</b><br><br>

        <span style="color:#777;">FTD</span>
        <div class="value">
            {int(ftd_pan_kpi["Orders"]):,}
        </div>

        <hr>

        <span style="color:#777;">MTD</span>
        <div class="value">
            {int(mtd_pan_kpi["Orders"]):,}
        </div>
    </div>

    <!-- Qty -->
    <div class="card">
        <b>Qty</b><br><br>

        <span style="color:#777;">FTD</span>
        <div class="value">
            {int(ftd_pan_kpi["Qty"]):,}
        </div>

        <hr>

        <span style="color:#777;">MTD</span>
        <div class="value">
            {int(mtd_pan_kpi["Qty"]):,}
        </div>
    </div>

    <!-- AOV -->
    <div class="card">
        <b>AOV</b><br><br>

        <span style="color:#777;">FTD</span>
        <div class="value">
            ₹{ftd_pan_kpi["AOV"]:,.0f}
        </div>

        <hr>

        <span style="color:#777;">MTD</span>
        <div class="value">
            ₹{mtd_pan_kpi["AOV"]:,.0f}
        </div>
    </div>

    <!-- Discount % -->
    <div class="card">
        <b>Discount %</b><br><br>

        <span style="color:#777;">FTD</span>
        <div class="value">
            {ftd_pan_kpi["Dis %"]:.1f}%
        </div>

        <hr>

        <span style="color:#777;">MTD</span>
        <div class="value">
            {mtd_pan_kpi["Dis %"]:.1f}%
        </div>
    </div>

</div>

<h3>📊 FTD vs MTD KPI Summary</h3>

{html_table(kpi_df)}

<br>

<h3>🏢 COCO vs FOFO (FTD | MTD)</h3>

{html_table(compare_df)}

<br>

<h3>🏷 Brand Performance (FTD | MTD)</h3>

{html_table(brand_summary)}

<br>

<h3>🛒 Source Performance (FTD | MTD)</h3>

{html_table(source_summary)}

<br>

<h3>🌎 Region Performance (FTD | MTD)</h3>

{html_table(region_summary)}

<br>

<h3>🕒 Session Performance (FTD | MTD)</h3>

{html_table(session_summary)}

<br>

<h3>🏪 Top 10 Branches (FTD | MTD)</h3>

{html_table(top_branch_df)}

</body>

</html>
"""
def send_mail(subject, body):

    msg = MIMEMultipart()

    msg["From"] = EMAIL
    msg["To"] = ",".join(TO)
    msg["Subject"] = subject

    msg.attach(
        MIMEText(
            body,
            "html"
        )
    )

    server = smtplib.SMTP(
        SMTP_SERVER,
        SMTP_PORT
    )

    server.starttls()

    server.login(
        EMAIL,
        PASSWORD
    )

    server.sendmail(
        EMAIL,
        TO,
        msg.as_string()
    )

    server.quit()

    print("✅ Dashboard Mail Sent")

send_mail(

    subject=f"MTD Dashboard | {today.strftime('%d-%b-%Y')}",

    body=body

)


