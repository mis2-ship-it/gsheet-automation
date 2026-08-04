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
    qty = df.get("Quantity", pd.Series([0]*len(df))).sum() if "Quantity" in df.columns else 0

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
# KPI SUMMARY COMPUTATION
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
# EMAIL DATA (FTD vs MTD KPI + compare table)
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
# GENERIC GROWTH & AUX FUNCTIONS (kept as before)
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
print(lw_df["Date"].value_counts() if not lw_df.empty else "LW empty")

print("="*60)
print("L2W Dates")
print("="*60)
print(l2w_df["Date"].value_counts() if not l2w_df.empty else "L2W empty")

print("LW Net :", lw_df["Net Sales"].sum() if "Net Sales" in lw_df.columns else 0)
print("L2W Net:", l2w_df["Net Sales"].sum() if "Net Sales" in l2w_df.columns else 0)

# =========================================================
# BUILD & PUSH SUMMARY SHEETS (FTD / MTD)
# =========================================================

# FTD summaries (already computed earlier and pushed individually)
ftd_brand_summary = build_summary(ftd_coco_df, "Brand Name")
push("Dashboard_Brand_FTD", ftd_brand_summary.round(2))

mtd_brand_summary = build_summary(mtd_coco_df, "Brand Name")
push("Dashboard_Brand_MTD", mtd_brand_summary.round(2))

ftd_source_summary = build_summary(ftd_coco_df, "Source")
push("Dashboard_Source_FTD", ftd_source_summary.round(2))

mtd_source_summary = build_summary(mtd_coco_df, "Source")
push("Dashboard_Source_MTD", mtd_source_summary.round(2))

ftd_region_summary = build_summary(ftd_coco_df, "Region")
push("Dashboard_Region_FTD", ftd_region_summary.round(2))

mtd_region_summary = build_summary(mtd_coco_df, "Region")
push("Dashboard_Region_MTD", mtd_region_summary.round(2))

ftd_branch_summary = build_summary(ftd_coco_df, "Branch")
push("Dashboard_Branch_FTD", ftd_branch_summary.round(2))

mtd_branch_summary = build_summary(mtd_coco_df, "Branch")
push("Dashboard_Branch_MTD", mtd_branch_summary.round(2))

# Session summaries
ftd_session_summary = build_summary(ftd_coco_df, "Session")
ftd_session_summary["Session"] = ftd_session_summary["Session"].fillna("Others").replace("", "Others")
session_order = {"Breakfast":1,"Lunch":2,"Snacks":3,"Dinner":4,"Late Night":5,"Closing":6,"Others":99}
ftd_session_summary["Sort"] = ftd_session_summary["Session"].map(session_order).fillna(99)
ftd_session_summary = ftd_session_summary.sort_values("Sort").drop(columns="Sort").reset_index(drop=True)
push("Dashboard_Session_FTD", ftd_session_summary.round(2))

mtd_session_summary = build_summary(mtd_coco_df, "Session")
mtd_session_summary["Session"] = mtd_session_summary["Session"].fillna("Others").replace("", "Others")
mtd_session_summary["Sort"] = mtd_session_summary["Session"].map(session_order).fillna(99)
mtd_session_summary = mtd_session_summary.sort_values("Sort").drop(columns="Sort").reset_index(drop=True)
push("Dashboard_Session_MTD", mtd_session_summary.round(2))

# Top branches (FTD)
top_branch_df = ftd_branch_summary.sort_values("Net", ascending=False).head(10)
push("Dashboard_TopBranches_FTD", top_branch_df.round(2))

# =========================================================
# EMAIL PREPARATION — KPI + Combined summaries + Day-level pivots
# =========================================================

# ensure a 'today' variable for email headers (use latest available date)
today = ftd_date if 'ftd_date' in globals() else datetime.now()

# Build combined summaries with Period column and push split sheets
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

def move_period_first(df):
    if "Period" in df.columns:
        cols = ["Period"] + [c for c in df.columns if c != "Period"]
        return df[cols]
    return df

def push_split_by_period(combined_df, sheet_base):
    if "Period" not in combined_df.columns:
        push(sheet_base, combined_df.round(2))
        return None, None
    ftd = combined_df[combined_df["Period"] == "FTD"].copy().drop(columns=["Period"])
    mtd = combined_df[combined_df["Period"] == "MTD"].copy().drop(columns=["Period"])
    push(f"{sheet_base}_FTD", ftd.round(2))
    push(f"{sheet_base}_MTD", mtd.round(2))
    return ftd, mtd

brand_summary = move_period_first(brand_summary)
source_summary = move_period_first(source_summary)
region_summary = move_period_first(region_summary)
session_summary = move_period_first(session_summary)

push_split_by_period(brand_summary, "Dashboard_Brand")
push_split_by_period(source_summary, "Dashboard_Source")
push_split_by_period(region_summary, "Dashboard_Region")
push_split_by_period(session_summary, "Dashboard_Session")

# =========================================================
# DAY-LEVEL PIVOTS (Date columns, parameters rows)
# =========================================================

date_index = pd.date_range(start=mtd_start, end=ftd_date, freq="D")
date_index = pd.to_datetime(date_index).normalize()

def pivot_days_from_group(df, group_col=None, value_col="Net Sales"):
    work = df.copy()
    if group_col:
        # produce rows per group, columns per date
        daily = work.groupby([pd.Grouper(key="Date"), group_col])[value_col].sum().unstack(fill_value=0)
        out = daily.T.reindex(columns=date_index, fill_value=0)
        out.columns = [d.strftime("%Y-%m-%d") for d in date_index]
    else:
        daily = work.groupby(pd.Grouper(key="Date"))[value_col].sum().reindex(date_index, fill_value=0)
        out = pd.DataFrame([daily.values], index=["Overall Net Sales"], columns=[d.strftime("%Y-%m-%d") for d in date_index])
    return out

# Overall, COCO, FOFO
overall_day = pivot_days_from_group(mtd_df, group_col=None, value_col="Net Sales")
coco_day = pivot_days_from_group(mtd_df[mtd_df["Store Type"] == "COCO"], group_col=None, value_col="Net Sales")
fofo_day = pivot_days_from_group(mtd_df[mtd_df["Store Type"] == "FOFO"], group_col=None, value_col="Net Sales")
day_level_main = pd.concat([overall_day, coco_day, fofo_day])
day_level_main.index = ["Overall Net Sales", "COCO Net Sales", "FOFO Net Sales"]
push("Dashboard_DayLevel_Main", day_level_main.round(2))

# COCO brands pivot for requested brands
brands_required = ["Frozen Bottle", "Madno", "Boba Bar", "Lubov"]
brand_pivot = pivot_days_from_group(mtd_coco_df, group_col="Brand Name", value_col="Net Sales")
# ensure we have the brand_pivot index and columns
brand_rows = []
for b in brands_required:
    if b in brand_pivot.index:
        brand_rows.append(brand_pivot.loc[b])
    else:
        brand_rows.append(pd.Series(0, index=brand_pivot.columns))
brand_by_date = pd.DataFrame(brand_rows, index=brands_required)
push("Dashboard_COCO_Brands_Day", brand_by_date.round(2))

# COCO regions
def normalize_region(x):
    if pd.isna(x): return "Others"
    s = str(x).strip().upper()
    if s.startswith("KA"): return "KA"
    if s.startswith("MH"): return "MH"
    if s in ("TN", "TAMIL NADU"): return "TN"
    if s.startswith("KER"): return "KL"
    if s == "KERALA": return "KL"
    return s

mtd_coco_df = mtd_coco_df.copy()
mtd_coco_df["RegionNorm"] = mtd_coco_df["Region"].apply(normalize_region)
regions_required = ["KA", "MH", "TN", "KL"]
region_pivot = pivot_days_from_group(mtd_coco_df, group_col="RegionNorm", value_col="Net Sales")
region_rows = []
for r in regions_required:
    if r in region_pivot.index:
        region_rows.append(region_pivot.loc[r])
    else:
        region_rows.append(pd.Series(0, index=region_pivot.columns))
region_by_date = pd.DataFrame(region_rows, index=regions_required)
push("Dashboard_COCO_Regions_Day", region_by_date.round(2))

# COCO sources
def normalize_source(x):
    if pd.isna(x): return "Others"
    s = str(x).strip().lower()
    if "swiggy" in s: return "Swiggy"
    if "zomato" in s: return "Zomato"
    if "own" in s or "ownly" in s: return "Ownly"
    if "store" in s or "in store" in s or "instore" in s: return "In Store"
    return "Others"

mtd_coco_df["SourceNorm"] = mtd_coco_df["Source"].apply(normalize_source)
sources_required = ["In Store", "Swiggy", "Zomato", "Ownly", "Others"]
source_pivot = pivot_days_from_group(mtd_coco_df, group_col="SourceNorm", value_col="Net Sales")
source_rows = []
for s in sources_required:
    if s in source_pivot.index:
        source_rows.append(source_pivot.loc[s])
    else:
        source_rows.append(pd.Series(0, index=source_pivot.columns))
source_by_date = pd.DataFrame(source_rows, index=sources_required)
push("Dashboard_COCO_Sources_Day", source_by_date.round(2))

# =========================================================
# AOV & Discount Buckets per day
# =========================================================

invoice_col = None
for c in ("invoiceNumber", "invoice", "Order", "orderNumber"):
    if c in mtd_df.columns:
        invoice_col = c
        break

def build_buckets_for_metric(df, metric_col, bucket_edges, bucket_labels, index_name):
    work = df.copy()
    if invoice_col:
        inv = work.groupby([invoice_col, "Date"]).agg(
            Net=("Net Sales", "sum"),
            Orders=("Orders", "sum"),
            Discount=("Discount", "sum"),
            Gross=("Gross Sales","sum")
        ).reset_index()
        inv["AOV"] = inv["Net"] / inv["Orders"].replace(0,1)
        inv["DiscPct"] = inv["Discount"] / inv["Gross"].replace(0,1) * 100
        if metric_col == "AOV":
            inv["bucket"] = pd.cut(inv["AOV"], bins=bucket_edges, labels=bucket_labels, include_lowest=True)
        else:
            inv["bucket"] = pd.cut(inv["DiscPct"], bins=bucket_edges, labels=bucket_labels, include_lowest=True)
        daily = inv.groupby([pd.Grouper(key="Date"), "bucket"])["Net"].sum().unstack(fill_value=0)
    else:
        work["AOV_est"] = work["Net Sales"] / work["Orders"].replace(0,1)
        work["DiscPct_est"] = work["Discount"] / work["Gross Sales"].replace(0,1) * 100
        col = "AOV_est" if metric_col == "AOV" else "DiscPct_est"
        work["bucket"] = pd.cut(work[col], bins=bucket_edges, labels=bucket_labels, include_lowest=True)
        daily = work.groupby([pd.Grouper(key="Date"), "bucket"])["Net Sales"].sum().unstack(fill_value=0)
    # ensure columns order
    if hasattr(daily, "columns"):
        daily = daily.reindex(columns=bucket_labels, fill_value=0)
    daily = daily.reindex(date_index, fill_value=0).T
    daily.index.name = index_name
    daily.columns = [d.strftime("%Y-%m-%d") for d in date_index]
    return daily

aov_edges = [0,100,200,300,400,500,600,700,800,900,1000,float("inf")]
aov_labels = ["0-100","100-200","200-300","300-400","400-500","500-600","600-700","700-800","800-900","900-1000","1000+"]
aov_by_bucket = build_buckets_for_metric(mtd_df, "AOV", aov_edges, aov_labels, "AOV Bucket")
push("Dashboard_AOV_Buckets_Day", aov_by_bucket.round(2))

disc_edges = [0,5,10,20,100,float("inf")]
disc_labels = ["0-5%","5-10%","10-20%","20-100%","100%+"]
disc_by_bucket = build_buckets_for_metric(mtd_df, "DiscountPct", disc_edges, disc_labels, "Discount Bucket")
push("Dashboard_Discount_Buckets_Day", disc_by_bucket.round(2))

# =========================================================
# Prepare HTML email and insert day-level tables
# =========================================================

# Email KPI table
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

# email config
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

SMTP_SERVER = "smtp.gmail.com"
SMTP_PORT = 587

EMAIL = os.environ.get("EMAIL_USER")
PASSWORD = os.environ.get("EMAIL_PASS")

TO = ["mis2@frozenbottle.in"]
CC = []

TO = [x.strip() for x in TO if x.strip()]
CC = [x.strip() for x in CC if x.strip()]

def html_table(df):
    return (
        df.round(2)
        .to_html(
            index=True if df.index.name else False,
            classes="table",
            border=0
        )
    )

# Build body with day-level inserts (keep concise: include main day-level + links to sheets)
body = f"""
<html>
<head>
<style>
body{{font-family:Calibri;font-size:14px;background:#F5F7FA;}}
h2{{background:#243447;color:white;padding:10px;}}
h3{{background:#2E8B57;color:white;padding:6px;}}
.table{{border-collapse:collapse;width:90%;margin-bottom:25px;}}
.table th{{background:#243447;color:white;padding:8px;text-align:center;}}
.table td{{padding:6px;text-align:center;border:1px solid #dddddd;}}
.table tr:nth-child(even){{background:#f5f5f5;}}
.kpi{{display:flex;gap:20px;margin-bottom:20px;flex-wrap:wrap;}}
.card{{background:white;padding:12px;border-radius:8px;box-shadow:0 2px 5px #ccc;width:180px;text-align:center;}}
.value{{font-size:22px;font-weight:bold;color:#0A7D32;}}
</style>
</head>
<body>
<h2>📊 MTD Dashboard<br>{today.strftime("%d-%b-%Y")}</h2>

<div class="kpi">
    <div class="card"><b>Gross Revenue</b><br><br><span style="color:#777;">FTD</span>
        <div class="value">₹{ftd_pan_kpi["Gross"]:,.0f}</div><hr><span style="color:#777;">MTD</span>
        <div class="value">₹{mtd_pan_kpi["Gross"]:,.0f}</div></div>

    <div class="card"><b>Net Revenue</b><br><br><span style="color:#777;">FTD</span>
        <div class="value">₹{ftd_pan_kpi["Net"]:,.0f}</div><hr><span style="color:#777;">MTD</span>
        <div class="value">₹{mtd_pan_kpi["Net"]:,.0f}</div></div>

    <div class="card"><b>Orders</b><br><br><span style="color:#777;">FTD</span>
        <div class="value">{int(ftd_pan_kpi["Orders"]):,}</div><hr><span style="color:#777;">MTD</span>
        <div class="value">{int(mtd_pan_kpi["Orders"]):,}</div></div>

    <div class="card"><b>Qty</b><br><br><span style="color:#777;">FTD</span>
        <div class="value">{int(ftd_pan_kpi["Qty"]):,}</div><hr><span style="color:#777;">MTD</span>
        <div class="value">{int(mtd_pan_kpi["Qty"]):,}</div></div>

    <div class="card"><b>AOV</b><br><br><span style="color:#777;">FTD</span>
        <div class="value">₹{ftd_pan_kpi["AOV"]:,.0f}</div><hr><span style="color:#777;">MTD</span>
        <div class="value">₹{mtd_pan_kpi["AOV"]:,.0f}</div></div>

    <div class="card"><b>Discount %</b><br><br><span style="color:#777;">FTD</span>
        <div class="value">{ftd_pan_kpi["Dis %"]:.1f}%</div><hr><span style="color:#777;">MTD</span>
        <div class="value">{mtd_pan_kpi["Dis %"]:.1f}%</div></div>
</div>

<h3>📊 FTD vs MTD KPI Summary</h3>
{html_table(kpi_df)}

<h3>Day-level Performance (main)</h3>
{html_table(day_level_main)}

<p>More day-level tabs pushed to the Google Sheet: COCO Brands, COCO Regions, COCO Sources, AOV Buckets, Discount Buckets.</p>

<h3>🏢 COCO vs FOFO (FTD | MTD)</h3>
{html_table(compare_df)}

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

# Send (comment out send_mail() during testing if you don't want mails sent)
send_mail(
    subject=f"MTD Dashboard | {today.strftime('%d-%b-%Y')}",
    body=body
)
