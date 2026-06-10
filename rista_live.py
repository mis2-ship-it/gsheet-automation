import os
import json
import time
import jwt
import requests
import pandas as pd
from datetime import datetime, timedelta
import gspread
from google.oauth2.service_account import Credentials
import matplotlib.pyplot as plt
from io import BytesIO
from email.mime.image import MIMEImage

print("🚀 Live Script Started")

# ---------------- AUTH ---------------- #

API_KEY = os.environ["API_KEY"]
SECRET_KEY = os.environ["SECRET_KEY"]

def get_token():
    payload = {"iss": API_KEY, "iat": int(time.time())}
    return jwt.encode(payload, SECRET_KEY, algorithm="HS256")

def headers():
    return {
        "x-api-key": API_KEY,
        "x-api-token": get_token(),
        "content-type": "application/json"
    }

# ---------------- GOOGLE ---------------- #

creds = Credentials.from_service_account_info(
    json.loads(os.environ["GOOGLE_CREDENTIALS"]),
    scopes=[
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive"
    ]
)

client = gspread.authorize(creds)
sheet_url = "https://docs.google.com/spreadsheets/d/1CVUS-BSBfDIoQI4Yk2GB4_Zp1CIJRF-9YRfpvCih-FM/edit"

retry = 5

for i in range(retry):

    try:

        spreadsheet = client.open_by_url(sheet_url)

        print("✅ Connected to Google Sheet")

        break

    except Exception as e:

        print(f"⚠️ Google Sheet connection failed ({i+1}/{retry})")
        print(str(e))

        time.sleep(10)

else:

    raise Exception("❌ Failed to connect Google Sheet after retries")

# ---------------- TIME ---------------- #

now = datetime.utcnow() + timedelta(hours=5, minutes=30)

print("⏰ Auto Trigger Time:", now)
print("🕒 IST Time:", now)

# ---------------- BUSINESS DATE FIX ---------------- #

def get_business_day(now):
    if now.hour < 6:
        return (now - timedelta(days=1)).date()
    return now.date()

business_day = get_business_day(now)

today = business_day.strftime("%Y-%m-%d")
last_week = (business_day - timedelta(days=7)).strftime("%Y-%m-%d")
last2week = (business_day - timedelta(days=14)).strftime("%Y-%m-%d")
month_on_month = (business_day - timedelta(days=28)).strftime("%Y-%m-%d")
last_year = (business_day - timedelta(days=364)).strftime("%Y-%m-%d")

print("📅 Business Day:", today)
print("📅 Last Week:", last_week)
print(f"🧠 Business Window: {business_day} 09:00 → Next Day 06:00")

# ---------------- FETCH BRANCH ---------------- #

b_resp = requests.get("https://api.ristaapps.com/v1/branch/list", headers=headers())
data = b_resp.json()
data = data.get("data", []) if isinstance(data, dict) else data

branches = [b["branchCode"] for b in data if b.get("status") == "Active"]

print("🏪 Branch count:", len(branches))

# ---------------- FETCH SALES ---------------- #

from concurrent.futures import ThreadPoolExecutor, as_completed

def fetch_branch_data(branch, day):
    all_data = []
    last_key = None

    while True:
        params = {"branch": branch, "day": day}
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
                return pd.DataFrame()

            js = r.json()
            data = js.get("data", [])

            if not data:
                break

            all_data.append(pd.json_normalize(data))

            last_key = js.get("lastKey")
            if not last_key:
                break

        except:
            return pd.DataFrame()

    return pd.concat(all_data, ignore_index=True) if all_data else pd.DataFrame()


def fetch_sales(day):

    results = []

    # 🔥 THREAD CONTROL (IMPORTANT)
    max_threads = 10   # safe limit (don’t exceed 15 for API safety)

    with ThreadPoolExecutor(max_workers=max_threads) as executor:
        futures = [executor.submit(fetch_branch_data, b, day) for b in branches]

        for future in as_completed(futures):
            df = future.result()
            if df is not None and not df.empty:
                results.append(df)

    return pd.concat(results, ignore_index=True) if results else pd.DataFrame()

# ---------------- RUN ---------------- #

today_df = fetch_sales(today)
lastweek_df = fetch_sales(last_week)

if today_df.empty:
    print("❌ No today data")
    exit()

last2week = (business_day - timedelta(days=14)).strftime("%Y-%m-%d")
month_on_month = (business_day - timedelta(days=28)).strftime("%Y-%m-%d")
last_year = (business_day - timedelta(days=364)).strftime("%Y-%m-%d")

last2week_df = fetch_sales(last2week)
month_on_month_df = fetch_sales(month_on_month)
lastyear_df = fetch_sales(last_year)

# ---------------- DATE CLEAN ---------------- #

def prepare_dates(df):
    if df.empty:
        return df

    df["invoiceDate"] = pd.to_datetime(df["invoiceDate"], errors="coerce").dt.tz_localize(None)

    def get_business_date(dt):
        if pd.isna(dt):
            return pd.NaT
        return (dt - pd.Timedelta(days=1)).date() if dt.hour < 5 else dt.date()

    df["businessDate"] = df["invoiceDate"].apply(get_business_date)
    df["Date"] = df["businessDate"]
    df["Hour"] = df["invoiceDate"].dt.hour

    return df

today_df = prepare_dates(today_df)
lastweek_df = prepare_dates(lastweek_df)
last2week_df = prepare_dates(last2week_df)
month_on_month_df = prepare_dates(month_on_month_df)
lastyear_df = prepare_dates(lastyear_df)

# ---------------- TAGGING ---------------- #

today_df["Data_Type"] = "Today"
lastweek_df["Data_Type"] = "Last Week"
last2week_df["Data_Type"] = "Last 2 Week"
month_on_month_df["Data_Type"] = "Last Month"
lastyear_df["Data_Type"] = "Last Year"

final_df = pd.concat([
    today_df,
    lastweek_df,
    last2week_df,
    month_on_month_df,
    lastyear_df
], ignore_index=True)

# ================================
# 📌 SAFE COLUMN FIX (IMPORTANT)
# ================================

if "channel" not in final_df.columns:
    final_df["channel"] = "Unknown"

if "branchName" not in final_df.columns:
    final_df["branchName"] = "Unknown"


# ---------------- SAFE COLUMN CHECK ---------------- #

required_cols = ["netAmount", "chargeAmount", "status", "branchName", "channel"]
for col in required_cols:
    if col not in final_df.columns:
        final_df[col] = 0

# ---------------- NET SALES ---------------- #

final_df["netAmount"] = pd.to_numeric(final_df["netAmount"], errors="coerce").fillna(0)
final_df["chargeAmount"] = pd.to_numeric(final_df["chargeAmount"], errors="coerce").fillna(0)

final_df["Net Sales"] = (
    (final_df["netAmount"] + final_df["chargeAmount"])
    .where(final_df["status"] == "Closed", 0)
)

# =========================================================
# HELP SHEET MAPPING (FINAL STABLE VERSION)
# =========================================================

sheet = client.open(
    "Sales Dashboard"
).worksheet("Help Sheet")

# =========================================================
# STORE / REGION / AM / TM MASTER
# =========================================================

branch_master = pd.DataFrame(
    sheet.get("G:M")[1:],
    columns=sheet.get("G:M")[0]
)

# Clean text
for col in branch_master.columns:
    branch_master[col] = (
        branch_master[col]
        .astype(str)
        .str.strip()
    )

# =========================================================
# STORE TYPE MAP
# =========================================================

storetype_map = dict(
    zip(
        branch_master["Store Name"],
        branch_master["Ownership"]
    )
)

# =========================================================
# REGION MAP
# =========================================================

region_map = dict(
    zip(
        branch_master["Store Name"],
        branch_master["Region"]
    )
)

# =========================================================
# AM / TM MAP
# =========================================================

branch_master["AM Mail"] = (
    branch_master["AM Mail"]
    .astype(str)
    .str.strip()
    .str.lower()
)

branch_master["TM Mail"] = (
    branch_master["TM Mail"]
    .astype(str)
    .str.strip()
    .str.lower()
)

am_store_map = (
    branch_master
    .groupby("AM Mail")["Store Name"]
    .apply(list)
    .to_dict()
)

tm_region_map = (
    branch_master
    .groupby("TM Mail")["Region"]
    .apply(list)
    .to_dict()
)

# remove blank emails
am_store_map = {
    k: v
    for k, v in am_store_map.items()
    if k and k != "nan"
}

tm_region_map = {
    k: v
    for k, v in tm_region_map.items()
    if k and k != "nan"
}

# =========================================================
# SOURCE + BRAND MASTER
# D:F = Channel, Source Group, Brand
# =========================================================

source_master = pd.DataFrame(
    sheet.get("D:F")[1:],
    columns=sheet.get("D:F")[0]
)

# =========================================================
# CLEAN TEXT
# =========================================================

for col in source_master.columns:
    source_master[col] = (
        source_master[col]
        .astype(str)
        .str.strip()
    )

source_master["Channel"] = (
    source_master["Channel"]
    .astype(str)
    .str.upper()
    .str.strip()
)

# =========================================================
# FINAL DF CHANNEL CLEAN
# =========================================================

final_df["channel"] = (
    final_df["channel"]
    .astype(str)
    .str.upper()
    .str.strip()
)

# =========================================================
# SOURCE MAP
# =========================================================

source_map = dict(
    zip(
        source_master["Channel"],
        source_master["Source Group"]
    )
)

# =========================================================
# BRAND MAP
# =========================================================

brand_map = dict(
    zip(
        source_master["Channel"],
        source_master["Brand"]
    )
)

# =========================================================
# APPLY MAPPING
# =========================================================

final_df["Source Group"] = (
    final_df["channel"]
    .map(source_map)
    .fillna("Others")
)

final_df["Brand"] = (
    final_df["channel"]
    .map(brand_map)
    .fillna("Unknown")
)

final_df["Store Type"] = (
    final_df["branchName"]
    .astype(str)
    .str.strip()
    .map(storetype_map)
    .fillna("UNKNOWN")
)

final_df["Region"] = (
    final_df["branchName"]
    .astype(str)
    .str.strip()
    .map(region_map)
    .fillna("UNKNOWN")
)

# =========================================================
# DEBUG
# =========================================================

print("SOURCE GROUP CHECK")
print(
    final_df["Source Group"]
    .value_counts(dropna=False)
)

print("BRAND CHECK")
print(
    final_df["Brand"]
    .value_counts(dropna=False)
)

print("UNMAPPED CHANNELS")
print(
    set(final_df["channel"].unique())
    - set(source_master["Channel"].unique())
)

print("STORE TYPE CHECK")
print(
    final_df["Store Type"]
    .value_counts(dropna=False)
)

print("AM EMAIL SAMPLE")
print(list(am_store_map.keys())[:5])

print("TM EMAIL SAMPLE")
print(list(tm_region_map.keys())[:5])

print("✅ AM Count:", len(am_store_map))
print("✅ TM Count:", len(tm_region_map))
print("✅ Final Mapping Completed")

# =========================================================
# 📊 MTD DATA PUSH TO GSHEET
# PASTE BEFORE FILTER BLOCK
# =========================================================

import numpy as np
print("AVAILABLE VARIABLES CHECK")

try:
    print("final_df:", len(final_df))
except:
    print("final_df missing")

try:
    print("df:", len(df))
except:
    print("df missing")

try:
    print("sales_df:", len(sales_df))
except:
    print("sales_df missing")

try:
    print("raw_df:", len(raw_df))
except:
    print("raw_df missing")

try:
    print("merged_df:", len(merged_df))
except:
    print("merged_df missing")
print("🚀 MTD Data Creation Started")

# =========================================================
# MTD DATA (RAW DATA)
# =========================================================

month_start = business_day.replace(day=1)
yesterday = business_day - timedelta(days=1)

mtd_df = sales_df[
    (
        pd.to_datetime(
            reqcolumns["businessDate"]
        ).dt.date >= month_start
    )
    &
    (
        pd.to_datetime(
            reqcolumns["businessDate"]
        ).dt.date <= yesterday
    )
    &
    (
        reqcolumns["status"] == "Closed"
    )
].copy()

print(
    mtd_df["businessDate"]
    .sort_values()
    .unique()[:20]
)
print(
    "MTD DATE RANGE:",
    mtd_df["businessDate"].min(),
    "to",
    mtd_df["businessDate"].max()
)

print("✅ MTD Rows:", len(mtd_df))
# =========================================================
# REGION HELP SHEET (ONLY FOR MTD)
# =========================================================

mtd_spreadsheet = client.open_by_key(
    "1g4vuRZPy7qsUvDzF5yYM60VKWTL2r0VSDvtvNl06hiY"
)

region_help_ws = mtd_spreadsheet.worksheet(
    "Region_Help_Sheet"
)

region_help_df = pd.DataFrame(
    region_help_ws.get_all_records()
)

# clean
for col in region_help_df.columns:

    region_help_df[col] = (
        region_help_df[col]
        .astype(str)
        .str.strip()
    )

storetype_map = dict(
    zip(
        region_help_df["Branch"],
        region_help_df["Store Type"]
    )
)

region_map = dict(
    zip(
        region_help_df["Branch"],
        region_help_df["Region"]
    )
)

mtd_df["branchName"] = (
    mtd_df["branchName"]
    .astype(str)
    .str.strip()
)

mtd_df["Store Type"] = (
    mtd_df["branchName"]
    .map(storetype_map)
    .fillna("UNKNOWN")
)

mtd_df["Region"] = (
    mtd_df["branchName"]
    .map(region_map)
    .fillna("UNKNOWN")
)

print("✅ Region Mapping Done")

# =========================================================
# SESSION MAP
# =========================================================

def get_session(hour):

    if 8 <= hour < 12:
        return "Breakfast"

    elif 12 <= hour < 16:
        return "Lunch"

    elif 16 <= hour < 19:
        return "Snacks"

    elif 19 <= hour < 23:
        return "Dinner"

    else:
        return "Post Dinner"

mtd_df["Session"] = (
    mtd_df["Hour"]
    .fillna(0)
    .astype(int)
    .apply(get_session)
)

# =========================================================
# WEEK FORMAT (WK 23)
# =========================================================

mtd_df["Date"] = pd.to_datetime(
    mtd_df["businessDate"]
)

mtd_df["Week"] = (
    "WK "
    + mtd_df["Date"]
    .dt.isocalendar()
    .week.astype(str)
)

# =========================================================
# SAFE NUMERIC
# =========================================================

numeric_cols = [
    "Net Sales",
    "discountAmount",
    "grossAmount",
    "taxAmount"
]

for col in numeric_cols:

    if col in mtd_df.columns:

        mtd_df[col] = pd.to_numeric(
            mtd_df[col],
            errors="coerce"
        ).fillna(0)

# =========================================================
# DISCOUNT POSITIVE
# =========================================================

mtd_df["discountAmount"] = (
    mtd_df["discountAmount"]
    .abs()
)

print("✅ Discount Converted Positive")

# =========================================================
# AOV
# =========================================================

if "billNo" in mtd_df.columns:

    order_df = (
        mtd_df.groupby("billNo")
        .size()
        .reset_index(name="dummy")
    )

    orders = (
        mtd_df.groupby(
            [
                "businessDate",
                "branchName",
                "Source Group",
                "Brand",
                "Session",
                "Store Type",
                "Region"
            ]
        )["billNo"]
        .nunique()
        .reset_index(name="Orders")
    )

else:

    orders = (
        mtd_df.groupby(
            [
                "businessDate",
                "branchName",
                "Source Group",
                "Brand",
                "Session",
                "Store Type",
                "Region"
            ]
        )
        .size()
        .reset_index(name="Orders")
    )

# =========================================================
# DEBUG COLUMN CHECK
# =========================================================

print("MTD DF COLUMNS")
print(final_df.columns.tolist())

# =========================================================
# AGGREGATION
# =========================================================

mtd_summary = (
    mtd_df.groupby(
        [
            "businessDate",
            "Week",
            "branchName",
            "Source Group",
            "Brand",
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
        "grossAmount": "sum"
    })
    .reset_index()
)
# =========================================================
# MERGE ORDERS
# =========================================================

merge_cols = [
    "businessDate",
    "branchName",
    "Source Group",
    "Brand",
    "Session",
    "Store Type",
    "Region"
]

mtd_summary = mtd_summary.merge(
    orders,
    on=merge_cols,
    how="left"
)

mtd_summary["Orders"] = (
    mtd_summary["Orders"]
    .fillna(0)
)

# =========================================================
# DIS %
# =========================================================

mtd_summary["Dis %"] = np.where(
    mtd_summary["grossAmount"] > 0,

    (
        mtd_summary["discountAmount"]
        / mtd_summary["grossAmount"]
    ) * 100,

    0
)

# =========================================================
# AOV
# =========================================================

mtd_summary["AOV"] = np.where(
    mtd_summary["Orders"] > 0,

    (
        mtd_summary["Net Sales"]
        / mtd_summary["Orders"]
    ),

    0
)

# =========================================================
# AOV BUCKET
# =========================================================

def aov_bucket(x):

    if x <= 100:
        return "0-100"
    elif x <= 200:
        return "100-200"
    elif x <= 300:
        return "200-300"
    elif x <= 400:
        return "300-400"
    elif x <= 500:
        return "400-500"
    elif x <= 600:
        return "500-600"
    elif x <= 900:
        return "600-900"
    else:
        return ">900"

mtd_summary["AOV Bucket"] = (
    mtd_summary["AOV"]
    .apply(aov_bucket)
)

# =========================================================
# DISCOUNT BUCKET
# =========================================================

def dis_bucket(x):

    if x == 0:
        return "0%"
    elif x <= 10:
        return "1%-10%"
    elif x <= 20:
        return "10%-20%"
    elif x <= 30:
        return "20%-30%"
    elif x <= 40:
        return "30%-40%"
    elif x <= 50:
        return "40%-50%"
    elif x <= 60:
        return "50%-60%"
    elif x <= 70:
        return "60%-70%"
    elif x <= 80:
        return "70%-80%"
    elif x <= 90:
        return "80%-90%"
    else:
        return "90%-100%"

mtd_summary["Discount Bucket"] = (
    mtd_summary["Dis %"]
    .apply(dis_bucket)
)

# =========================================================
# FINAL COLUMN FORMAT
# =========================================================

mtd_summary = mtd_summary.rename(
    columns={
        "Brand": "Brand Name",
        "businessDate": "Date",
        "branchName": "Branch",
        "Source Group": "Source",
        "discountAmount": "Discount",
        "taxAmount": "Taxes",
        "grossAmount": "Gross Sales"
    }
)

required_cols = [
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
    "Orders",
    "Taxes",
    "Gross Sales",
    "Dis %",
    "AOV",
    "AOV Bucket",
    "Discount Bucket"
]

mtd_summary = mtd_summary[
    required_cols
]

# =========================================================
# DATE FIX FOR GSHEET
# =========================================================

mtd_summary["Date"] = (
    pd.to_datetime(
        mtd_summary["Date"]
    )
    .dt.strftime("%Y-%m-%d")
)
# =========================================================
# GSHEET PUSH
# =========================================================

mtd_sheet = client.open_by_key(
    "1g4vuRZPy7qsUvDzF5yYM60VKWTL2r0VSDvtvNl06hiY"
).worksheet("MTD_Data")

mtd_sheet.clear()

mtd_sheet.update(
    [
        mtd_summary.columns.values.tolist()
    ] +
    mtd_summary.values.tolist()
)

print("✅ MTD Data Updated Successfully")

# ---------------- FILTER ---------------- #

today_cut = final_df[
    (final_df["Data_Type"] == "Today") &
    (final_df["Store Type"] == "COCO") &
    (final_df["status"] == "Closed")
]

lastweek_cut = final_df[
    (final_df["Data_Type"] == "Last Week") &
    (final_df["Store Type"] == "COCO") &
    (final_df["status"] == "Closed")
]

last2week_cut = final_df[
    (final_df["Data_Type"] == "Last 2 Week") &
    (final_df["Store Type"] == "COCO") &
    (final_df["status"] == "Closed")
]

month_on_month_cut = final_df[
    (final_df["Data_Type"] == "Last Month") &
    (final_df["Store Type"] == "COCO") &
    (final_df["status"] == "Closed")
]

lastyear_cut = final_df[
    (final_df["Data_Type"] == "Last Year") &
    (final_df["Store Type"] == "COCO") &
    (final_df["status"] == "Closed")
]


# ---------------- BUSINESS HOUR ---------------- #

def map_business_hour(h):
    return h if h >= 8 else h + 24

for df in [today_cut, lastweek_cut]:
    df["BusinessHour"] = df["Hour"].apply(map_business_hour)

# ---------------- TIME FILTER ---------------- #

current_hour = now.hour
cutoff_hour = current_hour + 24 if current_hour < 8 else current_hour - 1

today_cut = today_cut.query("BusinessHour>=8 and BusinessHour<=@cutoff_hour")
lastweek_cut = lastweek_cut.query("BusinessHour>=8 and BusinessHour<=@cutoff_hour")

print("✅ Data Prepared Successfully")

# ---------------- APPLY SAME TIME FILTER TO L2W & LY ---------------- #

last2week_cut["BusinessHour"] = last2week_cut["Hour"].apply(map_business_hour)
month_on_month_cut["BusinessHour"] = month_on_month_cut["Hour"].apply(map_business_hour)
lastyear_cut["BusinessHour"] = lastyear_cut["Hour"].apply(map_business_hour)

last2week_cut = last2week_cut[
    (last2week_cut["BusinessHour"] >= 8) &
    (last2week_cut["BusinessHour"] <= cutoff_hour)
]

month_on_month_cut = month_on_month_cut[
    (month_on_month_cut["BusinessHour"] >= 8) &
    (month_on_month_cut["BusinessHour"] <= cutoff_hour)
]

lastyear_cut = lastyear_cut[
    (lastyear_cut["BusinessHour"] >= 8) &
    (lastyear_cut["BusinessHour"] <= cutoff_hour)
]

# ---------------- SESSION ---------------- #

def get_session(h):
    if 8 <= h <= 11: return "Breakfast"
    elif 12 <= h <= 15: return "Lunch"
    elif 16 <= h <= 19: return "Snacks"
    elif 20 <= h <= 23: return "Dinner"
    else: return "Post Dinner"

today_cut["Session"] = today_cut["Hour"].apply(get_session)
lastweek_cut["Session"] = lastweek_cut["Hour"].apply(get_session)

def add_session(df):
    if "Session" not in df.columns:
        df["Session"] = df["Hour"].apply(get_session)
    return df


final_df = add_session(final_df)

# =========================================================
# 🔥 KPI FUNCTION
# =========================================================

def build_kpi(df_today, df_lw, label=None):

    def calc(df):
        if df is None or df.empty:
            return 0,0,0,0
        return (
            df["grossAmount"].sum(),
            df["discountAmount"].sum(),
            df["Net Sales"].sum(),
            len(df)
        )

    gt, dt, nt, tt = calc(df_today)
    gl, dl, nl, tl = calc(df_lw)

    data = pd.DataFrame({
        "Parameters": ["Gross","Discount","Net","Txn","AOV","Discount %"],
        "Today": [gt,dt,nt,tt,nt/max(tt,1),dt/max(gt,1)*100],
        "Last Week": [gl,dl,nl,tl,nl/max(tl,1),dl/max(gl,1)*100]
    })

    data["Growth %"] = ((data["Today"]-data["Last Week"])/data["Last Week"].replace(0,1))*100

    if label:
        data.insert(0,label[0],label[1])

    return data.round(2)

#Store Metrics

def calc_store_metrics(df, lw_df):

    def agg(d):
        return (
            d["Net Sales"].sum(),
            d["grossAmount"].sum(),
            d["discountAmount"].sum()
        )

    t_net, t_gross, t_disc = agg(df)
    l_net, l_gross, l_disc = agg(lw_df)

    return {
        "Today Rev": t_net,
        "LW Rev": l_net,
        "Growth %": (t_net - l_net) / max(l_net, 1) * 100,
        "Today Dis %": (t_disc / max(t_gross,1)) * 100,
        "LW Dis %": (l_disc / max(l_gross,1)) * 100,
        "Changes %": ((t_disc / max(t_gross,1)) - (l_disc / max(l_gross,1))) * 100
    }

# =========================================================
# 📅 DATE LOGIC (CRITICAL FIX)
# =========================================================

def get_same_weekday_last_year(date):
    last_year_date = date - pd.DateOffset(years=1)
    
    # Align weekday
    while last_year_date.weekday() != date.weekday():
        last_year_date += timedelta(days=1)
    
    return last_year_date


# =========================================================
# 📈 OVERALL EXTENDED FUNCTION
# =========================================================

def build_overall_extended(today_df, lw_df, l2w_df, mom_df, ly_df):

    def calc(df):
        if df is None or df.empty:
            return 0,0,0,0
        return (
            df["grossAmount"].sum(),
            df["discountAmount"].sum(),
            df["Net Sales"].sum(),
            len(df)
        )

    gt,dt,nt,tt = calc(today_df)
    gl,dl,nl,tl = calc(lw_df)
    g2,d2,n2,t2 = calc(l2w_df)
    gm,dm,nm,tm = calc(mom_df)
    gy,dy,ny,ty = calc(ly_df)

    df = pd.DataFrame({
        "Parameters":["Gross","Discount","Net","Txn","AOV","Discount %"],
        "Today":[gt,dt,nt,tt,nt/max(tt,1),dt/max(gt,1)*100],
        "Last Week":[gl,dl,nl,tl,nl/max(tl,1),dl/max(gl,1)*100],
        "Last 2 Week":[g2,d2,n2,t2,n2/max(t2,1),d2/max(g2,1)*100],
        "Last Month":[gm,dm,nm,tm,nm/max(tm,1),dm/max(gm,1)*100],
        "Last Year":[gy,dy,ny,ty,ny/max(ty,1),dy/max(gy,1)*100]
    })

    # Growth calculations
    df["LW Growth %"] = ((df["Today"]-df["Last Week"]) / df["Last Week"].replace(0,1)) * 100
    df["L2W Growth %"] = ((df["Today"]-df["Last 2 Week"]) / df["Last 2 Week"].replace(0,1)) * 100
    df["MoM Growth %"] = ((df["Today"]-df["Last Month"]) / df["Last Month"].replace(0,1)) * 100
    df["LY Growth %"] = ((df["Today"]-df["Last Year"]) / df["Last Year"].replace(0,1)) * 100

    # =========================================================
    # 🔮 EOD PROJECTION
    # =========================================================

    growth = ((nt - nl) / max(nl,1)) * 100

    lw_full = final_df[
        (final_df["Date"] == lw_df["Date"].max()) &
        (final_df["Store Type"] == "COCO") &
        (final_df["status"] == "Closed")
    ]["Net Sales"].sum()

    eod = lw_full * (1 + growth/100)

    df["EOD Projection"] = 0.0
    df.loc[df["Parameters"]=="Net","EOD Projection"] = round(eod,2)

    return df.round(2), eod

    

# =========================================================
# 🔥 FINAL EXECUTION
# =========================================================

def prepare_data_cuts(final_df):

    today_df = final_df[
        (final_df["Data_Type"] == "Today") &
        (final_df["Store Type"] == "COCO") &
        (final_df["status"] == "Closed")
    ].copy()

    lw_df = final_df[
        (final_df["Data_Type"] == "Last Week") &
        (final_df["Store Type"] == "COCO") &
        (final_df["status"] == "Closed")
    ].copy()

    l2w_df = final_df[
        (final_df["Data_Type"] == "Last 2 Week") &
        (final_df["Store Type"] == "COCO") &
        (final_df["status"] == "Closed")
    ].copy()

    mom_df = final_df[
        (final_df["Data_Type"] == "Last Month") &
        (final_df["Store Type"] == "COCO") &
        (final_df["status"] == "Closed")
    ].copy()

    ly_df = final_df[
        (final_df["Data_Type"] == "Last Year") &
        (final_df["Store Type"] == "COCO") &
        (final_df["status"] == "Closed")
    ].copy()

    # Business date (safe)
    today = final_df["Date"].dropna().max()

    return today_df, lw_df, l2w_df, mom_df, ly_df, today

print("Today rows:", len(today_cut))
print("LW rows:", len(lastweek_cut))
print("L2W rows:", len(last2week_cut))
print("MoM rows:", len(month_on_month_cut))
print("LY rows:", len(lastyear_cut))

# =====================================================
# 📌 STORE FILTER
# =====================================================

def filter_store_data(store_list):
    return final_df[
        (final_df["branchName"].isin(store_list)) &
        (final_df["Store Type"] == "COCO") &
        (final_df["status"] == "Closed")
    ].copy()


# =====================================================
# 📌 STORE KPI TABLE
# =====================================================

def store_kpi(df):
    grouped = df.groupby("branchName")

    rows = []

    for store, g in grouped:

        lw = lastweek_cut[lastweek_cut["branchName"] == store]

        t_rev = g["Net Sales"].sum()
        lw_rev = lw["Net Sales"].sum()

        growth = ((t_rev - lw_rev) / max(lw_rev, 1)) * 100

        t_disc = (g["discountAmount"].sum() / max(g["grossAmount"].sum(), 1)) * 100
        lw_disc = (lw["discountAmount"].sum() / max(lw["grossAmount"].sum(), 1)) * 100

        rows.append({
            "Store Name": store,
            "Today Rev": round(t_rev, 2),
            "LW Rev": round(lw_rev, 2),
            "Growth %": round(growth, 2),
            "Today Dis %": round(t_disc, 2),
            "LW Dis %": round(lw_disc, 2),
            "Changes %": round(t_disc - lw_disc, 2)
        })

    return pd.DataFrame(rows)

# =====================================================
# 📌 SESSION REPORT
# =====================================================

def session_report(df, lw_df):
    out = []

    for store in df["branchName"].unique():

        s_df = df[df["branchName"] == store]
        s_lw = lw_df[lw_df["branchName"] == store]

        for session in ["Breakfast","Lunch","Snacks","Dinner","Post Dinner"]:

            t = s_df[s_df["Session"] == session]["Net Sales"].sum()
            lw = s_lw[s_lw["Session"] == session]["Net Sales"].sum()

            growth = ((t - lw) / max(lw, 1)) * 100

            out.append({
                "Store Name": store,
                "Session": session,
                "Today Rev": round(t, 2),
                "LW Rev": round(lw, 2),
                "Growth %": round(growth, 2)
            })

    return pd.DataFrame(out)


# =====================================================
# 📌 BRAND REPORT
# =====================================================

def brand_report(df, lw_df):
    rows = []

    for store in df["branchName"].unique():

        s_df = df[df["branchName"] == store]
        s_lw = lw_df[lw_df["branchName"] == store]

        for brand in s_df["Brand"].unique():

            t = s_df[s_df["Brand"] == brand]
            lw = s_lw[s_lw["Brand"] == brand]

            t_rev = t["Net Sales"].sum()
            lw_rev = lw["Net Sales"].sum()

            growth = ((t_rev - lw_rev) / max(lw_rev, 1)) * 100

            t_disc = (t["discountAmount"].sum() / max(t["grossAmount"].sum(), 1)) * 100
            lw_disc = (lw["discountAmount"].sum() / max(lw["grossAmount"].sum(), 1)) * 100

            rows.append({
                "Store Name": store,
                "Brand": brand,
                "Today Rev": round(t_rev, 2),
                "LW Rev": round(lw_rev, 2),
                "Growth %": round(growth, 2),
                "Today Dis %": round(t_disc, 2),
                "LW Dis %": round(lw_disc, 2),
                "Changes %": round(t_disc - lw_disc, 2)
            })

    return pd.DataFrame(rows)


# =========================================================
# 🔥 INSIGHT ENGINE
# =========================================================

def generate_insight(overall):

    try:
        row = overall[overall["Parameters"]=="Net"].iloc[0]

        lw = row["LW Growth %"]
        l2w = row["L2W Growth %"]
        mom = row["MoM Growth %"]
        ly = row["LY Growth %"]

        text = f"{lw:+.1f}% vs LW, {l2w:+.1f}% vs L2W, {mom:+.1f}% vs MoM, {ly:+.1f}% vs LY"

        if lw>0 and ly<0:
            text += " → ⚠️ slowdown"
        elif lw>0 and ly>0:
            text += " → 🚀 strong growth"
        elif lw<0:
            text += " → 🔻 decline"

        return text
    except:
        return "Insight not available"

# =========================================================
# 🔥 SAFE ANALYSIS BUILDER
# =========================================================

def safe_kpi_builder(df_today, df_lw, col, label):

    if df_today.empty or col not in df_today.columns:
        return pd.DataFrame()

    grouped_today = df_today.groupby(col)
    grouped_lw = df_lw.groupby(col)

    frames = []

    for key in grouped_today.groups.keys():

        t_df = grouped_today.get_group(key)
        lw_df = grouped_lw.get_group(key) if key in grouped_lw.groups else pd.DataFrame()

        frames.append(build_kpi(t_df, lw_df, (label, key)))

    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()



# ---------------- SUMMARY ---------------- #

today_total = today_cut["Net Sales"].sum()
lw_total = lastweek_cut["Net Sales"].sum()

growth = ((today_total - lw_total) / max(lw_total, 1)) * 100

lw_full_day = final_df[
    (final_df["Data_Type"] == "Last Week") &
    (final_df["Store Type"] == "COCO") &
    (final_df["status"] == "Closed")
]["Net Sales"].sum()

eod_projection = lw_full_day * (1 + (growth / 100))

summary = pd.DataFrame({
    "Metric": ["Total Sales"],
    "Today": [today_total],
    "Last Week (Till Now)": [lw_total],
    "Growth %": [growth],
    "EOD Projection": [eod_projection]
}).round(2)

print("✅ Summary Created")



# ---------------- HOURLY ANALYSIS ---------------- #

hourly_today = today_cut.groupby("BusinessHour")["Net Sales"].sum()
hourly_lw = lastweek_cut.groupby("BusinessHour")["Net Sales"].sum()

hourly_analysis = pd.DataFrame({
    "Today": hourly_today,
    "Last Week": hourly_lw
}).fillna(0)

hourly_analysis["Growth %"] = ((hourly_analysis["Today"] - hourly_analysis["Last Week"]) /
                               hourly_analysis["Last Week"].replace(0,1))*100

hourly_analysis = hourly_analysis.reset_index()
hourly_analysis["Hour"] = hourly_analysis["BusinessHour"].apply(lambda x: x if x < 24 else x-24)
hourly_analysis = hourly_analysis.sort_values("BusinessHour")

# ---------------- HOURLY TREND ---------------- #

hourly_analysis["Spike"] = hourly_analysis["Growth %"].apply(
    lambda x: "🚀 Spike" if x > 50 else ("🔻 Drop" if x < -30 else "")
)

# =========================================================
# 🔥 OVERALL ANALYSIS Summary
# =========================================================

overall, eod = build_overall_extended(
    today_cut,
    lastweek_cut,
    last2week_cut,
    month_on_month_cut,
    lastyear_cut
)

insight_text = generate_insight(overall)

print("🧠 Insight:", insight_text)

print("✅ Summary Created")

# =========================================================
# 🎯 TARGET SUMMARY
# =========================================================

target_ws = spreadsheet.worksheet("Target Sheet")

target_df = pd.DataFrame(
    target_ws.get_all_records()
)

target_df["Date"] = pd.to_datetime(
    target_df["Date"]
).dt.date

today_target_row = target_df[
    target_df["Date"] == business_day
]

if not today_target_row.empty:

    # =====================================================
    # SAFE TARGET FETCH
    # =====================================================

    try:
        total_target = float(
            str(
                today_target_row["Total Target"]
                .iloc[0]
            )
            .replace(",", "")
            .strip()
        )
    except:
        total_target = 0

    try:
        offline_target = float(
            str(
                today_target_row["Offline Target"]
                .iloc[0]
            )
            .replace(",", "")
            .strip()
        )
    except:
        offline_target = 0

    try:
        online_target = float(
            str(
                today_target_row["Online Target"]
                .iloc[0]
            )
            .replace(",", "")
            .strip()
        )
    except:
        online_target = 0


today_sales_total = today_cut["Net Sales"].sum()

instore_sales = today_cut[
    today_cut["Source Group"] == "In Store"
]["Net Sales"].sum()

online_sales = (
    today_sales_total - instore_sales
)

offline_mix = (
    instore_sales /
    max(today_sales_total, 1)
)

online_mix = (
    online_sales /
    max(today_sales_total, 1)
)

offline_eod = eod * offline_mix
online_eod = eod * online_mix


target_summary = pd.DataFrame([
    {
        "Metric": "Total",
        "Target": round(total_target,2),
        "EOD Projection": round(eod,2),
        "Ach %": round(
            (eod /
             max(total_target,1))*100,
            2
        )
    },
    {
        "Metric": "Offline",
        "Target": round(offline_target,2),
        "EOD Projection": round(offline_eod,2),
        "Ach %": round(
            (offline_eod /
             max(offline_target,1))*100,
            2
        )
    },
    {
        "Metric": "Online",
        "Target": round(online_target,2),
        "EOD Projection": round(online_eod,2),
        "Ach %": round(
            (online_eod /
             max(online_target,1))*100,
            2
        )
    }
])

print("✅ Target Summary Created")


# =========================================================
# 🔥 BRAND ANALYSIS
# =========================================================

brand_rows = []

brands = sorted(today_cut["Brand"].dropna().unique())

for brand in brands:

    t = today_cut[today_cut["Brand"] == brand]
    lw = lastweek_cut[lastweek_cut["Brand"] == brand]

    t_rev = t["Net Sales"].sum()
    lw_rev = lw["Net Sales"].sum()

    growth = ((t_rev - lw_rev) / max(lw_rev, 1)) * 100

    t_gross = t["grossAmount"].sum()
    lw_gross = lw["grossAmount"].sum()

    t_disc = (t["discountAmount"].sum() / max(t_gross, 1)) * 100
    lw_disc = (lw["discountAmount"].sum() / max(lw_gross, 1)) * 100

    disc_change = t_disc - lw_disc

    brand_rows.append({
        "Brand": brand,
        "Today Rev": round(t_rev, 2),
        "LW Rev": round(lw_rev, 2),
        "Growth %": round(growth, 2),
        "Today Dis %": round(t_disc, 2),
        "LW Dis %": round(lw_disc, 2),
        "Dis Change %": round(disc_change, 2)
    })

brand_summary = pd.DataFrame(brand_rows)

print("✅ Brand Summary Created")

# =========================================================
# 🔥 SOURCE ANALYSIS
# =========================================================

source_rows = []

sources = sorted(
    today_cut["Source Group"]
    .dropna()
    .unique()
)

for source in sources:

    t = today_cut[
        today_cut["Source Group"] == source
    ]

    lw = lastweek_cut[
        lastweek_cut["Source Group"] == source
    ]

    t_rev = t["Net Sales"].sum()
    lw_rev = lw["Net Sales"].sum()

    growth = (
        (t_rev - lw_rev)
        / max(lw_rev, 1)
    ) * 100

    t_gross = t["grossAmount"].sum()
    lw_gross = lw["grossAmount"].sum()

    t_disc = (
        t["discountAmount"].sum()
        / max(t_gross, 1)
    ) * 100

    lw_disc = (
        lw["discountAmount"].sum()
        / max(lw_gross, 1)
    ) * 100

    disc_change = (
        t_disc - lw_disc
    )

    source_rows.append({
        "Source Group": source,
        "Today Rev": round(t_rev, 2),
        "LW Rev": round(lw_rev, 2),
        "Growth %": round(growth, 2),
        "Today Dis %": round(t_disc, 2),
        "LW Dis %": round(lw_disc, 2),
        "Dis Change %": round(disc_change, 2)
    })

source_summary = pd.DataFrame(source_rows)

print("SOURCE SUMMARY CHECK")
print(source_summary)

# =========================================================
# 🔥 BRAND x SOURCE
# =========================================================

brand_source_rows = []

source = sorted(
    today_cut["Source Group"]
    .dropna()
    .unique()
)

brands_required = [
    "Frozen Bottle",
    "Madno",
    "Boba Bar",
    "Lubov"
]

for brand in brands_required:

    # BRAND HEADER
    brand_source_rows.append({
        "Brand": f"🔹 {brand}",
        "Source Group": "Total",
        "Today Rev": "",
        "LW Rev": "",
        "Growth %": "",
        "Today Dis %": "",
        "LW Dis %": "",
        "Dis Change %": ""
    })

    for source in sources:

        t = today_cut[
            (today_cut["Brand"] == brand)
            & (today_cut["Source Group"] == source)
        ]

        lw = lastweek_cut[
            (lastweek_cut["Brand"] == brand)
            & (lastweek_cut["Source Group"] == source)
        ]

        t_rev = t["Net Sales"].sum()
        lw_rev = lw["Net Sales"].sum()

        growth = (
            (t_rev - lw_rev)
            / max(lw_rev, 1)
        ) * 100

        t_disc = (
            t["discountAmount"].sum()
            / max(t["grossAmount"].sum(), 1)
        ) * 100

        lw_disc = (
            lw["discountAmount"].sum()
            / max(lw["grossAmount"].sum(), 1)
        ) * 100

        disc_change = (
            t_disc - lw_disc
        )

        brand_source_rows.append({
            "Brand": "",
            "Source Group": source,
            "Today Rev": round(t_rev, 2),
            "LW Rev": round(lw_rev, 2),
            "Growth %": round(growth, 2),
            "Today Dis %": round(t_disc, 2),
            "LW Dis %": round(lw_disc, 2),
            "Dis Change %": round(disc_change, 2)
        })

brand_source_analysis = pd.DataFrame(
    brand_source_rows
)

print("✅ Brand Source Analysis Created")

# =========================================================
# 🔥 REGION x SOURCE
# =========================================================

region_source_rows = []

source = sorted(
    today_cut["Source Group"]
    .dropna()
    .unique()
)

regions_required = ["KA", "MH", "TN", "Kerela"]

for region in regions_required:

    region_source_rows.append({
        "Region": f"🔹 {region}",
        "Source Group": "Total",
        "Today Rev": "",
        "LW Rev": "",
        "Growth %": "",
        "Today Dis %": "",
        "LW Dis %": "",
        "Dis Change %": ""
    })

    for source in sources:

        t = today_cut[
            (today_cut["Region"] == region) &
            (today_cut["Source Group"] == source)
        ]

        lw = lastweek_cut[
            (lastweek_cut["Region"] == region) &
            (lastweek_cut["Source Group"] == source)
        ]

        t_rev = t["Net Sales"].sum()
        lw_rev = lw["Net Sales"].sum()

        growth = ((t_rev - lw_rev) / max(lw_rev, 1)) * 100

        t_disc = (
            t["discountAmount"].sum()
            / max(t["grossAmount"].sum(), 1)
        ) * 100

        lw_disc = (
            lw["discountAmount"].sum()
            / max(lw["grossAmount"].sum(), 1)
        ) * 100

        disc_change = t_disc - lw_disc

        region_source_rows.append({
            "Region": "",
            "Source Group": source,
            "Today Rev": round(t_rev, 2),
            "LW Rev": round(lw_rev, 2),
            "Growth %": round(growth, 2),
            "Today Dis %": round(t_disc, 2),
            "LW Dis %": round(lw_disc, 2),
            "Dis Change %": round(disc_change, 2)
        })

region_source_analysis = pd.DataFrame(region_source_rows)

print("✅ Region Source Analysis Created")

# =========================================================
# 🔥 SESSION ANALYSIS
# =========================================================

sessions = ["Breakfast", "Lunch", "Snacks", "Dinner", "Post Dinner"]

# ---------------- BRAND SESSION ---------------- #

brand_session = pd.pivot_table(
    today_cut,
    index="Brand",
    columns="Session",
    values="Net Sales",
    aggfunc="sum",
    fill_value=0
)

lw_brand_session = pd.pivot_table(
    lastweek_cut,
    index="Brand",
    columns="Session",
    values="Net Sales",
    aggfunc="sum",
    fill_value=0
)

for s in sessions:

    if s not in brand_session.columns:
        brand_session[s] = 0

    if s not in lw_brand_session.columns:
        lw_brand_session[s] = 0

    brand_session[f"{s} Growth %"] = (
        (brand_session[s] - lw_brand_session[s])
        / lw_brand_session[s].replace(0, 1)
    ) * 100

brand_session = brand_session.reset_index()

print("✅ Brand Session Analysis Created")

# ---------------- Source SESSION ---------------- #

source_session = pd.pivot_table(
    today_cut,
    index="Source Group",
    columns="Session",
    values="Net Sales",
    aggfunc="sum",
    fill_value=0
)

lw_source_session = pd.pivot_table(
    lastweek_cut,
    index="Source Group",
    columns="Session",
    values="Net Sales",
    aggfunc="sum",
    fill_value=0
)

for s in sessions:

    if s not in source_session.columns:
        source_session[s] = 0

    if s not in lw_source_session.columns:
        lw_source_session[s] = 0

    source_session[f"{s} Growth %"] = (
        (source_session[s] - lw_source_session[s])
        / lw_source_session[s].replace(0, 1)
    ) * 100

source_session = source_session.reset_index()

print("✅ Source Session Analysis Created")

# ---------------- REGION SESSION ---------------- #

region_session = pd.pivot_table(
    today_cut,
    index="Region",
    columns="Session",
    values="Net Sales",
    aggfunc="sum",
    fill_value=0
)

lw_region_session = pd.pivot_table(
    lastweek_cut,
    index="Region",
    columns="Session",
    values="Net Sales",
    aggfunc="sum",
    fill_value=0
)

for s in sessions:

    if s not in region_session.columns:
        region_session[s] = 0

    if s not in lw_region_session.columns:
        lw_region_session[s] = 0

    region_session[f"{s} Growth %"] = (
        (region_session[s] - lw_region_session[s])
        / lw_region_session[s].replace(0, 1)
    ) * 100

region_session = region_session.reset_index()

print("✅ Region Session Analysis Created")


# =========================================================
# 🔥 ALL ANALYSIS (SAFE & CLEAN)
# =========================================================

source_analysis = safe_kpi_builder(
    today_cut,
    lastweek_cut,
    "Source Group",
    "Source Group"
)

region_analysis = safe_kpi_builder(
    today_cut,
    lastweek_cut,
    "Region",
    "Region"
)

brand_analysis = safe_kpi_builder(
    today_cut,
    lastweek_cut,
    "Brand",
    "Brand"
)

session_analysis = safe_kpi_builder(
    today_cut,
    lastweek_cut,
    "Session",
    "Session"
)

print("✅ All Analysis Completed")

# =========================================================
# 📊 CHART DATA BLOCK
# =========================================================
chart_df = today_cut.copy()
# =========================================================
# BRAND SALES CHART
# =========================================================

brand_chart_df = (
    chart_df.groupby("Brand")["Net Sales"]
    .sum()
    .reset_index()
    .sort_values(
        "Net Sales",
        ascending=False
    )
)

# =========================================================
# SOURCE MIX CHART
# =========================================================

source_chart_df = (
    chart_df.groupby("Source Group")["Net Sales"]
    .sum()
    .reset_index()
)

# =========================================================
# BRAND x SOURCE DISCOUNT %
# =========================================================

discount_brand_source = (
    chart_df.groupby(
        ["Brand", "Source Group"]
    )
    .agg({
        "discountAmount": "sum",
        "grossAmount": "sum"
    })
    .reset_index()
)

discount_brand_source["Discount %"] = (
    discount_brand_source["discountAmount"]
    / discount_brand_source[
        "grossAmount"
    ].replace(0, 1)
) * 100

print("✅ Today Chart Data Prepared")

# =========================================================
# 📈 HOURLY SALES TREND CHART DATA
# =========================================================

hourly_chart_df = hourly_analysis.copy()

print("✅ Chart Data Prepared")

# =========================================================
# 📈 HOURLY SALES TREND CHART
# =========================================================

def create_hourly_chart():

    chart_df = hourly_analysis.copy()

    if chart_df.empty:
        return None

    fig, ax = plt.subplots(figsize=(10, 4))

    # TODAY LINE
    ax.plot(
        chart_df["Hour"],
        chart_df["Today"],
        marker="o",
        linewidth=2,
        label="Today"
    )

    # LAST WEEK LINE
    ax.plot(
        chart_df["Hour"],
        chart_df["Last Week"],
        marker="o",
        linewidth=2,
        label="Last Week"
    )

    ax.set_title(
        "Hourly Sales Trend (Today vs Last Week)"
    )

    ax.set_xlabel("Hour")
    ax.set_ylabel("Net Sales")

    ax.legend()

    ax.grid(True)

    # =====================================================
    # TODAY DATA LABELS (Lakhs)
    # =====================================================

    for x, y in zip(
        chart_df["Hour"],
        chart_df["Today"]
    ):

        ax.text(
            x,
            y,
            f"{y/100000:.1f}L",
            fontsize=8,
            ha="center",
            va="bottom",
            fontweight="bold"
        )

    # =====================================================
    # LAST WEEK DATA LABELS (Lakhs)
    # =====================================================

    for x, y in zip(
        chart_df["Hour"],
        chart_df["Last Week"]
    ):

        ax.text(
            x,
            y,
            f"{y/100000:.1f}L",
            fontsize=8,
            ha="center",
            va="top"
        )

    img_buffer = BytesIO()

    plt.tight_layout()

    plt.savefig(
        img_buffer,
        format="png",
        bbox_inches="tight"
    )

    img_buffer.seek(0)

    plt.close()

    return img_buffer

print("✅ Hourly Chart Ready")

# =========================================================
# 📊 BRAND SALES CHART
# =========================================================

def create_brand_chart():

    if brand_chart_df.empty:
        return None

    fig, ax = plt.subplots(figsize=(10, 5))

    bars = ax.bar(
        brand_chart_df["Brand"],
        brand_chart_df["Net Sales"]
    )

    ax.set_title("Brand Sales")

    ax.set_xlabel("Brand")
    ax.set_ylabel("Net Sales")

    plt.xticks(rotation=45)

    ax.grid(True)

    # =====================================================
    # DATA LABELS
    # =====================================================

    for bar in bars:

        height = bar.get_height()

        ax.text(
            bar.get_x() + bar.get_width() / 2,
            height,
            f'{height/100000:.1f}L',
            ha='center',
            va='bottom',
            fontsize=9,
            fontweight='bold'
        )

    img_buffer = BytesIO()

    plt.tight_layout()

    plt.savefig(
        img_buffer,
        format="png",
        bbox_inches="tight"
    )

    img_buffer.seek(0)

    plt.close()

    return img_buffer

# =========================================================
# 📦 SOURCE MIX CHART
# =========================================================

def create_source_chart():

    if source_chart_df.empty:
        return None

    fig, ax = plt.subplots(figsize=(7, 7))

    ax.pie(
        source_chart_df["Net Sales"],
        labels=source_chart_df["Source Group"],
        autopct="%1.1f%%"
    )

    ax.set_title("Source Mix")

    img_buffer = BytesIO()

    plt.tight_layout()

    plt.savefig(
        img_buffer,
        format="png",
        bbox_inches="tight"
    )

    img_buffer.seek(0)

    plt.close()

    return img_buffer


# =========================================================
# 💸 BRAND X SOURCE DISCOUNT % CHART
# =========================================================

def create_discount_chart():

    if discount_brand_source.empty:
        return None

    pivot_df = discount_brand_source.pivot_table(
        index="Brand",
        columns="Source Group",
        values="Discount %",
        aggfunc="sum",
        fill_value=0
    )

    fig, ax = plt.subplots(figsize=(10, 5))

    pivot_df.plot(
        kind="bar",
        ax=ax
    )

    ax.set_title(
        "Brand x Source Discount %"
    )

    ax.set_xlabel("Brand")
    ax.set_ylabel("Discount %")

    plt.xticks(rotation=45)

    ax.grid(True)

    # =====================================================
    # DATA LABELS
    # =====================================================

    for container in ax.containers:

        for bar in container:

            height = bar.get_height()

            if height > 0:

                ax.text(
                    bar.get_x() + bar.get_width()/2,
                    height,
                    f"{height:.1f}%",
                    ha="center",
                    va="bottom",
                    fontsize=8,
                    fontweight="bold"
                )

    img_buffer = BytesIO()

    plt.tight_layout()

    plt.savefig(
        img_buffer,
        format="png",
        bbox_inches="tight"
    )

    img_buffer.seek(0)

    plt.close()

    return img_buffer

# =========================================================
# 🔥 TOP 10 STORES
# =========================================================

top_stores = (
    today_cut.groupby("branchName")
    .agg(Today_Sales=("Net Sales", "sum"))
    .sort_values("Today_Sales", ascending=False)
    .head(10)
)

lw_store = (
    lastweek_cut.groupby("branchName")
    .agg(LW_Sales=("Net Sales", "sum"))
)

top_stores = top_stores.join(lw_store, how="left").fillna(0)

top_stores["Growth %"] = (
    (top_stores["Today_Sales"] - top_stores["LW_Sales"])
    / top_stores["LW_Sales"].replace(0, 1)
) * 100

top_stores = top_stores.reset_index()
top_stores.rename(columns={"branchName": "Store Name"}, inplace=True)

top_stores = top_stores.round(2)

# =========================================================
# 🔥 BOTTOM 10 STORES
# =========================================================

bottom_stores = (
    today_cut.groupby("branchName")
    .agg(Today_Sales=("Net Sales", "sum"))
    .sort_values("Today_Sales", ascending=True)  # 👈 change here
    .head(10)
)

lw_store = (
    lastweek_cut.groupby("branchName")
    .agg(LW_Sales=("Net Sales", "sum"))
)

bottom_stores = bottom_stores.join(lw_store, how="left").fillna(0)

bottom_stores["Growth %"] = (
    (bottom_stores["Today_Sales"] - bottom_stores["LW_Sales"])
    / bottom_stores["LW_Sales"].replace(0, 1)
) * 100

bottom_stores = bottom_stores.reset_index()
bottom_stores.rename(columns={"branchName": "Store Name"}, inplace=True)

bottom_stores = bottom_stores.round(2)


# =========================================================
# 🔍 DEBUG (CORRECT VARIABLES)
# =========================================================


print("🔍 Top Stores Check")
print(top_stores.head())

print("🔍 Bottom Stores Check")
print(bottom_stores.head())


# ---------------- PUSH ---------------- #

def push(name, df):
    try:
        ws = spreadsheet.worksheet(name)
    except:
        ws = spreadsheet.add_worksheet(title=name, rows="1000", cols="50")

    ws.clear()
    ws.update([df.columns.tolist()] + df.astype(str).values.tolist())


        
# =====================================================
# FINAL HTML TABLE
# =====================================================

def styled_html(df):

    df = df.copy()

    growth_cols = [c for c in df.columns if "Growth" in c]

    text_cols = [
        "Parameters",
        "Parameter",
        "Metric",
        "Source Group",
        "Region",
        "Brand",
        "Session",
        "Hour",
        "Store Name",
        "Insight"
    ]

    # =====================================================
    # FORMAT
    # =====================================================

    for col in df.columns:

        # ---------------- TEXT COLUMNS ---------------- #

        if col in text_cols:

            df[col] = df[col].fillna("").astype(str)

        # ---------------- GROWTH COLUMNS ---------------- #

        elif col in growth_cols:

            def growth_format(x):

                try:

                    if str(x).strip() == "":
                        return ""

                    val = float(str(x).replace("%", "").replace(",", "").strip())

                    bg = "#d4edda" if val >= 0 else "#f8d7da"
                    color = "#155724" if val >= 0 else "#721c24"

                    return (
                        f'<div style="'
                        f'background:{bg};'
                        f'color:{color};'
                        f'padding:4px 8px;'
                        f'border-radius:4px;'
                        f'font-weight:bold;'
                        f'text-align:center;'
                        f'white-space:nowrap;'
                        f'">'
                        f'{val:.2f}%'
                        f'</div>'
                    )

                except:
                    return ""

            df[col] = df[col].apply(growth_format)

        # ---------------- NORMAL NUMBER COLUMNS ---------------- #

        else:

            df[col] = pd.to_numeric(df[col], errors="coerce")

            df[col] = df[col].apply(
                lambda x: f"{x:,.2f}" if pd.notnull(x) else ""
            )

    # =====================================================
    # HTML CONVERT
    # =====================================================

    html = df.to_html(
        index=False,
        escape=False,
        border=0
    )

    # =====================================================
    # TABLE STYLE
    # =====================================================

    html = html.replace(
        '<table class="dataframe">',
        '''
        <table style="
            border-collapse:collapse;
            width:auto;
            min-width:60%;
            font-family:Arial;
            font-size:12px;
            background:white;
        ">
        '''
    )

    # =====================================================
    # HEADER STYLE
    # =====================================================

    html = html.replace(
        '<th>',
        '''
        <th style="
            background:#1f4e78;
            color:white;
            padding:8px;
            border:1px solid #d9d9d9;
            text-align:center;
            white-space:nowrap;
        ">
        '''
    )

    # =====================================================
    # CELL STYLE
    # =====================================================

    html = html.replace(
        '<td>',
        '''
        <td style="
            padding:6px;
            border:1px solid #e5e5e5;
            text-align:left;
            white-space:nowrap;
        ">
        '''
    )

    return html

# ---------------- SAFE TABLE ---------------- #

def safe_table(df, title):

    if df is None or df.empty:
        return f"<p>⚠️ No data available for {title}</p>"

    return styled_html(df)


# ---------------- SEND EMAIL ---------------- #

def send_email():

    import smtplib
    import os

    from email.mime.multipart import MIMEMultipart
    from email.mime.text import MIMEText
    from email.mime.image import MIMEImage

    EMAIL_USER = os.environ.get("EMAIL_USER")
    EMAIL_PASS = os.environ.get("EMAIL_PASS")
    TO_EMAIL = os.environ.get("EMAIL_TO")
    CC_EMAIL = os.environ.get("EMAIL_CC")

    report_time = now.replace(
        minute=0,
        second=0,
        microsecond=0
    )

    # =====================================================
    # CREATE MESSAGE
    # =====================================================

    source_df = pd.DataFrame(source_rows)

    print("SOURCE DF CHECK")
    print(source_df.head(10))
    
    msg = MIMEMultipart("related")

    msg["From"] = EMAIL_USER
    msg["To"] = TO_EMAIL
    msg["Cc"] = CC_EMAIL

    msg["Subject"] = (
        f"📊 Live Sales Dashboard - "
        f"{report_time.strftime('%d %b %Y')}"
    )

    # =====================================================
    # EMAIL BODY
    # =====================================================

    body = f"""

    <div style="
        font-family:Arial;
        background:#f4f6f9;
        padding:20px;
    ">

        <h1 style="color:#1f4e78;">
            📊 LIVE SALES DASHBOARD
        </h1>

        <p>
            🕒 Data Till:
            <b>{report_time.strftime('%d %b %Y %I:%M %p')}</b>
        </p>

        <div style="
            background:white;
            padding:15px;
            border-radius:8px;
            margin-bottom:20px;
        ">

            <h2>🧠 Executive Insight</h2>

            <p style="
                font-size:15px;
                font-weight:bold;
                color:#333;
            ">
                {insight_text}
            </p>

        </div>

        <h2>📈 Overall KPI</h2>
        {styled_html(overall)}

        <br><br>

        <h2>🎯 Target vs EOD Projection</h2>
        {styled_html(target_summary)}

        <br><br>

        <h2>🏷️ Brand Sales</h2>

        <img src="cid:brand_chart"
        style="
        width:60%;
        max-width:900px;
        border-radius:8px;
        margin-bottom:15px;
        ">

        <br><br>

        <h2>📦 Source Mix</h2>

        <img src="cid:source_chart"
        style="
        width:60%;
        max-width:700px;
        border-radius:8px;
        margin-bottom:15px;
        ">

        <br><br>

        <h2>💸 Brand x Source Discount %</h2>

        <img src="cid:discount_chart"
        style="
        width:60%;
        max-width:900px;
        border-radius:8px;
        margin-bottom:15px;
        ">

        <br><br>


        <h2>🏷️ Brand Summary</h2>
        {styled_html(brand_summary)}

        <br><br>

        <h2>🏷️ Source Summary</h2>
        {styled_html(source_summary)}

        <br><br>

        <h2>🏷️ Brand Source Analysis</h2>
        {styled_html(brand_source_analysis)}

        <br><br>

        <h2>🌍 Region Source Analysis</h2>
        {styled_html(region_source_analysis)}

        <br><br>

        <h2>🍽️ Brand Session Analysis</h2>
        {styled_html(brand_session)}

        <br><br>

        <h2>🌍 Region Session Analysis</h2>
        {styled_html(region_session)}

        <br><br>

        <h2>🌍 Source Session Analysis</h2>
        {styled_html(source_session)}

        <br><br>

        <h2>⏰ Hourly Sales Trend</h2>

        <img src="cid:hourly_chart"
        style="
        width:60%;
        max-width:900px;
        border-radius:8px;
        margin-bottom:15px;
        ">

        {styled_html(hourly_analysis)}

        <br><br>

        <h2>🏆 Top Stores</h2>
        {styled_html(top_stores)}

        <br><br>

        <h2>⚠️ Bottom Stores</h2>
        {styled_html(bottom_stores)}

    </div>

    """

    # =====================================================
    # ATTACH HTML BODY
    # =====================================================

    msg.attach(MIMEText(body, "html"))

    # =====================================================
    # ATTACH CHARTS
    # =====================================================

    chart_mapping = {

        "brand_chart":
            create_brand_chart(),

        "source_chart":
            create_source_chart(),

        "discount_chart":
            create_discount_chart(),

        "hourly_chart":
            create_hourly_chart()
    }

    for cid, chart_buffer in chart_mapping.items():

        if chart_buffer:

            image = MIMEImage(
                chart_buffer.read()
            )

            image.add_header(
                "Content-ID",
                f"<{cid}>"
            )

            image.add_header(
                "Content-Disposition",
                "inline",
                filename=f"{cid}.png"
            )

            msg.attach(image)

    # =====================================================
    # RECEIVERS
    # =====================================================

    receivers = []

    if TO_EMAIL:
        receivers += TO_EMAIL.split(",")

    if CC_EMAIL:
        receivers += CC_EMAIL.split(",")

    # =====================================================
    # SEND EMAIL
    # =====================================================

    server = smtplib.SMTP(
        "smtp.gmail.com",
        587
    )

    server.starttls()

    server.login(
        EMAIL_USER,
        EMAIL_PASS
    )

    server.sendmail(
        EMAIL_USER,
        receivers,
        msg.as_string()
    )

    server.quit()

    print("📩 Email Sent Successfully")

# ================================
# 📌 ROLE DASHBOARD ENGINE
# ================================

def build_role_scope(role, identifier):

    if role == "AM":

        stores = am_store_map.get(identifier, [])

        df_today = today_cut[
            today_cut["branchName"].isin(stores)
        ].copy()

        df_lw = lastweek_cut[
            lastweek_cut["branchName"].isin(stores)
        ].copy()

        return df_today, df_lw, stores, None


    elif role == "TM":

        regions = tm_region_map.get(identifier, [])

        df_today = today_cut[
            today_cut["Region"].isin(regions)
        ].copy()

        df_lw = lastweek_cut[
            lastweek_cut["Region"].isin(regions)
        ].copy()

        return df_today, df_lw, None, regions


    else:
        return pd.DataFrame(), pd.DataFrame(), None, None

# ================================
# CLEAN PERIOD FILTERING
# ================================

today_df_clean = final_df[final_df["Data_Type"] == "Today"].copy()
lw_df_clean = final_df[final_df["Data_Type"] == "Last Week"].copy()

# =====================================================
# 📩 AM MAIL
# =====================================================

def send_am_mail():

    import smtplib
    from email.mime.multipart import MIMEMultipart
    from email.mime.text import MIMEText

    EMAIL_USER = os.environ.get("EMAIL_USER")
    EMAIL_PASS = os.environ.get("EMAIL_PASS")

    report_time = now.replace(
        minute=0,
        second=0,
        microsecond=0
    )

    def calc_store_metrics(t, l):

        t_rev = t["Net Sales"].sum()
        l_rev = l["Net Sales"].sum()

        growth = (
            ((t_rev - l_rev) / max(l_rev, 1))
        ) * 100

        t_disc = (
            t["discountAmount"].sum()
            / max(t["grossAmount"].sum(), 1)
        ) * 100

        l_disc = (
            l["discountAmount"].sum()
            / max(l["grossAmount"].sum(), 1)
        ) * 100

        return {
            "Today Rev": round(t_rev, 2),
            "LW Rev": round(l_rev, 2),
            "Growth %": round(growth, 2),
            "Today Dis %": round(t_disc, 2),
            "LW Dis %": round(l_disc, 2),
            "Change %": round(t_disc - l_disc, 2)
        }

    for am_email, stores in am_store_map.items():
    
        am_email = (
            str(am_email)
            .strip()
            .lower()
        )
    
        if (
            not am_email
            or am_email == "nan"
        ):
            continue
    
        stores = [
            str(x).strip()
            for x in stores
            if str(x).strip()
        ]
    
        print(
            f"📨 Sending AM Mail → "
            f"{am_email}"
        )
    
        print(
            "🏪 Store Count:",
            len(stores)
        )

        df_today = today_cut[
            today_cut["branchName"].isin(stores)
        ].copy()

        df_lw = lastweek_cut[
            lastweek_cut["branchName"].isin(stores)
        ].copy()

        if not df_today.empty:
            df_today["Session"] = (
                df_today["Hour"]
                .apply(get_session)
            )
        
        if not df_lw.empty:
            df_lw["Session"] = (
                df_lw["Hour"]
                .apply(get_session)
            )

        print(
            "Today Rows:",
            len(df_today),
            "| LW Rows:",
            len(df_lw)
        )
        
        if df_today.empty and df_lw.empty:
            print(
                f"⚠️ No data for {am_email}"
            )
            continue

        # =====================================================
        # STORE DASHBOARD
        # =====================================================

        store_rows = []

        for store in stores:

            t = df_today[
                df_today["branchName"] == store
            ]

            l = df_lw[
                df_lw["branchName"] == store
            ]

            m = calc_store_metrics(t, l)

            m = {
                "Store Name": store,
                **m
            }

            store_rows.append(m)

        store_df = pd.DataFrame(store_rows)

        # =====================================================
        # SESSION DASHBOARD
        # =====================================================

        session_rows = []

        session_order = [
            "Breakfast",
            "Lunch",
            "Snacks",
            "Dinner",
            "Post Dinner"
        ]

        for store in stores:

            t_store = df_today[
                df_today["branchName"] == store
            ]

            l_store = df_lw[
                df_lw["branchName"] == store
            ]

            row = {
                "Store Name": store
            }

            for s in session_order:

                row[s] = round(
                    t_store[
                        t_store["Session"] == s
                    ]["Net Sales"].sum(),
                    2
                )

            today_rev = t_store["Net Sales"].sum()
            lw_rev = l_store["Net Sales"].sum()

            growth = (
                ((today_rev - lw_rev)
                 / max(lw_rev, 1))
            ) * 100

            row["Today Rev"] = round(today_rev, 2)
            row["LW Rev"] = round(lw_rev, 2)
            row["Growth %"] = round(growth, 2)

            session_rows.append(row)

        session_df = pd.DataFrame(session_rows)

        # =====================================================
        # BRAND DASHBOARD
        # =====================================================

        brand_rows = []

        for brand in sorted(
            df_today["Brand"]
            .dropna()
            .unique()
        ):

            b_t = df_today[
                df_today["Brand"] == brand
            ]

            b_l = df_lw[
                df_lw["Brand"] == brand
            ]

            for store in stores:

                t = b_t[
                    b_t["branchName"] == store
                ]

                l = b_l[
                    b_l["branchName"] == store
                ]

                if t.empty and l.empty:
                    continue

                m = calc_store_metrics(t, l)

                m = {
                    "Brand": brand,
                    "Store Name": store,
                    **m
                }

                brand_rows.append(m)

        brand_df = pd.DataFrame(brand_rows)

        # =====================================================
        # SOURCE DASHBOARD
        # =====================================================
        
        source_rows = []
        
        sources = sorted(
            df_today["Source Group"]
            .dropna()
            .unique()
        )
        
        for source in sources:
        
            # ==========================================
            # SOURCE HEADER
            # ==========================================
        
            source_rows.append({
                "Source Group": f"🔹 {source}",
                "Store Name": "Total",
                "Today Rev": "",
                "LW Rev": "",
                "Growth %": "",
                "Today Dis %": "",
                "LW Dis %": "",
                "Change %": ""
            })
        
            s_t = df_today[
                df_today["Source Group"] == source
            ]
        
            s_l = df_lw[
                df_lw["Source Group"] == source
            ]
        
            for store in stores:
        
                t = s_t[
                    s_t["branchName"] == store
                ]
        
                l = s_l[
                    s_l["branchName"] == store
                ]
        
                if t.empty and l.empty:
                    continue
        
                m = calc_store_metrics(t, l)
        
                m = {
                    "Source Group": "",
                    "Store Name": store,
                    **m
                }
        
                source_rows.append(m)
        
        source_df = pd.DataFrame(source_rows)
        
        print("SOURCE DF CHECK")
        print(source_df.head(20))

        # =====================================================
        # EMAIL
        # =====================================================

        msg = MIMEMultipart()

        msg["From"] = EMAIL_USER
        msg["To"] = am_email

        msg["Subject"] = (
            f"📊 AM Sales Dashboard - "
            f"{report_time.strftime('%d %b %Y')}"
        )

        body = f"""

        <p>
        🕒 Data Till:
        <b>{report_time.strftime('%d %b %Y %I:%M %p')}</b>
        </p>
        
        <br><br>
        
        <h2>🏪 Store Wise Report</h2>
        {styled_html(store_df)}

        <br><br>

        <h2>🍽 Session Report</h2>
        {styled_html(session_df)}

        <br><br>

        <h2>🏷 Brand Report</h2>
        {styled_html(brand_df)}

        <br><br>

        <h2>📦 Source Report</h2>
        {styled_html(source_df)}
        """

        msg.attach(
            MIMEText(body, "html")
        )

        server = smtplib.SMTP(
            "smtp.gmail.com",
            587
        )

        server.starttls()

        server.login(
            EMAIL_USER,
            EMAIL_PASS
        )

        server.sendmail(
            EMAIL_USER,
            [am_email],
            msg.as_string()
        )

        server.quit()

        print(
            "📩 AM Mail Sent →",
            am_email
        )
# =====================================================
# 📩 TM MAIL
# =====================================================

def send_tm_mail():

    import smtplib

    from email.mime.multipart import MIMEMultipart
    from email.mime.text import MIMEText

    EMAIL_USER = os.environ.get("EMAIL_USER")
    EMAIL_PASS = os.environ.get("EMAIL_PASS")

    report_time = now.replace(
        minute=0,
        second=0,
        microsecond=0
    )

    # =====================================================
    # STORE METRIC FUNCTION
    # =====================================================

    def calc_store_metrics(t, l):

        t_rev = t["Net Sales"].sum()
        l_rev = l["Net Sales"].sum()

        growth = (
            ((t_rev - l_rev) / max(l_rev, 1))
        ) * 100

        t_disc = (
            t["discountAmount"].sum()
            / max(t["grossAmount"].sum(), 1)
        ) * 100

        l_disc = (
            l["discountAmount"].sum()
            / max(l["grossAmount"].sum(), 1)
        ) * 100

        return {
            "Today Rev": round(t_rev, 2),
            "LW Rev": round(l_rev, 2),
            "Growth %": round(growth, 2),
            "Today Dis %": round(t_disc, 2),
            "LW Dis %": round(l_disc, 2),
            "Change %": round(
                t_disc - l_disc,
                2
            )
        }

    # =====================================================
    # LOOP TM MAIL
    # =====================================================

    for tm_email, regions in tm_region_map.items():
    
        tm_email = (
            str(tm_email)
            .strip()
            .lower()
        )
    
        if (
            not tm_email
            or tm_email == "nan"
        ):
            continue
    
        regions = [
            str(x).strip()
            for x in regions
            if str(x).strip()
        ]
    
        print(
            f"📨 Sending TM Mail → "
            f"{tm_email}"
        )
    
        print(
            "🌍 Region Count:",
            len(regions)
        )

        # =====================================================
        # REGION FILTER
        # =====================================================

        df_today = today_cut[
            today_cut["Region"].isin(regions)
        ].copy()
        
        df_lw = lastweek_cut[
            lastweek_cut["Region"].isin(regions)
        ].copy()

        if df_today.empty:
            continue

        print(
            "Today Rows:",
            len(df_today),
            "| LW Rows:",
            len(df_lw)
        )
        
        if df_today.empty and df_lw.empty:
            print(
                f"⚠️ No data for {tm_email}"
            )
            continue

        # =====================================================
        # SESSION TAG
        # =====================================================

        if not df_today.empty:
            df_today["Session"] = (
                df_today["Hour"]
                .apply(get_session)
            )
        
        if not df_lw.empty:
            df_lw["Session"] = (
                df_lw["Hour"]
                .apply(get_session)
            )

        stores = sorted(
            df_today["branchName"]
            .dropna()
            .unique()
        )

        # =====================================================
        # STORE DASHBOARD
        # =====================================================

        store_rows = []

        for store in stores:

            t = df_today[
                df_today["branchName"] == store
            ]

            l = df_lw[
                df_lw["branchName"] == store
            ]

            m = calc_store_metrics(t, l)

            m = {
                "Store Name": store,
                **m
            }

            store_rows.append(m)

        store_df = pd.DataFrame(store_rows)

        # =====================================================
        # SESSION DASHBOARD
        # =====================================================

        session_rows = []

        session_order = [
            "Breakfast",
            "Lunch",
            "Snacks",
            "Dinner",
            "Post Dinner"
        ]

        for store in stores:

            t_store = df_today[
                df_today["branchName"] == store
            ]

            l_store = df_lw[
                df_lw["branchName"] == store
            ]

            row = {
                "Store Name": store
            }

            # Session Sales
            for s in session_order:

                row[s] = round(
                    t_store[
                        t_store["Session"] == s
                    ]["Net Sales"].sum(),
                    2
                )

            # Revenue
            today_rev = (
                t_store["Net Sales"]
                .sum()
            )

            lw_rev = (
                l_store["Net Sales"]
                .sum()
            )

            growth = (
                ((today_rev - lw_rev)
                 / max(lw_rev, 1))
            ) * 100

            row["Today Rev"] = round(
                today_rev, 2
            )

            row["LW Rev"] = round(
                lw_rev, 2
            )

            row["Growth %"] = round(
                growth, 2
            )

            session_rows.append(row)

        session_df = pd.DataFrame(
            session_rows
        )

        # =====================================================
        # BRAND DASHBOARD
        # =====================================================

        brand_rows = []

        brands = sorted(
            df_today["Brand"]
            .dropna()
            .unique()
        )

        for brand in brands:

            b_t = df_today[
                df_today["Brand"] == brand
            ]

            b_l = df_lw[
                df_lw["Brand"] == brand
            ]

            for store in stores:

                t = b_t[
                    b_t["branchName"] == store
                ]

                l = b_l[
                    b_l["branchName"] == store
                ]

                if t.empty and l.empty:
                    continue

                m = calc_store_metrics(
                    t, l
                )

                m = {
                    "Brand": brand,
                    "Store Name": store,
                    **m
                }

                brand_rows.append(m)

        brand_df = pd.DataFrame(
            brand_rows
        )

        # =====================================================
        # SOURCE DASHBOARD
        # =====================================================
        
        source_rows = []
        
        sources = sorted(
            df_today["Source Group"]
            .dropna()
            .unique()
        )
        
        for source in sources:
        
            # ==========================================
            # SOURCE HEADER
            # ==========================================
        
            source_rows.append({
                "Source Group": f"🔹 {source}",
                "Store Name": "Total",
                "Today Rev": "",
                "LW Rev": "",
                "Growth %": "",
                "Today Dis %": "",
                "LW Dis %": "",
                "Change %": ""
            })
        
            s_t = df_today[
                df_today["Source Group"] == source
            ]
        
            s_l = df_lw[
                df_lw["Source Group"] == source
            ]
        
            for store in stores:
        
                t = s_t[
                    s_t["branchName"] == store
                ]
        
                l = s_l[
                    s_l["branchName"] == store
                ]
        
                if t.empty and l.empty:
                    continue
        
                m = calc_store_metrics(t, l)
        
                m = {
                    "Source Group": "",
                    "Store Name": store,
                    **m
                }
        
                source_rows.append(m)
        
        source_df = pd.DataFrame(source_rows)
        
        print("SOURCE DF CHECK")
        print(source_df.head(20))

        # =====================================================
        # EMAIL
        # =====================================================

        msg = MIMEMultipart()

        msg["From"] = EMAIL_USER
        msg["To"] = tm_email

        msg["Subject"] = (
            f"📊 TM Sales Dashboard - "
            f"{report_time.strftime('%d %b %Y')}"
        )

        body = f"""

        <p>
        🕒 Data Till:
        <b>{report_time.strftime('%d %b %Y %I:%M %p')}</b>
        </p>
        
        <br><br>

        <h2>🏪 Store Wise Report</h2>
        {styled_html(store_df)}

        <br><br>

        <h2>🍽 Session Report</h2>
        {styled_html(session_df)}

        <br><br>

        <h2>🏷 Brand Report</h2>
        {styled_html(brand_df)}

        <br><br>

        <h2>📦 Source Report</h2>
        {styled_html(source_df)}

        """

        msg.attach(
            MIMEText(
                body,
                "html"
            )
        )

        server = smtplib.SMTP(
            "smtp.gmail.com",
            587
        )

        server.starttls()

        server.login(
            EMAIL_USER,
            EMAIL_PASS
        )

        server.sendmail(
            EMAIL_USER,
            [tm_email],
            msg.as_string()
        )

        server.quit()

        print(
            "📩 TM Mail Sent →",
            tm_email
        )


# ---------------- EXECUTE ---------------- #

push("Overall", overall)
push("Source Group", source_analysis)
push("Region", region_analysis)
push("Brand", brand_analysis)
push("Session", session_analysis)
push("Top_Stores", top_stores)
push("Bottom_Stores", bottom_stores)
push("Hourly", hourly_analysis)


send_email()        # Full dashboard
send_am_mail()      # AM targeted
send_tm_mail()      # TM targeted

print("🎉 ALL EMAILS SENT SUCCESSFULLY")
