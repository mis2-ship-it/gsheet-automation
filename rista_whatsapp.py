
# =========================================================
# 📱 RISTA LIVE → WHATSAPP AUTOMATION
# =========================================================

import os
import json
import time
import jwt
import requests
import pandas as pd

from datetime import (
    datetime,
    timedelta
)

import gspread

from google.oauth2.service_account import (
    Credentials
)

print("=" * 60)
print("🚀 RISTA WHATSAPP SCRIPT STARTED")
print("=" * 60)

# =========================================================
# 🔐 RISTA API AUTHENTICATION
# =========================================================

API_KEY = os.environ.get(
    "API_KEY"
)

SECRET_KEY = os.environ.get(
    "SECRET_KEY"
)

if not API_KEY:
    raise Exception(
        "❌ API_KEY is missing"
    )

if not SECRET_KEY:
    raise Exception(
        "❌ SECRET_KEY is missing"
    )

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

def headers():

    return {

        "x-api-key":
            API_KEY,

        "x-api-token":
            get_token(),

        "content-type":
            "application/json"
    }

print(
    "✅ Rista API authentication configured"
)

# =========================================================
# 📊 GOOGLE SHEETS AUTHENTICATION
# =========================================================

GOOGLE_CREDENTIALS = os.environ.get(
    "GOOGLE_CREDENTIALS"
)

if not GOOGLE_CREDENTIALS:

    raise Exception(
        "❌ GOOGLE_CREDENTIALS is missing"
    )

try:

    creds = (
        Credentials
        .from_service_account_info(
            json.loads(
                GOOGLE_CREDENTIALS
            ),
            scopes=[
                "https://www.googleapis.com/auth/spreadsheets",
                "https://www.googleapis.com/auth/drive"
            ]
        )
    )

    client = gspread.authorize(
        creds
    )

    print(
        "✅ Google credentials loaded"
    )

except Exception as e:

    print(
        "❌ Google authentication failed"
    )

    print(
        str(e)
    )

    raise

# =========================================================
# 📊 GOOGLE SHEET
# =========================================================

SHEET_URL = (
    "https://docs.google.com/"
    "spreadsheets/d/"
    "1CVUS-BSBfDIoQI4Yk2GB4_"
    "Zp1CIJRF-9YRfpvCih-FM/edit"
)

RETRY_COUNT = 5
spreadsheet = None
for i in range(
    RETRY_COUNT
):

    try:

        print(
            f"🔄 Connecting to Google Sheet "
            f"({i + 1}/{RETRY_COUNT})"
        )

        spreadsheet = (
            client.open_by_url(
                SHEET_URL
            )
        )

        print(
            "✅ Connected to Google Sheet"
        )

        break

    except Exception as e:

        print(
            f"⚠️ Google Sheet connection failed "
            f"({i + 1}/{RETRY_COUNT})"
        )

        print(
            str(e)
        )

        if i < RETRY_COUNT - 1:

            time.sleep(10)


if spreadsheet is None:

    raise Exception(
        "❌ Failed to connect Google Sheet "
        "after retries"
    )

# =========================================================
# 📱 WHATSAPP BACKEND CONFIGURATION
# =========================================================

WHATSAPP_WEBHOOK_DATA_URL = (
    os.environ.get(
        "WHATSAPP_WEBHOOK_DATA_URL"
    )
)
WHATSAPP_DATA_SECRET = (
    os.environ.get(
        "WHATSAPP_DATA_SECRET"
    )
)
if not WHATSAPP_WEBHOOK_DATA_URL:

    raise Exception(
        "❌ WHATSAPP_WEBHOOK_DATA_URL "
        "is missing"
    )


if not WHATSAPP_DATA_SECRET:

    raise Exception(
        "❌ WHATSAPP_DATA_SECRET "
        "is missing"
    )
print(
    "✅ WhatsApp webhook URL configured"
)

print(
    "✅ WhatsApp data secret configured:",
    bool(
        WHATSAPP_DATA_SECRET
    )
)
# =========================================================
# 📱 INITIAL WHATSAPP CONFIGURATION CHECK
# =========================================================

print("=" * 60)

print(
    "📱 WHATSAPP AUTOMATION CONFIGURATION"
)
print(
    "Rista API      :",
    bool(API_KEY)
)

print(
    "Google Sheet   :",
    bool(spreadsheet)
)

print(
    "WhatsApp URL   :",
    bool(
        WHATSAPP_WEBHOOK_DATA_URL
    )
)

print(
    "WhatsApp Secret:",
    bool(
        WHATSAPP_DATA_SECRET
    )
)

print("=" * 60)

print(
    "✅ Rista WhatsApp initialization completed"
)
print("=" * 60)

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

    # 🔥 THREAD CONTROL
    max_threads = 10

    with ThreadPoolExecutor(max_workers=max_threads) as executor:

        futures = [
            executor.submit(fetch_branch_data, b, day)
            for b in branches
        ]

        for future in as_completed(futures):

            df = future.result()

            if df is not None and not df.empty:
                results.append(df)

    # =====================================================
    # FINAL CONCAT
    # =====================================================

    final_sales_df = (
        pd.concat(results, ignore_index=True)
        if results
        else pd.DataFrame()
    )

    # =====================================================
    # RAW DATA CHECK
    # =====================================================

    print("RAW DATA CHECK")
    print(final_sales_df.shape)

    if (
        not final_sales_df.empty
        and "businessDate" in final_sales_df.columns
    ):

        print(
            sorted(
                pd.to_datetime(
                    final_sales_df["businessDate"]
                ).dt.date.unique()
            )[:15]
        )

    return final_sales_df

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

# =========================================================
# ⏰ HOURLY DEBUG
# =========================================================

print("=" * 60)
print("⏰ HOURLY DEBUG")
print("Current clock hour:", now.hour)
print("Hourly Analysis:")

print(
    hourly_analysis[
        ["BusinessHour", "Hour", "Today", "Last Week", "Growth %"]
    ].tail(10)
)

print("=" * 60)

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
# 🔍 WHATSAPP DATA DEBUG
# =========================================================

print("=" * 60)
print("🔍 WHATSAPP DATA DEBUG")
print("=" * 60)

print("Brand Analysis:")
print(
    brand_analysis.columns.tolist()
    if brand_analysis is not None
    else "brand_analysis is None"
)

print("Source Analysis:")
print(
    source_analysis.columns.tolist()
    if source_analysis is not None
    else "source_analysis is None"
)

print("Region Analysis:")
print(
    region_analysis.columns.tolist()
    if region_analysis is not None
    else "region_analysis is None"
)

print("Top Stores:")
print(
    top_stores.columns.tolist()
    if top_stores is not None
    else "top_stores is None"
)

print("=" * 60)


# =========================================================
# 📱 BUILD WHATSAPP BACKEND SNAPSHOT
# =========================================================

def _safe_float(value):
    try:
        if value is None or value == "":
            return 0.0
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _get_kpi(df, parameter, column="Today"):
    try:
        if df is None or df.empty:
            return 0.0

        row = df[
            df["Parameters"].astype(str).str.strip().str.lower()
            == str(parameter).strip().lower()
        ]

        if row.empty or column not in row.columns:
            return 0.0

        return _safe_float(row.iloc[0][column])

    except Exception as e:
        print(
            f"⚠️ KPI read failed | {parameter} | {column} | {e}"
        )
        return 0.0


def _build_analysis_dict(df, key_column):
    result = {}

    if df is None or df.empty:
        return result

    required = {
        key_column,
        "Parameters",
        "Today",
        "Last Week",
        "Growth %"
    }

    if not required.issubset(df.columns):
        print(
            "⚠️ Analysis columns missing:",
            sorted(required - set(df.columns))
        )
        return result

    work = df.copy()

    work[key_column] = (
        work[key_column]
        .astype(str)
        .str.strip()
    )

    work["Parameters"] = (
        work["Parameters"]
        .astype(str)
        .str.strip()
        .str.lower()
    )

    for name in work[key_column].dropna().unique():
        if not name or name.lower() == "nan":
            continue

        rows = work[
            (work[key_column] == name)
            & (work["Parameters"] == "net")
        ]

        if rows.empty:
            continue

        row = rows.iloc[0]

        result[name] = {
            "today": _safe_float(row.get("Today", 0)),
            "lw": _safe_float(row.get("Last Week", 0)),
            "growth": _safe_float(row.get("Growth %", 0))
        }

    return result


def _build_store_dict(df):
    result = {}

    if df is None or df.empty:
        return result

    required = {
        "Store Name",
        "Today_Sales",
        "LW_Sales",
        "Growth %"
    }

    if not required.issubset(df.columns):
        print(
            "⚠️ Store columns missing:",
            sorted(required - set(df.columns))
        )
        return result

    for _, row in df.iterrows():
        name = str(row.get("Store Name", "")).strip()

        if not name or name.lower() == "nan":
            continue

        result[name] = {
            "today": _safe_float(row.get("Today_Sales", 0)),
            "lw": _safe_float(row.get("LW_Sales", 0)),
            "growth": _safe_float(row.get("Growth %", 0))
        }

    return result


def build_whatsapp_snapshot():
    print("=" * 60)
    print("📱 BUILDING WHATSAPP SNAPSHOT")
    print("=" * 60)

    # =========================================================
    # 🏪 ALL STORE SALES
    # =========================================================
    
    stores = {}
    
    try:
    
        # Today's COCO stores
        today_store_df = (
            today_cut
            .groupby("branchName")["Net Sales"]
            .sum()
            .reset_index()
        )
    
        today_store_df.rename(
            columns={
                "branchName": "Store Name",
                "Net Sales": "Today_Sales"
            },
            inplace=True
        )
    
        # Last Week COCO stores
        lw_store_df = (
            lastweek_cut
            .groupby("branchName")["Net Sales"]
            .sum()
            .reset_index()
        )
    
        lw_store_df.rename(
            columns={
                "branchName": "Store Name",
                "Net Sales": "LW_Sales"
            },
            inplace=True
        )
    
        # Merge
        store_df = today_store_df.merge(
            lw_store_df,
            on="Store Name",
            how="outer"
        ).fillna(0)
    
        # Growth
        store_df["Growth %"] = (
            (
                store_df["Today_Sales"]
                -
                store_df["LW_Sales"]
            )
            /
            store_df["LW_Sales"].replace(
                0,
                1
            )
        ) * 100
    
        # Build JSON
        for _, row in store_df.iterrows():
    
            store_name = str(
                row["Store Name"]
            ).strip()
    
            if not store_name:
                continue
    
            stores[store_name] = {
    
                "today":
                    round(
                        float(
                            row["Today_Sales"]
                            or 0
                        ),
                        2
                    ),
    
                "lw":
                    round(
                        float(
                            row["LW_Sales"]
                            or 0
                        ),
                        2
                    ),
    
                "growth":
                    round(
                        float(
                            row["Growth %"]
                            or 0
                        ),
                        2
                    )
            }
    
    except Exception as e:
    
        print(
            "❌ Store snapshot build error:",
            str(e)
        )
    
        stores = {}
    
    snapshot = {
        "date": business_day.strftime("%d-%b-%y"),

        "report_time":
            now.strftime("%I:%M %p"),

        "overall": {

            "gross":
                _get_kpi(
                    overall,
                    "Gross"
                ),

            "net":
                _get_kpi(
                    overall,
                    "Net"
                ),

            "txn":
                _get_kpi(
                    overall,
                    "Txn"
                ),

            "aov":
                _get_kpi(
                    overall,
                    "AOV"
                ),

            "discount":
                _get_kpi(
                    overall,
                    "Discount %"
                ),

            "lw_net":
                _get_kpi(
                    overall,
                    "Net",
                    "Last Week"
                ),

            "lw_growth":
                _get_kpi(
                    overall,
                    "Net",
                    "LW Growth %"
                ),

            "eod_projection":
                _safe_float(eod)
        },

        "brands":
            _build_analysis_dict(
                brand_analysis,
                "Brand"
            ),

        "sources":
            _build_analysis_dict(
                source_analysis,
                "Source Group"
            ),

        "regions":
            _build_analysis_dict(
                region_analysis,
                "Region"
            ),

        # ✅ IMPORTANT
        "stores":
            stores
    }

    print("📅 Date:", snapshot["date"])
    print("🕒 Time:", snapshot["report_time"])
    print("💰 Net:", snapshot["overall"]["net"])
    print("🧾 Txn:", snapshot["overall"]["txn"])
    print("🏷 Brands:", len(snapshot["brands"]))
    print("📦 Sources:", len(snapshot["sources"]))
    print("🌍 Regions:", len(snapshot["regions"]))
    print("🏪 Stores:", len(snapshot["stores"]))
    print("📋 Sections:", list(snapshot.keys()))
    print("=" * 60)

    print("=" * 60)
    print("📱 WHATSAPP STORE SNAPSHOT DEBUG")
    print("=" * 60)
    
    print(
        "Total stores in snapshot:",
        len(stores)
    )
    
    print(
        "Store names:",
        list(stores.keys())
    )
    
    print(
        "Snapshot sections:",
        list(snapshot.keys())
    )
    
    print("=" * 60)
    
    return snapshot



# =========================================================
# 📱 SEND SNAPSHOT TO WHATSAPP BACKEND
# =========================================================

def send_whatsapp_backend_data():
    print("=" * 60)
    print("📱 UPDATING WHATSAPP BACKEND DATA")
    print("=" * 60)

    webhook_url = os.environ.get(
        "WHATSAPP_WEBHOOK_DATA_URL"
    )

    data_secret = os.environ.get(
        "WHATSAPP_DATA_SECRET"
    )

    if not webhook_url:
        print(
            "❌ WHATSAPP_WEBHOOK_DATA_URL not configured"
        )
        return False

    if not data_secret:
        print(
            "❌ WHATSAPP_DATA_SECRET not configured"
        )
        return False

    print("Webhook URL:", webhook_url)
    print("Data Secret configured:", bool(data_secret))

    try:
        payload = build_whatsapp_snapshot()
    except Exception as e:
        print(
            "❌ Failed to build WhatsApp snapshot:",
            repr(e)
        )
        return False

    headers = {
        "Content-Type": "application/json",
        "X-WhatsApp-Data-Secret": data_secret
    }

    print("📤 Sending sections:", list(payload.keys()))

    try:
        response = requests.post(
            webhook_url,
            headers=headers,
            json=payload,
            timeout=30
        )

        print(
            "📱 WhatsApp Backend Status:",
            response.status_code
        )

        print(
            "WhatsApp Backend Response:",
            response.text
        )

        print("=" * 60)

        if response.ok:
            print(
                "✅ WhatsApp backend snapshot updated successfully"
            )
            return True

        print("❌ WhatsApp backend update failed")
        return False

    except Exception as e:
        print(
            "❌ WhatsApp backend request error:",
            repr(e)
        )
        return False


# =========================================================
# 🚀 FINAL EXECUTION — WHATSAPP ONLY
# =========================================================

print("=" * 60)
print("🚀 WHATSAPP LIVE SALES PROCESS STARTED")
print("=" * 60)

whatsapp_backend_updated = send_whatsapp_backend_data()

if whatsapp_backend_updated:
    print("✅ WhatsApp backend data is ready")
else:
    print("⚠️ WhatsApp backend data was NOT updated")

print("=" * 60)
print("🏁 WHATSAPP LIVE SALES PROCESS COMPLETED")
print("=" * 60)

print("=" * 60)
print("📱 WHATSAPP SCRIPT TEST")
print("=" * 60)

print("API configured      :", bool(API_KEY))
print("Google connected    :", bool(spreadsheet))
print("WhatsApp URL        :", bool(WHATSAPP_WEBHOOK_DATA_URL))
print("WhatsApp secret     :", bool(WHATSAPP_DATA_SECRET))

print("=" * 60)
