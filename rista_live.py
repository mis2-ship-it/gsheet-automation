import os
import json
import time
import jwt
import requests
import pandas as pd
from datetime import datetime, timedelta
import gspread
from google.oauth2.service_account import Credentials

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
spreadsheet = client.open_by_url("https://docs.google.com/spreadsheets/d/1CVUS-BSBfDIoQI4Yk2GB4_Zp1CIJRF-9YRfpvCih-FM/edit")

print("✅ Connected to Google Sheet")

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
last_year = (business_day - timedelta(days=365)).strftime("%Y-%m-%d")

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
last_year = (business_day - timedelta(days=365)).strftime("%Y-%m-%d")

last2week_df = fetch_sales(last2week)
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
lastyear_df = prepare_dates(lastyear_df)

# ---------------- TAGGING ---------------- #

today_df["Data_Type"] = "Today"
lastweek_df["Data_Type"] = "Last Week"
last2week_df["Data_Type"] = "Last 2 Week"
lastyear_df["Data_Type"] = "Last Year"

final_df = pd.concat([
    today_df,
    lastweek_df,
    last2week_df,
    lastyear_df
], ignore_index=True)

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

# ---------------- MAPPING ---------------- #

help_ws = spreadsheet.worksheet("Help Sheet")

branch_master = pd.DataFrame(help_ws.get("G:M")[1:], columns=help_ws.get("G:M")[0])
source_master = pd.DataFrame(help_ws.get("D:F")[1:], columns=help_ws.get("D:F")[0])

store_map = dict(zip(branch_master["Store Name"], branch_master["Ownership"]))
region_map = dict(zip(branch_master["Store Name"], branch_master["Region"]))
source_map = dict(zip(source_master["Channel"], source_master["Source"]))
brand_map = dict(zip(source_master["Channel"], source_master["Brand"]))

final_df["Store Type"] = final_df["branchName"].map(store_map).fillna("Unknown")
final_df["Region"] = final_df["branchName"].map(region_map).fillna("Unknown")
final_df["Source"] = final_df["channel"].map(source_map).fillna("Other")
final_df["Brand"] = final_df["channel"].map(brand_map).fillna("Others")

main_sources = ["In Store", "Swiggy", "Zomato", "Ownly"]
final_df["Source Group"] = final_df["Source"].apply(lambda x: x if x in main_sources else "Others")

# ---------------- FILTER ---------------- #

today_cut = final_df.query('Data_Type=="Today" and `Store Type`=="COCO" and status=="Closed"')
lastweek_cut = final_df.query('Data_Type=="Last Week" and `Store Type`=="COCO" and status=="Closed"')
last2week_cut = final_df.query('Data_Type=="Last 2 Week" and `Store Type`=="COCO" and status=="Closed"')
lastyear_cut = final_df.query('Data_Type=="Last Year" and `Store Type`=="COCO" and status=="Closed"')

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
lastyear_cut["BusinessHour"] = lastyear_cut["Hour"].apply(map_business_hour)

last2week_cut = last2week_cut[
    (last2week_cut["BusinessHour"] >= 8) &
    (last2week_cut["BusinessHour"] <= cutoff_hour)
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

# =========================================================
# 🔥 OVERALL EXTENDED
# =========================================================

def build_overall_extended(today_df, lw_df, l2w_df, ly_df):

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
    gy,dy,ny,ty = calc(ly_df)

    df = pd.DataFrame({
        "Parameters":["Gross","Discount","Net","Txn","AOV","Discount %"],
        "Today":[gt,dt,nt,tt,nt/max(tt,1),dt/max(gt,1)*100],
        "Last Week":[gl,dl,nl,tl,nl/max(tl,1),dl/max(gl,1)*100],
        "Last 2 Week":[g2,d2,n2,t2,n2/max(t2,1),d2/max(g2,1)*100],
        "Last Year":[gy,dy,ny,ty,ny/max(ty,1),dy/max(gy,1)*100]
    })

    df["LW Growth %"] = ((df["Today"]-df["Last Week"])/df["Last Week"].replace(0,1))*100
    df["L2W Growth %"] = ((df["Today"]-df["Last 2 Week"])/df["Last 2 Week"].replace(0,1))*100
    df["LY Growth %"] = ((df["Today"]-df["Last Year"])/df["Last Year"].replace(0,1))*100

    growth = ((nt-nl)/max(nl,1))*100
    lw_full = final_df[
    (final_df["Data_Type"]=="Last Week") &
    (final_df["Store Type"]=="COCO") &
    (final_df["status"]=="Closed")
    ]["Net Sales"].sum()
    eod = lw_full * (1 + growth/100)

    df["EOD Projection"] = 0.0
    df.loc[df["Parameters"]=="Net","EOD Projection"] = round(eod,2)

    return df.round(2)

# =========================================================
# 🔥 INSIGHT ENGINE
# =========================================================

def generate_insight(overall):

    try:
        row = overall[overall["Parameters"]=="Net"].iloc[0]

        lw = row["LW Growth %"]
        ly = row["LY Growth %"]

        text = f"{lw:+.1f}% vs LW, {ly:+.1f}% vs LY"

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
# 🔥 OVERALL ANALYSIS
# =========================================================

overall = build_overall_extended(
    today_cut,
    lastweek_cut,
    last2week_cut,
    lastyear_cut
)

insight_text = generate_insight(overall)

print("🧠 Insight:", insight_text)


# =========================================================
# 🔥 ALL ANALYSIS (SAFE & CLEAN)
# =========================================================

source_analysis = safe_kpi_builder(
    today_cut,
    lastweek_cut,
    "Source Group",
    "Source"
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
# 🔥 BRAND x SOURCE (EXECUTIVE FORMAT)
# =========================================================

sources = ["In Store", "Swiggy", "Zomato"]
params = ["Net Sales", "Txn", "Discount %"]

rows = []

# 👉 Pre-filter once (performance boost)
base_df = final_df[
    (final_df["Store Type"] == "COCO") &
    (final_df["status"] == "Closed")
].copy()

for brand in base_df["Brand"].dropna().unique():

    brand_df = base_df[base_df["Brand"] == brand]

    for param in params:

        row = {
            "Brand": brand,
            "Parameter": param
        }

        for s in sources:

            src_df = brand_df[brand_df["Source Group"] == s]

            def get_vals(data_type):
                temp = src_df[src_df["Data_Type"] == data_type]

                if temp.empty:
                    return 0, 0, 0

                net = temp["Net Sales"].sum()
                txn = len(temp)
                gross = temp["grossAmount"].sum()
                disc_amt = temp["discountAmount"].sum()

                disc = (disc_amt / gross * 100) if gross != 0 else 0

                return net, txn, disc

            t = get_vals("Today")
            lw = get_vals("Last Week")
            l2w = get_vals("Last 2 Week")
            ly = get_vals("Last Year")

            def pick(metric, data):
                if metric == "Net Sales":
                    return data[0]
                elif metric == "Txn":
                    return data[1]
                else:
                    return data[2]

            today_val = pick(param, t)
            lw_val = pick(param, lw)
            l2w_val = pick(param, l2w)
            ly_val = pick(param, ly)

            # 👉 Safe growth calc
            def growth(a, b):
                return ((a - b) / b * 100) if b != 0 else 0

            row[f"{s} (Today)"] = round(today_val, 2)
            row[f"{s} LW %"] = round(growth(today_val, lw_val), 2)
            row[f"{s} L2W %"] = round(growth(today_val, l2w_val), 2)
            row[f"{s} YoY %"] = round(growth(today_val, ly_val), 2)

        rows.append(row)

brand_source_pivot = pd.DataFrame(rows)

# 👉 Ensure no blank parameter
brand_source_pivot["Parameter"] = brand_source_pivot["Parameter"].fillna("Unknown")

print("✅ Brand Source Built")

sources = ["In Store", "Swiggy", "Zomato"]
params = ["Net Sales", "Txn", "Discount %"]

rows = []

# 👉 Pre-filter once (performance boost)
base_df = final_df[
    (final_df["Store Type"] == "COCO") &
    (final_df["status"] == "Closed")
].copy()

for brand in base_df["Brand"].dropna().unique():

    brand_df = base_df[base_df["Brand"] == brand]

    for param in params:

        row = {
            "Brand": brand,
            "Parameter": param
        }

        for s in sources:

            src_df = brand_df[brand_df["Source Group"] == s]

            def get_vals(data_type):
                temp = src_df[src_df["Data_Type"] == data_type]

                if temp.empty:
                    return 0, 0, 0

                net = temp["Net Sales"].sum()
                txn = len(temp)
                gross = temp["grossAmount"].sum()
                disc_amt = temp["discountAmount"].sum()

                disc = (disc_amt / gross * 100) if gross != 0 else 0

                return net, txn, disc

            t = get_vals("Today")
            lw = get_vals("Last Week")
            l2w = get_vals("Last 2 Week")
            ly = get_vals("Last Year")

            def pick(metric, data):
                if metric == "Net Sales":
                    return data[0]
                elif metric == "Txn":
                    return data[1]
                else:
                    return data[2]

            today_val = pick(param, t)
            lw_val = pick(param, lw)
            l2w_val = pick(param, l2w)
            ly_val = pick(param, ly)

            # 👉 Safe growth calc
            def growth(a, b):
                return ((a - b) / b * 100) if b != 0 else 0

            row[f"{s} (Today)"] = round(today_val, 2)
            row[f"{s} LW %"] = round(growth(today_val, lw_val), 2)
            row[f"{s} L2W %"] = round(growth(today_val, l2w_val), 2)
            row[f"{s} YoY %"] = round(growth(today_val, ly_val), 2)

        rows.append(row)

brand_source_pivot = pd.DataFrame(rows)

# 👉 Ensure no blank parameter
brand_source_pivot["Parameter"] = brand_source_pivot["Parameter"].fillna("Unknown")

print("✅ Brand Source Built")

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
# 🔍 DEBUG (CORRECT VARIABLES)
# =========================================================

print("🔍 Brand Source Check")
print(brand_source_pivot.head())

print("🔍 Region Source Check")
print(region_source_pivot.head())

print("🔍 Top Stores Check")
print(top_stores.head())


# ---------------- PUSH ---------------- #

def push(name, df):
    try:
        ws = spreadsheet.worksheet(name)
    except:
        ws = spreadsheet.add_worksheet(title=name, rows="1000", cols="50")

    ws.clear()
    ws.update([df.columns.tolist()] + df.astype(str).values.tolist())


# ---------------- EMAIL ---------------- #

def styled_html(df):

    df = df.copy()
    growth_cols = [c for c in df.columns if "Growth" in c]

    for col in df.columns:

        # Skip text columns
        if col in ["Parameters", "Source", "Region", "Brand", "Session", "Hour", "Store Name"]:
            continue

        # Growth column with color
        if col in growth_cols:
            df[col] = df[col].apply(lambda x:
                f'<span style="background:#d4edda;padding:4px;">{float(x):.2f}%</span>'
                if pd.notnull(x) and float(x) >= 0 else
                f'<span style="background:#f8d7da;padding:4px;">{float(x):.2f}%</span>'
                if pd.notnull(x) else ""
            )
        else:
            df[col] = pd.to_numeric(df[col], errors="coerce")
            df[col] = df[col].apply(lambda x: f"{x:.2f}" if pd.notnull(x) else "")

    # Convert to HTML
    html = df.to_html(index=False, escape=False)

    # ✅ TABLE BORDER + STYLE (INSIDE FUNCTION)
    html = html.replace(
        '<table border="1" class="dataframe">',
        '<table style="border-collapse:collapse;font-family:Arial;font-size:12px;border:1px solid black;">'
    )

    html = html.replace(
        '<th>',
        '<th style="background:#f2f2f2;padding:6px;text-align:center;border:1px solid black;">'
    )

    html = html.replace(
        '<td>',
        '<td style="padding:6px;text-align:right;border:1px solid black;">'
    )

    return html   # ✅ IMPORTANT

def safe_table(df, title):
    if df is None or df.empty:
        return f"<p>⚠️ No data available for {title}</p>"
    return styled_html(df)

    # ----------SEND EMAIL------------------------#
def send_email():

    import smtplib
    from email.mime.multipart import MIMEMultipart
    from email.mime.text import MIMEText

    EMAIL_USER = os.environ.get("EMAIL_USER")
    EMAIL_PASS = os.environ.get("EMAIL_PASS")
    TO_EMAIL = os.environ.get("EMAIL_TO")
    CC_EMAIL = os.environ.get("EMAIL_CC")

    # ✅ CORRECT INDENTATION (inside function)
    report_time = now.replace(minute=0, second=0, microsecond=0)

    msg = MIMEMultipart()
    msg["From"] = EMAIL_USER
    msg["To"] = TO_EMAIL
    msg["Cc"] = CC_EMAIL
    msg["Subject"] = f"📊 Live Sales Report - {report_time.strftime('%d %b %Y')}"

    # ✅ BODY
    body = f"""
    <h2>📊 Executive Summary</h2>
    <h3>Data Till {report_time.strftime('%I:%M %p')}</h3>

    <p style="font-size:14px;font-weight:bold;color:#333;">
    🧠 {insight_text}
    </p>

    <h2>Overall</h2>{styled_html(overall)}

    <h2>Source</h2>{safe_table(source_analysis, "Source")}
    <h2>Region</h2>{safe_table(region_analysis, "Region")}
    <h2>Brand</h2>{safe_table(brand_analysis, "Brand")}
    <h2>Session</h2>{safe_table(session_analysis, "Session")}

    <h2>Hourly Trend</h2>{styled_html(hourly_analysis)}

    <h2>Brand x Source</h2>{styled_html(brand_source_pivot)}

    <h2>Region x Source</h2>{styled_html(region_source_pivot)}

    <h2>Top 10 Stores</h2>{styled_html(top_stores)}
    """

    msg.attach(MIMEText(body, "html"))

    receivers = []
    if TO_EMAIL:
        receivers += TO_EMAIL.split(",")
    if CC_EMAIL:
        receivers += CC_EMAIL.split(",")

    server = smtplib.SMTP("smtp.gmail.com", 587)
    server.starttls()
    server.login(EMAIL_USER, EMAIL_PASS)
    server.sendmail(EMAIL_USER, receivers, msg.as_string())
    server.quit()

    print("📩 Email Sent Successfully")
    
# ---------------- EXECUTE ---------------- #

push("Overall", overall)
push("Source", source_analysis)
push("Region", region_analysis)
push("Brand", brand_analysis)
push("Session", session_analysis)
push("Brand_Source", brand_source_pivot)
push("Region_Source", region_source_pivot)
push("Top_Stores", top_stores)
push("Hourly", hourly_analysis)


send_email()
print("🎉 SUCCESS")
