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

spreadsheet = client.open_by_url(
    "https://docs.google.com/spreadsheets/d/1CVUS-BSBfDIoQI4Yk2GB4_Zp1CIJRF-9YRfpvCih-FM/edit"
)

print("✅ Connected to Google Sheet")

# ---------------- TIME ---------------- #

now = datetime.utcnow() + timedelta(hours=5, minutes=30)
today = now.strftime("%Y-%m-%d")
last_week = (now - timedelta(days=7)).strftime("%Y-%m-%d")

print("🕒 IST Time:", now)

# ---------------- FETCH BRANCH ---------------- #

b_resp = requests.get("https://api.ristaapps.com/v1/branch/list", headers=headers())
data = b_resp.json()

if isinstance(data, dict):
    data = data.get("data", [])

branches = [b["branchCode"] for b in data if isinstance(b, dict) and b.get("status") == "Active"]

print("🏪 Branch count:", len(branches))

# ---------------- FETCH SALES ---------------- #

def fetch_sales(day):
    print(f"\n📥 Fetching sales for {day}")
    all_data = []

    for b in branches:
        last_key = None

        while True:
            params = {"branch": b, "day": day}
            if last_key:
                params["lastKey"] = last_key

            r = requests.get(
                "https://api.ristaapps.com/v1/sales/summary",
                headers=headers(),
                params=params,
                timeout=30
            )

            if r.status_code != 200:
                break

            js = r.json()
            data = js.get("data", [])

            if not data:
                break

            df = pd.json_normalize(data)
            all_data.append(df)

            last_key = js.get("lastKey")
            if not last_key:
                break

    if not all_data:
        return pd.DataFrame()

    final_df = pd.concat(all_data, ignore_index=True)
    print("📊 Rows:", final_df.shape)
    return final_df

# ---------------- RUN ---------------- #

today_df = fetch_sales(today)
lastweek_df = fetch_sales(last_week)

if today_df.empty:
    print("❌ No today data")
    exit()

# ---------------- BUSINESS DATE ---------------- #

today_df["invoiceDate"] = pd.to_datetime(today_df["invoiceDate"], errors="coerce")
lastweek_df["invoiceDate"] = pd.to_datetime(lastweek_df["invoiceDate"], errors="coerce")

today_df["invoiceDate"] = today_df["invoiceDate"].dt.tz_localize(None)
lastweek_df["invoiceDate"] = lastweek_df["invoiceDate"].dt.tz_localize(None)

def get_business_date(dt):
    if pd.isna(dt):
        return pd.NaT
    if dt.hour < 5 or (dt.hour == 5 and dt.minute < 30):
        return (dt - pd.Timedelta(days=1)).date()
    return dt.date()

today_df["businessDate"] = today_df["invoiceDate"].apply(get_business_date)
lastweek_df["businessDate"] = lastweek_df["invoiceDate"].apply(get_business_date)

print("⏱ Business Date CREATED")

# ---------------- LAST COMPLETED HOUR LOGIC ---------------- #

current_hour = now.hour

if current_hour < 8:
    cutoff_hour = current_hour + 24
else:
    cutoff_hour = current_hour - 1   # ✅ LAST COMPLETED HOUR

start_hour = 8

today_cut = today_cut[
    (today_cut["BusinessHour"] >= start_hour) &
    (today_cut["BusinessHour"] <= cutoff_hour)
]

lastweek_cut = lastweek_cut[
    (lastweek_cut["BusinessHour"] >= start_hour) &
    (lastweek_cut["BusinessHour"] <= cutoff_hour)
]

print(f"⏱ Business hours used: {start_hour} to {cutoff_hour}")

# ---------------- BUSINESS TIME WINDOW ---------------- #

current_hour = now.hour

# If before 8 AM → still previous business day continuation
if current_hour < 8:
    cutoff_hour = current_hour + 24
else:
    cutoff_hour = current_hour

start_hour = 8  # business start

today_cut = today_cut[
    (today_cut["BusinessHour"] >= start_hour) &
    (today_cut["BusinessHour"] <= cutoff_hour)
]

lastweek_cut = lastweek_cut[
    (lastweek_cut["BusinessHour"] >= start_hour) &
    (lastweek_cut["BusinessHour"] <= cutoff_hour)
]

print(f"⏱ Business hours considered: {start_hour} to {cutoff_hour}")

# ---------------- DATE + HOUR ---------------- #

today_df["Date"] = today_df["businessDate"]
lastweek_df["Date"] = lastweek_df["businessDate"]

today_df["Hour"] = today_df["invoiceDate"].dt.hour
lastweek_df["Hour"] = lastweek_df["invoiceDate"].dt.hour


# ---------------- MERGE ---------------- #

today_df["Data_Type"] = "Today"
lastweek_df["Data_Type"] = "Last Week"

final_df = pd.concat([today_df, lastweek_df], ignore_index=True)

# ---------------- MAPPING ---------------- #

help_ws = spreadsheet.worksheet("Help Sheet")

branch_data = help_ws.get("G:M")
branch_master = pd.DataFrame(branch_data[1:], columns=branch_data[0])

store_map = dict(zip(branch_master["Store Name"], branch_master["Ownership"]))
region_map = dict(zip(branch_master["Store Name"], branch_master["Region"]))

source_data = help_ws.get("D:F")
source_master = pd.DataFrame(source_data[1:], columns=source_data[0])
source_master.columns = source_master.columns.str.strip()

source_map = dict(zip(source_master["Channel"], source_master["Source"]))
brand_map = dict(zip(source_master["Channel"], source_master["Brand"]))

final_df["Store Type"] = final_df["branchName"].map(store_map).fillna("Unknown")
final_df["Region"] = final_df["branchName"].map(region_map).fillna("Unknown")
final_df["Source"] = final_df["channel"].map(source_map).fillna("Other")
final_df["Brand"] = final_df["channel"].map(brand_map).fillna("Others")

main_sources = ["In Store", "Swiggy", "Zomato", "Ownly"]
final_df["Source Group"] = final_df["Source"].apply(lambda x: x if x in main_sources else "Others")

def get_session(h):
    if 8 <= h <= 11:
        return "Breakfast"
    elif 12 <= h <= 15:
        return "Lunch"
    elif 16 <= h <= 19:
        return "Snacks"
    elif 20 <= h <= 23:
        return "Dinner"
    else:
        return "Post Dinner"

today_cut["Session"] = today_cut["Hour"].apply(get_session)
lastweek_cut["Session"] = lastweek_cut["Hour"].apply(get_session)

print("✅ Mapping Done")

# ---------------- NET SALES ---------------- #

final_df["netAmount"] = pd.to_numeric(final_df["netAmount"], errors="coerce").fillna(0)
final_df["chargeAmount"] = pd.to_numeric(final_df["chargeAmount"], errors="coerce").fillna(0)

final_df["Net Sales"] = (
    (final_df["netAmount"] + final_df["chargeAmount"])
    .where(final_df["status"] == "Closed", 0)
)

print("✅ Net Sales Done")

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

# ---------------- BUSINESS HOUR ---------------- #

def map_business_hour(h):
    return h if h >= 8 else h + 24

today_cut["BusinessHour"] = today_cut["Hour"].apply(map_business_hour)
lastweek_cut["BusinessHour"] = lastweek_cut["Hour"].apply(map_business_hour)

# ---------------- KPI FUNCTION ---------------- #

def build_kpi(df_today, df_lw, label_name=None):

    gross_today = df_today["grossAmount"].sum()
    discount_today = df_today["discountAmount"].sum()
    net_today = df_today["Net Sales"].sum()
    txn_today = len(df_today)

    gross_lw = df_lw["grossAmount"].sum()
    discount_lw = df_lw["discountAmount"].sum()
    net_lw = df_lw["Net Sales"].sum()
    txn_lw = len(df_lw)

    aov_today = net_today / max(txn_today, 1)
    aov_lw = net_lw / max(txn_lw, 1)

    dis_per_today = (discount_today / max(gross_today, 1)) * 100
    dis_per_lw = (discount_lw / max(gross_lw, 1)) * 100

    df = pd.DataFrame({
        "Parameters": ["Gross Amount", "Discount", "Net Amount", "Transaction", "AOV", "Discount %"],
        "Today": [gross_today, discount_today, net_today, txn_today, aov_today, dis_per_today],
        "Last Week": [gross_lw, discount_lw, net_lw, txn_lw, aov_lw, dis_per_lw]
    })

    df["Growth %"] = ((df["Today"] - df["Last Week"]) / df["Last Week"].replace(0, 1)) * 100
    df = df.round(2)

    if label_name:
        df.insert(0, label_name[0], label_name[1])

    return df

# ---------------- ANALYSIS ---------------- #

overall = build_kpi(today_cut, lastweek_cut)

source_analysis = pd.concat([
    build_kpi(today_cut[today_cut["Source Group"]==s],
              lastweek_cut[lastweek_cut["Source Group"]==s],
              ("Source", s))
    for s in today_cut["Source Group"].dropna().unique()
], ignore_index=True)

region_analysis = pd.concat([
    build_kpi(today_cut[today_cut["Region"]==r],
              lastweek_cut[lastweek_cut["Region"]==r],
              ("Region", r))
    for r in today_cut["Region"].dropna().unique()
], ignore_index=True)

brand_analysis = pd.concat([
    build_kpi(today_cut[today_cut["Brand"]==b],
              lastweek_cut[lastweek_cut["Brand"]==b],
              ("Brand", b))
    for b in today_cut["Brand"].dropna().unique()
], ignore_index=True)

session_analysis = pd.concat([
    build_kpi(today_cut[today_cut["Session"]==b],
              lastweek_cut[lastweek_cut["Session"]==b],
              ("Session", b))
    for b in today_cut["Session"].dropna().unique()
], ignore_index=True)

# ---------------- HOURLY TREND ---------------- #

hourly_today = today_cut.groupby("BusinessHour").agg(
    Today_Sales=("Net Sales", "sum")
)

hourly_lw = lastweek_cut.groupby("BusinessHour").agg(
    LW_Sales=("Net Sales", "sum")
)

hourly_analysis = hourly_today.join(hourly_lw, how="outer").fillna(0)

# Growth
hourly_analysis["Growth %"] = (
    (hourly_analysis["Today_Sales"] - hourly_analysis["LW_Sales"])
    / hourly_analysis["LW_Sales"].replace(0, 1)
) * 100

hourly_analysis = hourly_analysis.reset_index()

# 🔥 Convert back to normal hour format
hourly_analysis["Hour"] = hourly_analysis["BusinessHour"].apply(lambda x: x if x < 24 else x - 24)

# Sort correctly
hourly_analysis = hourly_analysis.sort_values("BusinessHour")

# Format display
hourly_analysis["Hour"] = hourly_analysis["Hour"].apply(lambda x: f"{int(x):02d}:00")

# Round
hourly_analysis["Today_Sales"] = hourly_analysis["Today_Sales"].round(2)
hourly_analysis["LW_Sales"] = hourly_analysis["LW_Sales"].round(2)
hourly_analysis["Growth %"] = hourly_analysis["Growth %"].round(2)

hourly_analysis = hourly_analysis.drop(columns=["BusinessHour"])

print("✅ Hourly Trend Fixed (Business Hours)")

# ---------------- PUSH ---------------- #

def push(sheet_name, df):

    try:
        ws = spreadsheet.worksheet(sheet_name)
    except:
        ws = spreadsheet.add_worksheet(title=sheet_name, rows="1000", cols="50")

    df = df.fillna("").astype(str)
    data = [df.columns.tolist()] + df.values.tolist()

    ws.clear()
    ws.update(data, value_input_option="USER_ENTERED")

    print(f"✅ {sheet_name} updated")

# ---------------- EMAIL ---------------- #

def styled_html(df):

    df = df.copy()
    growth_cols = [c for c in df.columns if "Growth" in c]

    for col in df.columns:

        # ✅ Skip text columns (THIS FIXES YOUR ISSUE)
        if col in ["Parameters", "Source", "Region", "Brand"]:
            continue

        # 🎯 Growth formatting with color
        if col in growth_cols:
            df[col] = df[col].apply(lambda x:
                f'<span style="background:#d4edda;padding:4px;">{float(x):.2f}%</span>'
                if pd.notnull(x) and float(x) >= 0 else
                f'<span style="background:#f8d7da;padding:4px;">{float(x):.2f}%</span>'
                if pd.notnull(x) else ""
            )

        # 🎯 Other numeric columns
        else:
            df[col] = pd.to_numeric(df[col], errors="coerce")
            df[col] = df[col].apply(lambda x: f"{x:.2f}" if pd.notnull(x) else "")

    return df.to_html(index=False, escape=False)

# ---------------- EMAIL ---------------- #

def send_email():
    import smtplib
    from email.mime.multipart import MIMEMultipart
    from email.mime.text import MIMEText

    msg = MIMEMultipart()
    msg["From"] = os.environ["EMAIL_USER"]
    msg["To"] = os.environ["EMAIL_TO"]
    msg["Subject"] = "Sales Report"

    body = f"""
    <h2>Overall</h2>{styled_html(overall)}
    <h2>Source</h2>{styled_html(source_analysis)}
    <h2>Region</h2>{styled_html(region_analysis)}
    <h2>Brand</h2>{styled_html(brand_analysis)}
    <h2>Session</h2>{styled_html(session_analysis)}
    <h2>Hourly Trend</h2>{styled_html(hourly_analysis)}
    """

    msg.attach(MIMEText(body, "html"))

    server = smtplib.SMTP("smtp.gmail.com", 587)
    server.starttls()
    server.login(os.environ["EMAIL_USER"], os.environ["EMAIL_PASS"])
    server.sendmail(msg["From"], msg["To"].split(","), msg.as_string())
    server.quit()

    print("📩 Email Sent")

# ---------------- EXECUTE ---------------- #

push("Raw Data", final_df)
push("Overall", overall)
push("Source", source_analysis)
push("Region", region_analysis)
push("Brand", brand_analysis)
push("session", session_analysis)
push("Hourly Trend", hourly_analysis)

send_email()

print("🎉 SUCCESS")
