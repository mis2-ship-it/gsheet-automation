import os
import json
import time
import jwt
import requests
import pandas as pd
from datetime import datetime, timedelta, timezone
import gspread
from google.oauth2.service_account import Credentials

# 📩 EMAIL IMPORTS
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

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

creds_dict = json.loads(os.environ["GOOGLE_CREDENTIALS"])

creds = Credentials.from_service_account_info(
    creds_dict,
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

IST = timezone(timedelta(hours=5, minutes=30))
now_ist = datetime.now(IST)

today = now_ist.strftime("%Y-%m-%d")
last_week = (now_ist - timedelta(days=7)).strftime("%Y-%m-%d")

print("🕒 IST Time:", now_ist)

# ---------------- FETCH BRANCH ---------------- #

b_resp = requests.get("https://api.ristaapps.com/v1/branch/list", headers=headers())
data = b_resp.json()

if isinstance(data, dict):
    data = data.get("data", [])

branches = [
    b["branchCode"] for b in data
    if isinstance(b, dict) and b.get("status") == "Active"
]

print("🏪 Branch count:", len(branches))

# ---------------- FETCH SALES ---------------- #

def fetch_sales(day):
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
                params=params
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

    return pd.concat(all_data, ignore_index=True)

# ---------------- RUN ---------------- #

today_df = fetch_sales(today)
lastweek_df = fetch_sales(last_week)

if today_df.empty:
    print("❌ No today data")
    exit()

# ---------------- TIME CLEAN ---------------- #

today_df["invoiceDate"] = pd.to_datetime(today_df["invoiceDate"], errors="coerce")
lastweek_df["invoiceDate"] = pd.to_datetime(lastweek_df["invoiceDate"], errors="coerce")

today_df["invoiceDate"] = today_df["invoiceDate"].dt.tz_localize(None).dt.tz_localize(IST)
lastweek_df["invoiceDate"] = lastweek_df["invoiceDate"].dt.tz_localize(None).dt.tz_localize(IST)

# ---------------- TIME FILTER ---------------- #

start_time = now_ist.replace(hour=8, minute=30, second=0, microsecond=0)

today_cut = today_df[
    (today_df["invoiceDate"] >= start_time) &
    (today_df["invoiceDate"] <= now_ist)
]

lastweek_cut = lastweek_df[
    (lastweek_df["invoiceDate"].dt.time >= start_time.time()) &
    (lastweek_df["invoiceDate"].dt.time <= now_ist.time())
]

# ---------------- MAPPING ---------------- #

help_ws = spreadsheet.worksheet("Help Sheet")

branch_data = help_ws.get("G:M")
branch_master = pd.DataFrame(branch_data[1:], columns=branch_data[0])

branch_master["Store Name"] = branch_master["Store Name"].astype(str).str.strip()

store_map = dict(zip(branch_master["Store Name"], branch_master["Ownership"]))
region_map = dict(zip(branch_master["Store Name"], branch_master["Region"]))

source_data = help_ws.get("D:E")
source_master = pd.DataFrame(source_data[1:], columns=source_data[0])
source_map = dict(zip(source_master["Channel"], source_master["Source"]))

for df in [today_cut, lastweek_cut]:
    df["branchName"] = df["branchName"].astype(str).str.strip()

    df["Store Type"] = df["branchName"].map(store_map).fillna("Unknown")
    df["Region"] = df["branchName"].map(region_map).fillna("Unknown")
    df["Source"] = df["channel"].map(source_map).fillna("Other")

    df["Hour"] = df["invoiceDate"].dt.hour
    df["Date"] = df["invoiceDate"].dt.date

print("✅ Mapping Done")

# ---------------- NET SALES ---------------- #

for df in [today_cut, lastweek_cut]:
    df["netAmount"] = pd.to_numeric(df["netAmount"], errors="coerce").fillna(0)
    df["chargeAmount"] = pd.to_numeric(df["chargeAmount"], errors="coerce").fillna(0)

    df["Net Sales"] = (
        (df["netAmount"] + df["chargeAmount"])
        .where(df["status"] == "Closed", 0)
    )

print("✅ Net Sales Done")

# ---------------- FILTER ---------------- #

today_cut = today_cut[(today_cut["Store Type"] == "COCO") & (today_cut["status"] == "Closed")]
lastweek_cut = lastweek_cut[(lastweek_cut["Store Type"] == "COCO") & (lastweek_cut["status"] == "Closed")]

# ---------------- METRICS ---------------- #

def compute(df):
    gross = df.get("grossAmount", pd.Series()).astype(float).sum()
    disc = abs(df.get("discountAmount", pd.Series()).astype(float).sum())
    net = df["Net Sales"].sum()
    txn = len(df)
    aov = net / txn if txn else 0
    return gross, disc, net, txn, aov

def growth(t, l):
    return ((t - l) / l * 100) if l else 0

# ---------------- OVERALL ---------------- #

t = compute(today_cut)
l = compute(lastweek_cut)

overall = pd.DataFrame({
    "Metric": ["Gross", "Discount", "Net Sales", "Transactions", "AOV"],
    "Last Week": l,
    "Today": t,
    "Growth %": [growth(t[i], l[i]) for i in range(5)]
})

# ---------------- GROUP ANALYSIS ---------------- #

def group_analysis(col):
    rows = []

    for key in set(today_cut[col]).union(lastweek_cut[col]):
        t_df = today_cut[today_cut[col] == key]
        l_df = lastweek_cut[lastweek_cut[col] == key]

        t_val = compute(t_df)
        l_val = compute(l_df)

        rows.append([
            key,
            l_val[2], t_val[2], growth(t_val[2], l_val[2]),
            l_val[3], t_val[3], growth(t_val[3], l_val[3]),
            l_val[4], t_val[4], growth(t_val[4], l_val[4])
        ])

    return pd.DataFrame(rows, columns=[
        col,
        "LW Net", "Today Net", "Growth %",
        "LW Txn", "Today Txn", "Txn Growth %",
        "LW AOV", "Today AOV", "AOV Growth %"
    ])

source_analysis = group_analysis("Source")
region_analysis = group_analysis("Region")
brand_analysis = group_analysis("brandName") if "brandName" in today_cut.columns else pd.DataFrame()

# ---------------- EMAIL FUNCTION ---------------- #

def send_email(overall, source, region, brand):

    print("Email User:", EMAIL_USER)
    print("To:", EMAIL_TO)
    print("CC:", EMAIL_CC)
    
    EMAIL_USER = os.environ["EMAIL_USER"]
    EMAIL_PASS = os.environ["EMAIL_PASS"]

    EMAIL_TO = os.environ["mis2@frozenbottle.in"].split(",")
    EMAIL_CC = os.environ.get("mis2@frozenbottle.in", "").split(",")

    subject = f"COCO Sales Update | Today vs Last Week | {now_ist.strftime('%I %p')}"

    body = f"""
    <h2>Overall</h2>{overall.to_html(index=False)}
    <h2>Source</h2>{source.to_html(index=False)}
    <h2>Region</h2>{region.to_html(index=False)}
    <h2>Brand</h2>{brand.to_html(index=False) if not brand.empty else 'No Data'}
    """

    msg = MIMEMultipart()
    msg["From"] = EMAIL_USER
    msg["To"] = ", ".join(EMAIL_TO)
    msg["Cc"] = ", ".join([e for e in EMAIL_CC if e])
    msg["Subject"] = subject

    msg.attach(MIMEText(body, "html"))

    server = smtplib.SMTP("smtp.gmail.com", 587)
    server.starttls()
    server.login(EMAIL_USER, EMAIL_PASS)

    server.sendmail(
        EMAIL_USER,
        EMAIL_TO + EMAIL_CC,
        msg.as_string()
    )

    server.quit()

    print("📩 Email Sent")

# ---------------- PUSH ---------------- #

def push(name, df):
    try:
        ws = spreadsheet.worksheet(name)
    except:
        ws = spreadsheet.add_worksheet(title=name, rows="2000", cols="40")

    df = df.fillna("").astype(str)
    ws.clear()
    ws.update([df.columns.tolist()] + df.values.tolist())

# ---------------- EXECUTE ---------------- #

push("Analysis - Overall", overall)
push("Analysis - Source", source_analysis)
push("Analysis - Region", region_analysis)

if not brand_analysis.empty:
    push("Analysis - Brand", brand_analysis)

print("📊 Sheets Updated")

# 📧 SEND EMAIL
send_email(overall, source_analysis, region_analysis, brand_analysis)

print("🎉 FINAL SUCCESS")
