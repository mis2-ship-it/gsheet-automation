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
spreadsheet = client.open_by_url("YOUR_GSHEET_URL")

print("✅ Connected to Google Sheet")

# ---------------- TIME ---------------- #

now = datetime.utcnow() + timedelta(hours=5, minutes=30)
today = now.strftime("%Y-%m-%d")
last_week = (now - timedelta(days=7)).strftime("%Y-%m-%d")

print("🕒 IST Time:", now)

# ---------------- FETCH BRANCH ---------------- #

b_resp = requests.get("https://api.ristaapps.com/v1/branch/list", headers=headers())
data = b_resp.json()
data = data.get("data", []) if isinstance(data, dict) else data

branches = [b["branchCode"] for b in data if b.get("status") == "Active"]

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
                params=params,
                timeout=30
            )

            if r.status_code != 200:
                break

            js = r.json()
            data = js.get("data", [])

            if not data:
                break

            all_data.append(pd.json_normalize(data))

            last_key = js.get("lastKey")
            if not last_key:
                break

    return pd.concat(all_data, ignore_index=True) if all_data else pd.DataFrame()

# ---------------- RUN ---------------- #

today_df = fetch_sales(today)
lastweek_df = fetch_sales(last_week)

if today_df.empty:
    print("❌ No today data")
    exit()

# ---------------- BUSINESS DATE ---------------- #

for df in [today_df, lastweek_df]:
    df["invoiceDate"] = pd.to_datetime(df["invoiceDate"], errors="coerce").dt.tz_localize(None)

def get_business_date(dt):
    if pd.isna(dt):
        return pd.NaT
    return (dt - pd.Timedelta(days=1)).date() if dt.hour < 5 else dt.date()

today_df["businessDate"] = today_df["invoiceDate"].apply(get_business_date)
lastweek_df["businessDate"] = lastweek_df["invoiceDate"].apply(get_business_date)

# ---------------- DATE + HOUR ---------------- #

for df in [today_df, lastweek_df]:
    df["Date"] = df["businessDate"]
    df["Hour"] = df["invoiceDate"].dt.hour

# ---------------- MERGE ---------------- #

today_df["Data_Type"] = "Today"
lastweek_df["Data_Type"] = "Last Week"

final_df = pd.concat([today_df, lastweek_df], ignore_index=True)

# ---------------- MAPPING ---------------- #

help_ws = spreadsheet.worksheet("Help Sheet")

branch_master = pd.DataFrame(help_ws.get("G:M")[1:], columns=help_ws.get("G:M")[0])
source_master = pd.DataFrame(help_ws.get("D:F")[1:], columns=help_ws.get("D:F")[0])
source_master.columns = source_master.columns.str.strip()

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

# ---------------- NET SALES ---------------- #

final_df["netAmount"] = pd.to_numeric(final_df["netAmount"], errors="coerce").fillna(0)
final_df["chargeAmount"] = pd.to_numeric(final_df["chargeAmount"], errors="coerce").fillna(0)

final_df["Net Sales"] = (
    (final_df["netAmount"] + final_df["chargeAmount"])
    .where(final_df["status"] == "Closed", 0)
)

# ---------------- FILTER ---------------- #

today_cut = final_df[
    (final_df["Data_Type"] == "Today") &
    (final_df["Store Type"] == "COCO") &
    (final_df["status"] == "Closed")
].copy()

lastweek_cut = final_df[
    (final_df["Data_Type"] == "Last Week") &
    (final_df["Store Type"] == "COCO") &
    (final_df["status"] == "Closed")
].copy()

# ---------------- BUSINESS HOUR ---------------- #

def map_business_hour(h):
    return h if h >= 8 else h + 24

today_cut["BusinessHour"] = today_cut["Hour"].apply(map_business_hour)
lastweek_cut["BusinessHour"] = lastweek_cut["Hour"].apply(map_business_hour)

# ---------------- TIME FILTER ---------------- #

current_hour = now.hour
cutoff_hour = current_hour + 24 if current_hour < 8 else current_hour - 1

today_cut = today_cut[(today_cut["BusinessHour"] >= 8) & (today_cut["BusinessHour"] <= cutoff_hour)]
lastweek_cut = lastweek_cut[(lastweek_cut["BusinessHour"] >= 8) & (lastweek_cut["BusinessHour"] <= cutoff_hour)]

# ---------------- SESSION ---------------- #

def get_session(h):
    if 8 <= h <= 11: return "Breakfast"
    elif 12 <= h <= 15: return "Lunch"
    elif 16 <= h <= 19: return "Snacks"
    elif 20 <= h <= 23: return "Dinner"
    else: return "Post Dinner"

today_cut["Session"] = today_cut["Hour"].apply(get_session)
lastweek_cut["Session"] = lastweek_cut["Hour"].apply(get_session)

# ---------------- KPI FUNCTION ---------------- #

def build_kpi(df_today, df_lw, label=None):

    def calc(df):
        g = df["grossAmount"].sum()
        d = df["discountAmount"].sum()
        n = df["Net Sales"].sum()
        t = len(df)
        return g, d, n, t

    gt, dt, nt, tt = calc(df_today)
    gl, dl, nl, tl = calc(df_lw)

    data = pd.DataFrame({
        "Parameters": ["Gross Amount","Discount","Net Amount","Transaction","AOV","Discount %"],
        "Today": [gt, dt, nt, tt, nt/max(tt,1), dt/max(gt,1)*100],
        "Last Week": [gl, dl, nl, tl, nl/max(tl,1), dl/max(gl,1)*100]
    })

    data["Growth %"] = ((data["Today"] - data["Last Week"]) / data["Last Week"].replace(0,1))*100
    data = data.round(2)

    if label:
        data.insert(0, label[0], label[1])

    return data

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
    build_kpi(today_cut[today_cut["Session"]==s],
              lastweek_cut[lastweek_cut["Session"]==s],
              ("Session", s))
    for s in today_cut["Session"].dropna().unique()
], ignore_index=True)

# ---------------- HOURLY TREND ---------------- #

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

# ---------------- PUSH ---------------- #

def push(name, df):
    try:
        ws = spreadsheet.worksheet(name)
    except:
        ws = spreadsheet.add_worksheet(title=name, rows="1000", cols="50")

    ws.clear()
    ws.update([df.columns.tolist()] + df.astype(str).values.tolist())

# ---------------- EXECUTE ---------------- #

push("Overall", overall)
push("Source", source_analysis)
push("Region", region_analysis)
push("Brand", brand_analysis)
push("Session", session_analysis)
push("Hourly", hourly_analysis)

print("🎉 SUCCESS")
