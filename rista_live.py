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

branches = [
    b["branchCode"] for b in data
    if isinstance(b, dict) and b.get("status") == "Active"
]

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

# ---------------- TIME FILTER ---------------- #

today_df["invoiceDate"] = pd.to_datetime(today_df["invoiceDate"], errors="coerce")
lastweek_df["invoiceDate"] = pd.to_datetime(lastweek_df["invoiceDate"], errors="coerce")

today_df["invoiceDate"] = today_df["invoiceDate"].dt.tz_localize(None)
lastweek_df["invoiceDate"] = lastweek_df["invoiceDate"].dt.tz_localize(None)

now_naive = now.replace(tzinfo=None)
today_df = today_df[today_df["invoiceDate"] <= now_naive]

print("⏱ Time filter applied")

# ---------------- DATE + HOUR ---------------- #

today_df["Date"] = today_df["invoiceDate"].dt.date
lastweek_df["Date"] = lastweek_df["invoiceDate"].dt.date

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

source_data = help_ws.get("D:E")
source_master = pd.DataFrame(source_data[1:], columns=source_data[0])
source_map = dict(zip(source_master["Channel"], source_master["Source"]))

final_df["Store Type"] = final_df["branchName"].map(store_map).fillna("Unknown")
final_df["Region"] = final_df["branchName"].map(region_map).fillna("Unknown")
final_df["Source"] = final_df["channel"].map(source_map).fillna("Other")

# Source grouping
main_sources = ["Instore", "Swiggy", "Zomato", "Ownly"]
final_df["Source Group"] = final_df["Source"].apply(
    lambda x: x if x in main_sources else "Others"
)

print("✅ Mapping Done")

# ---------------- NET SALES ---------------- #

final_df["netAmount"] = pd.to_numeric(final_df["netAmount"], errors="coerce").fillna(0)
final_df["chargeAmount"] = pd.to_numeric(final_df["chargeAmount"], errors="coerce").fillna(0)

final_df["Net Sales"] = (
    (final_df["netAmount"] + final_df["chargeAmount"])
    .where(final_df["status"] == "Closed", 0)
)

print("✅ Net Sales Done")

# ---------------- FILTER COCO ---------------- #

today_cut = final_df[(final_df["Data_Type"]=="Today") & (final_df["Store Type"]=="COCO") & (final_df["status"]=="Closed")]
lastweek_cut = final_df[(final_df["Data_Type"]=="Last Week") & (final_df["Store Type"]=="COCO") & (final_df["status"]=="Closed")]

# ---------------- GROUP FUNCTION ---------------- #

def group_analysis(df_today, df_lw, group_col):

    t = df_today.groupby(group_col).agg(
        Today_Net=("Net Sales","sum"),
        Today_Txn=("invoiceNumber","count"),
        Gross_Today=("grossAmount","sum"),
        Discount_Today=("discountAmount","sum")
    )

    l = df_lw.groupby(group_col).agg(
        LW_Net=("Net Sales","sum"),
        LW_Txn=("invoiceNumber","count"),
        Gross_LW=("grossAmount","sum"),
        Discount_LW=("discountAmount","sum")
    )

    merged = t.join(l, how="outer").fillna(0)

    merged["Growth %"] = ((merged["Today_Net"] - merged["LW_Net"]) / merged["LW_Net"].replace(0, pd.NA)) * 100
    merged["Growth %"] = merged["Growth %"].fillna(0)

    merged["AOV Today"] = merged["Today_Net"] / merged["Today_Txn"].replace(0,1)
    merged["AOV LW"] = merged["LW_Net"] / merged["LW_Txn"].replace(0,1)

    merged["Dis % Today"] = (merged["Discount_Today"] / merged["Gross_Today"].replace(0,1)) * 100
    merged["Dis % LW"] = (merged["Discount_LW"] / merged["Gross_LW"].replace(0,1)) * 100

    return merged.reset_index()

# ---------------- OVERALL ---------------- #

overall = pd.DataFrame({
    "Metric": ["Gross", "Discount", "Net Sales", "Transactions", "AOV", "Dis %"],
    "Last Week": [
        lastweek_cut["grossAmount"].sum(),
        lastweek_cut["discountAmount"].sum(),
        lastweek_cut["Net Sales"].sum(),
        len(lastweek_cut),
        lastweek_cut["Net Sales"].sum()/max(len(lastweek_cut),1),
        (lastweek_cut["discountAmount"].sum()/max(lastweek_cut["grossAmount"].sum(),1))*100
    ],
    "Today": [
        today_cut["grossAmount"].sum(),
        today_cut["discountAmount"].sum(),
        today_cut["Net Sales"].sum(),
        len(today_cut),
        today_cut["Net Sales"].sum()/max(len(today_cut),1),
        (today_cut["discountAmount"].sum()/max(today_cut["grossAmount"].sum(),1))*100
    ]
})

overall["Growth %"] = ((overall["Today"] - overall["Last Week"]) / overall["Last Week"].replace(0,1))*100

# ---------------- OTHER ANALYSIS ---------------- #

source_analysis = group_analysis(today_cut, lastweek_cut, "Source Group")
region_analysis = group_analysis(today_cut, lastweek_cut, "Region")

brand_analysis = group_analysis(today_cut, lastweek_cut, "brandName") if "brandName" in final_df.columns else pd.DataFrame()

# ---------------- ROUND ---------------- #

def format_df(df):
    for col in df.columns:
        df[col] = pd.to_numeric(df[col], errors="coerce")
        if "Growth" in col:
            df[col] = df[col].round(1)
        else:
            df[col] = df[col].round(0)
    return df.fillna(0)

overall = format_df(overall)
source_analysis = format_df(source_analysis)
region_analysis = format_df(region_analysis)
if not brand_analysis.empty:
    brand_analysis = format_df(brand_analysis)

df.replace([float("inf"), float("-inf")], 0, inplace=True)

# ---------------- PUSH (FINAL SAFE VERSION) ---------------- #
def push(sheet_name, df):

    ws = sheet.worksheet(sheet_name)

    df = df.copy()

    # 🔥 STEP 1: Convert EVERYTHING safely (no Timestamp can survive)
    def safe_convert(x):
        try:
            if hasattr(x, "strftime"):
                return x.strftime("%Y-%m-%d %H:%M:%S")
            return x
        except:
            return str(x)

    df = df.applymap(safe_convert)

    # 🔥 STEP 2: Handle NaN / None
    df = df.fillna("")

    # 🔥 STEP 3: FORCE STRING FINAL SANITIZATION
    df = df.astype(str)

    data = [df.columns.astype(str).tolist()] + df.values.tolist()

    ws.clear()

    print(df.dtypes)
    print(df.head(2))
    print(type(df.iloc[0,0]))

    ws.update(data, value_input_option="USER_ENTERED")

    print(f"✅ {sheet_name} updated | Rows: {len(df)}")

# ---------------- EMAIL ---------------- #

def style_growth(val):
    try:
        return "background-color:#d4edda" if float(val)>=0 else "background-color:#f8d7da"
    except:
        return ""

def styled_html(df):
    growth_cols = [c for c in df.columns if "Growth" in c]
    return df.style.applymap(style_growth, subset=growth_cols).format("{:.0f}").to_html()

def send_email():
    import smtplib
    from email.mime.multipart import MIMEMultipart
    from email.mime.text import MIMEText

    EMAIL_USER = os.environ.get("EMAIL_USER")
    EMAIL_PASS = os.environ.get("EMAIL_PASS")
    TO_EMAIL = os.environ.get("EMAIL_TO")
    CC_EMAIL = os.environ.get("EMAIL_CC")

    msg = MIMEMultipart()
    msg["From"] = EMAIL_USER
    msg["To"] = TO_EMAIL
    msg["Cc"] = CC_EMAIL
    msg["Subject"] = "Sales Report"

    body = f"""
    <h2>Overall</h2>{styled_html(overall)}
    <h2>Source</h2>{styled_html(source_analysis)}
    <h2>Region</h2>{styled_html(region_analysis)}
    <h2>Brand</h2>{styled_html(brand_analysis) if not brand_analysis.empty else "No Data"}
    """

    msg.attach(MIMEText(body, "html"))

    receivers = []
    if TO_EMAIL: receivers += TO_EMAIL.split(",")
    if CC_EMAIL: receivers += CC_EMAIL.split(",")

    server = smtplib.SMTP("smtp.gmail.com", 587)
    server.starttls()
    server.login(EMAIL_USER, EMAIL_PASS)
    server.sendmail(EMAIL_USER, receivers, msg.as_string())
    server.quit()

    print("TO:", TO_EMAIL)
    print("CC:", CC_EMAIL)
    print("📩 Email Sent")


send_email()

print("🎉 FINAL SUCCESS")
