import os
import json
import time
import jwt
import requests
import pandas as pd
from datetime import datetime, timedelta
import gspread
from google.oauth2.service_account import Credentials
import smtplib
from email.mime.text import MIMEText

print("🚀 Cancellation Script Started")

# =========================================================
# 🔐 AUTH (SAME AS LIVE SCRIPT)
# =========================================================

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

# =========================================================
# 🔐 GOOGLE SHEETS
# =========================================================

creds = Credentials.from_service_account_info(
    json.loads(os.environ["GOOGLE_CREDENTIALS"]),
    scopes=[
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive"
    ]
)

client = gspread.authorize(creds)
spreadsheet = client.open("Cancellation Dashboard")

raw_ws = spreadsheet.worksheet("Raw_Data")
mapping_ws = spreadsheet.worksheet("Store_Mapping")

print("✅ Google Connected")

# =========================================================
# 📅 DATE
# =========================================================

now = datetime.utcnow() + timedelta(hours=5, minutes=30)

def get_business_day(now):
    if now.hour < 6:
        return (now - timedelta(days=1)).date()
    return now.date()

business_day = get_business_day(now)
today = business_day.strftime("%Y-%m-%d")

print("📅 Business Day:", today)

# =========================================================
# 📡 FETCH DATA (BRANCH LEVEL - CORRECT WAY)
# =========================================================

# 1. Get branches
b_resp = requests.get("https://api.ristaapps.com/v1/branch/list", headers=headers())
branches_data = b_resp.json()
branches_data = branches_data.get("data", []) if isinstance(branches_data, dict) else branches_data

branches = [b["branchCode"] for b in branches_data if b.get("status") == "Active"]

print("🏪 Branch count:", len(branches))


# 2. Fetch sales per branch
all_data = []

for branch in branches:

    params = {
        "branch": branch,
        "day": today
    }

    try:
        r = requests.get(
            "https://api.ristaapps.com/v1/sales/summary",
            headers=headers(),
            params=params,
            timeout=20
        )

        if r.status_code != 200:
            print(f"❌ Error for branch {branch}")
            continue

        js = r.json()
        data = js.get("data", [])

        if data:
            all_data.append(pd.json_normalize(data))

    except Exception as e:
        print(f"❌ Exception for branch {branch}: {e}")


# 3. Combine
df = pd.concat(all_data, ignore_index=True) if all_data else pd.DataFrame()

if df.empty:
    print("❌ No data fetched")
    exit()

print("✅ Data fetched:", len(df))

# =========================================================
# 🔻 FILTER CANCELLED
# =========================================================

cancel_df = df[df["status"].str.lower().isin(["cancelled", "voided"])].copy()

if cancel_df.empty:
    print("✅ No cancellations")
    exit()

print("🚨 Cancellations Found:", len(cancel_df))

# =========================================================
# 🔁 REMOVE DUPLICATES
# =========================================================

existing = pd.DataFrame(raw_ws.get_all_records())

if not existing.empty and "orderId" in existing.columns:
    cancel_df = cancel_df[~cancel_df["orderId"].isin(existing["orderId"])]

if cancel_df.empty:
    print("✅ No new cancellations")
    exit()

# =========================================================
# 🧩 STORE MAPPING
# =========================================================

data = mapping_ws.get_all_values()

headers = data[0]
rows = data[1:]

# Remove empty headers
clean_headers = [h if h != "" else f"col_{i}" for i, h in enumerate(headers)]

mapping_df = pd.DataFrame(mapping_ws.get_all_values()[1:], columns=["Store Name", "Email"])

final_df = cancel_df.merge(
    mapping_df,
    left_on="branchName",
    right_on="Store Name",
    how="left"
)

# =========================================================
# 📧 EMAIL FUNCTION
# =========================================================

def send_email(to_email, store_df):

    EMAIL_USER = os.environ.get("EMAIL_USER")
    EMAIL_PASS = os.environ.get("EMAIL_PASS")
    CC_EMAIL = os.environ.get("EMAIL_CC")

    body = "🚨 Cancellation Alert 🚨\n\n"

    for _, row in store_df.iterrows():
        body += (
            f"Order ID: {row.get('orderId','')}\n"
            f"Store: {row.get('branchName','')}\n"
            f"Time: {row.get('createdDate','')}\n"
            f"Amount: {row.get('netAmount','')}\n"
            "----------------------\n"
        )

    msg = MIMEText(body)
    msg["Subject"] = "🚨 Cancellation Alert"
    msg["From"] = EMAIL_USER
    msg["To"] = to_email

    receivers = [to_email]

    if CC_EMAIL:
        msg["Cc"] = CC_EMAIL
        receivers.append(CC_EMAIL)

    try:
        server = smtplib.SMTP("smtp.gmail.com", 587)
        server.starttls()
        server.login(EMAIL_USER, EMAIL_PASS)
        server.sendmail(EMAIL_USER, receivers, msg.as_string())
        server.quit()

        print(f"📩 Mail sent to {to_email}")

    except Exception as e:
        print(f"❌ Email error: {e}")

# =========================================================
# 🚀 SEND ALERTS STORE-WISE
# =========================================================

for store, group in final_df.groupby("branchName"):

    email = group["Email"].iloc[0] if "Email" in group.columns else None

    if pd.notna(email):
        send_email(email, group)

# =========================================================
# 📊 PUSH TO GOOGLE SHEET
# =========================================================

final_df["Fetched_At"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

raw_ws.append_rows(final_df.astype(str).values.tolist())

print("✅ Data Pushed to Sheet")

# =========================================================
# ✅ DONE
# =========================================================

print("🎉 Cancellation Flow Completed")
