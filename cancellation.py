import os
import requests
import pandas as pd
from datetime import datetime, timedelta
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import smtplib
from email.mime.text import MIMEText

# =========================================================
# 🔐 CONFIG
# =========================================================

RISTA_API_KEY = os.getenv("RISTA_API_KEY")

RISTA_URL = "https://api.ristaapps.com/v1/orders"  # confirm endpoint

GOOGLE_SHEET_NAME = "Cancellation Dashboard"
RAW_SHEET = "Raw_Data"
MAPPING_SHEET = "Store_Mapping"

EMAIL_SENDER = "EMAIL_USER"
EMAIL_PASSWORD = "EMAIL_PASS"

# =========================================================
# 📅 DATE RANGE (TODAY)
# =========================================================

today = datetime.now().strftime("%Y-%m-%d")

params = {
    "from_date": today,
    "to_date": today
}

headers = {
    "Authorization": f"Bearer {RISTA_API_KEY}",
    "Content-Type": "application/json"
}

# =========================================================
# 📡 FETCH DATA FROM RISTA
# =========================================================

response = requests.get(RISTA_URL, headers=headers, params=params)

if response.status_code != 200:
    raise Exception(f"API Error: {response.text}")

data = response.json()

df = pd.json_normalize(data.get("orders", []))

if df.empty:
    print("No data from API")
    exit()

# =========================================================
# 🔻 FILTER CANCELLED
# =========================================================

cancel_df = df[df["status"].str.lower() == "cancelled"].copy()

if cancel_df.empty:
    print("No cancellations found")
    exit()

# =========================================================
# 🔁 AVOID DUPLICATES (USE ORDER ID)
# =========================================================

# Google Sheets Auth
scope = [
    "https://spreadsheets.google.com/feeds",
    "https://www.googleapis.com/auth/drive"
]

creds = ServiceAccountCredentials.from_json_keyfile_name("creds.json", scope)
client = gspread.authorize(creds)

sheet = client.open(GOOGLE_SHEET_NAME)
raw_ws = sheet.worksheet(RAW_SHEET)

# Existing data
existing_data = raw_ws.get_all_records()
existing_df = pd.DataFrame(existing_data)

if not existing_df.empty and "orderId" in existing_df.columns:
    cancel_df = cancel_df[~cancel_df["orderId"].isin(existing_df["orderId"])]

if cancel_df.empty:
    print("No new cancellations")
    exit()

# =========================================================
# 🧩 STORE MAPPING
# =========================================================

mapping_ws = sheet.worksheet(MAPPING_SHEET)
mapping_df = pd.DataFrame(mapping_ws.get_all_records())

final_df = cancel_df.merge(
    mapping_df,
    left_on="branchName",
    right_on="Store Name",
    how="left"
)
# Auto Alert Email

def send_email(to_email, store_df):

    body = "🚨 Cancellation Alert 🚨\n\n"

    for _, row in store_df.iterrows():
        body += (
            f"Order ID: {row.get('orderId','')}\n"
            f"Store: {row.get('branchName','')}\n"
            f"Time: {row.get('orderTime','')}\n"
            f"Amount: {row.get('Net Sales','')}\n"
            "--------------------------\n"
        )


    EMAIL_USER = os.environ.get("EMAIL_USER")
    EMAIL_PASS = os.environ.get("EMAIL_PASS")
    TO_EMAIL = os.environ.get("EMAIL_TO")
    CC_EMAIL = os.environ.get("EMAIL_CC")

    msg = MIMEText(body)
    msg["Subject"] = "Cancellation Alert"
    msg["From"] = "EMAIL_USER"
    msg["To"] = "EMAIL_TO"

    server = smtplib.SMTP("smtp.gmail.com", 587)
    server.starttls()
    server.login(EMAIL_USER, EMAIL_PASS)
    server.sendmail(EMAIL_USER, receivers, msg.as_string())
    server.quit()
            print(f"Mail sent to {to_email}")
    except Exception as e:
        print(f"Email error: {e}")

# =========================================================
# 🚀 SEND ALERTS STORE-WISE
# =========================================================

for store, group in final_df.groupby("branchName"):

    email = group["Team Email"].iloc[0] if "Team Email" in group.columns else None

    if pd.notna(email):
        send_email(email, group)

# =========================================================
# 📊 PUSH TO GOOGLE SHEET
# =========================================================

# Prepare data
push_df = final_df.copy()

# Ensure consistent columns
push_df["Fetched_At"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

# Append to sheet
rows = push_df.values.tolist()

if raw_ws.row_count == 0:
    raw_ws.append_row(push_df.columns.tolist())

raw_ws.append_rows(rows)

print("Data pushed to Google Sheets")

# =========================================================
# ✅ DONE
# =========================================================

print("✅ Cancellation Alert Flow Completed")
