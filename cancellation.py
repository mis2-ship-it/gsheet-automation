import os
import json
import requests
import pandas as pd
from datetime import datetime
from requests_aws4auth import AWS4Auth
import gspread
from google.oauth2.service_account import Credentials
import smtplib
from email.mime.text import MIMEText

# =========================================================
# 🔐 GOOGLE AUTH (FIXED)
# =========================================================

creds_dict = json.loads(os.environ["GOOGLE_CREDS"])
creds = Credentials.from_service_account_info(creds_dict)

client = gspread.authorize(creds)

# =========================================================
# 🔐 RISTA AUTH (AWS SIGNED)
# =========================================================

RISTA_API_KEY = os.environ.get("API_KEY")
SECRET_KEY = os.environ.get("SECRET_KEY")
REGION = "ap-south-1"
SERVICE = "execute-api"

awsauth = AWS4Auth(RISTA_API_KEY, SECRET_KEY, REGION, SERVICE)

RISTA_URL = "https://api.ristaapps.com/v1/orders"

# =========================================================
# 📅 DATE
# =========================================================

today = datetime.now().strftime("%Y-%m-%d")

params = {
    "from_date": today,
    "to_date": today
}

# =========================================================
# 📡 API CALL
# =========================================================

response = requests.get(RISTA_URL, auth=awsauth, params=params)

if response.status_code != 200:
    print(response.text)
    raise Exception("API Error")

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
# 📊 GOOGLE SHEETS
# =========================================================

sheet = client.open("Cancellation Dashboard")
raw_ws = sheet.worksheet("Raw_Data")
mapping_ws = sheet.worksheet("Store_Mapping")

existing_df = pd.DataFrame(raw_ws.get_all_records())

# Remove duplicates
if not existing_df.empty and "orderId" in existing_df.columns:
    cancel_df = cancel_df[~cancel_df["orderId"].isin(existing_df["orderId"])]

if cancel_df.empty:
    print("No new cancellations")
    exit()

# =========================================================
# 🧩 STORE MAPPING
# =========================================================

mapping_df = pd.DataFrame(mapping_ws.get_all_records())

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
            f"Time: {row.get('orderTime','')}\n"
            f"-----------------------\n"
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

        print(f"Mail sent to {to_email}")

    except Exception as e:
        print(f"Email error: {e}")

# =========================================================
# 🚀 SEND ALERTS
# =========================================================

for store, group in final_df.groupby("branchName"):
    email = group["Team Email"].iloc[0] if "Team Email" in group.columns else None

    if pd.notna(email):
        send_email(email, group)

# =========================================================
# 📊 PUSH TO SHEET
# =========================================================

final_df["Fetched_At"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

raw_ws.append_rows(final_df.values.tolist())

print("✅ Completed")
