# =========================================================
# 🔥 IMPORTS
# =========================================================
import os, json, time, jwt, requests
import pandas as pd
from datetime import datetime, timedelta
import gspread
from google.oauth2.service_account import Credentials
import smtplib
from email.mime.text import MIMEText
import numpy as np

print("🚀 Cancellation Script Started")

# =========================================================
# 🔐 AUTH
# =========================================================
API_KEY = os.environ["API_KEY"]
SECRET_KEY = os.environ["SECRET_KEY"]

def get_token():
    payload = {
        "iss": API_KEY,
        "iat": int(time.time())
    }
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

raw_ws = spreadsheet.worksheet("Cancellation_Tracker")
mapping_ws = spreadsheet.worksheet("Store_Mapping")

print("✅ Google Connected")

# =========================================================
# 📅 DATE
# =========================================================
now = datetime.utcnow() + timedelta(hours=5, minutes=30)

def get_business_day(now):
    return (now - timedelta(days=1)).date() if now.hour < 6 else now.date()

today = get_business_day(now).strftime("%Y-%m-%d")
print("📅 Business Day:", today)

# =========================================================
# 📡 FETCH BRANCHES
# =========================================================
b_resp = requests.get(
    "https://api.ristaapps.com/v1/branch/list",
    headers=headers()
)

data = b_resp.json()

if isinstance(data, dict):
    branch_data = data.get("data", [])
else:
    branch_data = data

branches = [
    b.get("branchCode")
    for b in branch_data
    if b.get("status") == "Active"
]

print("🏪 Branch count:", len(branches))

# =========================================================
# 📡 FETCH ORDERS
# =========================================================
all_data = []

for branch in branches:
    try:
        params = {
            "branch": branch,
            "businessday": today
        }

        r = requests.get(
            "https://api.ristaapps.com/v1/sales",
            headers=headers(),
            params=params,
            timeout=20
        )
        if r.status_code != 200:
           print(f"❌ API Error {branch}: {r.status_code}")
           print(r.text)
            continue

        js = r.json()
        data = js.get("data", []) if isinstance(js, dict) else js

        if data:
            all_data.append(pd.json_normalize(data))

    except Exception as e:
        print(f"❌ Error for branch {branch}: {e}")

# Combine
if not all_data:
    print("❌ No data fetched")
    exit()

df = pd.concat(all_data, ignore_index=True)
print("✅ Data fetched:", len(df))

# =========================================================
# 🔁 STANDARDIZE COLUMN
# =========================================================
df.rename(columns={
    "invoiceNo": "invoiceNumber",
    "orderId": "invoiceNumber"
}, inplace=True)

df["invoiceNumber"] = df["invoiceNumber"].astype(str)

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
try:
    existing = pd.DataFrame(raw_ws.get_all_records())
except:
    existing = pd.DataFrame()

if not existing.empty and "invoiceNumber" in existing.columns:
    sent_ids = set(existing["invoiceNumber"].astype(str))
    cancel_df = cancel_df[~cancel_df["invoiceNumber"].isin(sent_ids)]

if cancel_df.empty:
    print("✅ No new cancellations")
    exit()

# =========================================================
# 🧩 STORE MAPPING
# =========================================================
data = mapping_ws.get_all_values()
headers_map = [h.strip() for h in data[0]]

mapping_df = pd.DataFrame(data[1:], columns=headers_map)

final_df = cancel_df.merge(
    mapping_df,
    left_on="branchName",
    right_on="Store Name",
    how="left"
)

# Safe channel
if "channel" not in final_df.columns:
    final_df["channel"] = "Unknown"

print("✅ Mapping Ready:", final_df.shape)

# =========================================================
# 📧 EMAIL FUNCTION
# =========================================================
def send_email(to_email, store_df):

    EMAIL_USER = os.environ.get("EMAIL_USER")
    EMAIL_PASS = os.environ.get("EMAIL_PASS")
    CC_EMAIL = os.environ.get("EMAIL_CC")

    rows_html = ""

    for _, row in store_df.iterrows():
        rows_html += f"""
        <tr>
            <td>{row.get('invoiceNumber','')}</td>
            <td>{row.get('branchName','')}</td>
            <td>{row.get('channel','Unknown')}</td>
            <td>{row.get('createdDate') or row.get('invoiceDate') or ''}</td>
            <td>{row.get('netAmount') or row.get('Net Sales') or ''}</td>
        </tr>
        """

    body = f"""
    <h2>🚨 Cancellation Alert</h2>
    <p><b>Store:</b> {store_df['branchName'].iloc[0]}</p>

    <table border="1" cellpadding="5" cellspacing="0">
        <tr>
            <th>Order ID</th>
            <th>Store</th>
            <th>Channel</th>
            <th>Time</th>
            <th>Amount</th>
        </tr>
        {rows_html}
    </table>
    """

    msg = MIMEText(body, "html")
    msg["Subject"] = f"🚨 Cancellation Alert - {store_df['branchName'].iloc[0]}"
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

        print(f"📩 Mail sent → {to_email}")

    except Exception as e:
        print(f"❌ Email error: {e}")

# =========================================================
# 🚀 SEND ALERTS
# =========================================================
for store, group in final_df.groupby("branchName"):

    email = str(group["Email"].iloc[0]).strip() if "Email" in group.columns else ""

    if not email or email.lower() == "nan":
        print(f"⚠️ No email mapped for store: {store}")
        continue

    send_email(email, group)

# =========================================================
# 📊 SAVE TO SHEET
# =========================================================
final_df["createdAt"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
final_df["emailSent"] = "YES"
final_df["status_flag"] = "SENT"

clean_df = final_df.replace([np.inf, -np.inf], 0).fillna("").astype(str)

raw_ws.append_rows(clean_df.values.tolist())

print("✅ Data appended to sheet")
print("🎉 Flow Completed")
