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

if not existing.empty and "invoiceNumber" in existing.columns:
    cancel_df = cancel_df[~cancel_df["invoiceNumber"].isin(existing["invoiceNumber"])]

if cancel_df.empty:
    print("✅ No new cancellations")
    exit()

# =========================================================
# 🧩 STORE MAPPING
# =========================================================

data = mapping_ws.get_all_values()

sheet_headers = [h.strip() for h in data[0]]
rows = data[1:]

mapping_df = pd.DataFrame(rows, columns=sheet_headers)

mapping_df = mapping_df.replace("", pd.NA)

print("✅ Mapping Ready:", mapping_df.shape)

final_df = cancel_df.merge(
    mapping_df,
    left_on="branchName",
    right_on="Store Name",   # must match EXACT column name
    how="left"
)

existing = pd.DataFrame(raw_ws.get_all_records())

sent_ids = set(existing[existing["emailSent"] == "YES"]["invoiceNumber"])

cancel_df = cancel_df[~cancel_df["invoiceNumber"].isin(sent_ids)]
# =========================================================
# 📧 EMAIL FUNCTION (ROBUST VERSION)
# =========================================================

def send_email(to_email, store_df):

    EMAIL_USER = os.environ.get("EMAIL_USER")
    EMAIL_PASS = os.environ.get("EMAIL_PASS")
    CC_EMAIL = os.environ.get("EMAIL_CC")

    if not EMAIL_USER or not EMAIL_PASS:
        print("❌ Missing EMAIL_USER or EMAIL_PASS")
        return

    store_name = store_df["branchName"].iloc[0] if "branchName" in store_df.columns else "Store"

    # =====================================================
    # 🧾 BUILD HTML TABLE
    # =====================================================

    rows_html = ""

    for _, row in store_df.iterrows():

    store_name = row.get("branchName", "")
    invoice_id = row.get("invoiceNumber") or row.get("orderId", "")
    channel = row.get("channel", "")
    item_name = row.get("itemName", "")
    qty = row.get("quantity", "")
    net_amount = row.get("netAmount", "")

        try:
            order_time = pd.to_datetime(order_time).strftime("%d-%b %I:%M %p")
        except:
            pass

        rows_html += f"""
     <tr>
        <td>{store_name}</td>
        <td>{invoice_id}</td>
        <td>{channel}</td>
        <td>{item_name}</td>
        <td>{qty}</td>
        <td>{net_amount}</td>
    </tr>
        """

    body = f"""
    <h2>🚨 Cancellation Alert</h2>
    <p><b>Store:</b> {store_name}</p>

    <table border="1" cellpadding="5" cellspacing="0">
 <tr>
    <th>Store Name</th>
    <th>Invoice Number</th>
    <th>Channel</th>
    <th>Item Name</th>
    <th>Quantity</th>
    <th>Net Amount</th>
</tr>
        {rows_html}
    </table>
    """

    msg = MIMEText(body, "html")
    msg["Subject"] = f"🚨 Cancellation Alert - {store_name}"
    msg["From"] = EMAIL_USER
    msg["To"] = to_email

    receivers = []

    if to_email:
        receivers += [e.strip() for e in to_email.split(",") if e.strip()]

    if CC_EMAIL:
        msg["Cc"] = CC_EMAIL
        receivers += [e.strip() for e in CC_EMAIL.split(",") if e.strip()]

    try:
        server = smtplib.SMTP("smtp.gmail.com", 587)
        server.starttls()
        server.login(EMAIL_USER, EMAIL_PASS)
        server.sendmail(EMAIL_USER, receivers, msg.as_string())
        server.quit()

        print(f"📩 Mail sent for {store_name} → {receivers}")

    except Exception as e:
        print(f"❌ Email error for {store_name}: {e}")

row_index = raw_ws.find(invoice_id).row

raw_ws.update_cell(row_index, EMAIL_SENT_COL, "YES")
raw_ws.update_cell(row_index, STATUS_COL, "SENT")
# =========================================================
# 🚀 SEND ALERTS STORE-WISE (SAFE VERSION)
# =========================================================

for store, group in final_df.groupby("branchName"):

    if "Email" not in group.columns:
        print(f"⚠️ Email column missing for {store}")
        continue

    email = str(group["Email"].iloc[0]).strip()

    # ❌ No email mapped
    if not email or email.lower() in ["nan", "none"]:
        print(f"⚠️ No email mapped for store: {store}")
        continue

    # ✅ Handle multiple emails (comma separated)
    email_list = [e.strip() for e in email.split(",") if e.strip()]

    try:
        send_email(",".join(email_list), group)
        print(f"📩 Alert sent for {store} → {email_list}")

    except Exception as e:
        print(f"❌ Failed for {store}: {e}")
# =========================================================
# 📊 REFRESH GOOGLE SHEET WITH HEADERS (FINAL)
# =========================================================

import numpy as np

final_df["Fetched_At"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

# 🧹 Clean data
clean_df = final_df.copy()
clean_df = clean_df.replace([np.inf, -np.inf], 0)
clean_df = clean_df.fillna("")
clean_df = clean_df.astype(str)

# =========================================================
# 🔄 FULL REFRESH (IMPORTANT)
# =========================================================

# 1. Clear sheet completely
raw_ws.clear()

# 2. Push header
raw_ws.append_row(clean_df.columns.tolist())

# 3. Push data
raw_ws.append_rows(clean_df.values.tolist())

print("✅ Sheet refreshed with latest data")

# =========================================================
# ✅ DONE
# =========================================================

print("🎉 Cancellation Flow Completed")
