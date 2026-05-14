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
# 📡 FETCH SALES DATA
# =========================================================
df_list = []

for branch in branches:
    try:

        from_time = (
        datetime.utcnow() - timedelta(minutes=10)
        ).strftime("%Y-%m-%d %H:%M:%S")

        params = {
           "branch": branch,
           "day": today,
           "fromDate": from_time
        }

        r = requests.get(
        "https://api.ristaapps.com/v1/sales/page",
        headers=headers(),
        params={
            "branch": branch,
            "day": today,
            "page": 1,
            "pageSize": 500
            "sort": "desc"
        },
        timeout=30
    )

        if r.status_code != 200:
            print(f"❌ API Error {branch}: {r.status_code}")
            print(r.text)
            continue

        resp = r.json()

        if isinstance(resp, dict):
            data = resp.get("data", {}).get("rows", [])
        else:
            data = []

        if data:
            df_list.append(pd.json_normalize(data))

    except Exception as e:
        print(f"❌ Error {branch}: {e}")

# Combine all
if not df_list:
    print("❌ No data fetched")
    exit()

df = pd.concat(df_list, ignore_index=True)

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
cancel_df = df[
    df["status"].astype(str).str.lower().isin(["voided"])
].copy()

if cancel_df.empty:
    print("✅ No cancellations")
    exit()

print("🚨 Cancellations Found:", len(cancel_df))

# =========================================================
# 🔁 REMOVE DUPLICATE ALERTS
# =========================================================

try:
    existing_data = raw_ws.get_all_values()

    if len(existing_data) > 1:

        existing_headers = existing_data[0]

        existing_df = pd.DataFrame(
            existing_data[1:],
            columns=existing_headers
        )

    else:
        existing_df = pd.DataFrame()

except Exception as e:
    print("⚠️ Existing sheet read error:", e)
    existing_df = pd.DataFrame()

# =========================================================
# 📌 EXISTING SENT IDS
# =========================================================

sent_ids = set()

if (
    not existing_df.empty and
    "invoiceNumber" in existing_df.columns
):

    sent_ids = set(
        existing_df["invoiceNumber"]
        .astype(str)
        .str.strip()
        .unique()
    )

print(f"📌 Existing Sent IDs: {len(sent_ids)}")

# =========================================================
# 🚫 REMOVE ALREADY SENT ORDERS
# =========================================================

cancel_df["invoiceNumber"] = (
    cancel_df["invoiceNumber"]
    .astype(str)
    .str.strip()
)

cancel_df = cancel_df[
    ~cancel_df["invoiceNumber"].isin(sent_ids)
]

print(f"🆕 New Cancellations: {len(cancel_df)}")

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
# 🧾 CANCELLATION REASON
# =========================================================

possible_reason_cols = [
    "cancelReason",
    "cancellationReason",
    "voidReason",
    "reason"
]

reason_col = None

for col in possible_reason_cols:
    if col in final_df.columns:
        reason_col = col
        break

if reason_col:
    final_df["Cancel_Reason"] = final_df[reason_col].fillna("Unknown")
else:
    final_df["Cancel_Reason"] = "Unknown"

# =========================================================
# 📧 EMAIL FUNCTION
# =========================================================
def send_email(to_email, store_df):

    EMAIL_USER = os.environ.get("EMAIL_USER")
    EMAIL_PASS = os.environ.get("EMAIL_PASS")
 

    rows_html = ""

    for _, row in store_df.iterrows():
        rows_html += f"""
        <tr>
            <td>{row.get('invoiceNumber','')}</td>
            <td>{row.get('branchName','')}</td>
            <td>{row.get('channel','Unknown')}</td>
            <td>{row.get('createdDate') or row.get('invoiceDate') or ''}</td>
            <td>{row.get('netAmount') or row.get('Net Sales') or ''}</td>
            <td>{row.get('Cancel_Reason','Unknown')}</td>
        </tr>
        """

    body = f"""
    <h2>🚨 Cancellation Alert</h2>
    <p><b>Store:</b> {store_df['branchName'].iloc[0]}</p>
    
    <p style="color:red; font-weight:bold;">
    ⚠️ Please check and update the reason for cancellation immediately.
    </p>
    <table border="1" cellpadding="5" cellspacing="0">
        <tr>
            <th>Order ID</th>
            <th>Store</th>
            <th>Channel</th>
            <th>Time</th>
            <th>Amount</th>
            <th>Reason</th>
        </tr>
        {rows_html}
    </table>
    """

    msg = MIMEText(body, "html")
    msg["Subject"] = f"🚨 Cancellation Alert - {store_df['branchName'].iloc[0]}"
    msg["From"] = EMAIL_USER
    to_list = [e.strip() for e in to_email.split(",") if e.strip()]
    msg["To"] = ", ".join(to_list)
    receivers = to_list.copy()


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
# 🚀 SEND ALERTS (TEAM + REGION MANAGER)
# =========================================================
for store, group in final_df.groupby("branchName"):

    # Team email
    team_email = str(group["Email"].iloc[0]).strip() if "Email" in group.columns else ""

    # Region Manager email
    rm_email = str(group["Region Manager Email"].iloc[0]).strip() if "Region Manager Email" in group.columns else ""

    receivers = []

    # Add team email
    if team_email and team_email.lower() != "nan":
        receivers.append(team_email)

    # Add RM email
    if rm_email and rm_email.lower() != "nan":
        receivers.append(rm_email)

    # Remove duplicates
    receivers = list(set(receivers))

    if not receivers:
        print(f"⚠️ No email mapped for store: {store}")
        continue

    send_email(",".join(receivers), group)

    print(f"📩 Alert sent for {store} → {receivers}")


# =========================================================
# 📧 SUMMARY EMAIL (ONLY CC)
# =========================================================
def send_summary_email(summary_df):

    EMAIL_USER = os.environ.get("EMAIL_USER")
    EMAIL_PASS = os.environ.get("EMAIL_PASS")
    CC_EMAIL = os.environ.get("EMAIL_CCOPS")

    if not CC_EMAIL:
        print("❌ No CC email configured")
        return

    # Convert dataframe to HTML
    table_html = summary_df.to_html(index=False, border=1)

    body = f"""
    <h2>📊 Store-Level Cancellation Summary</h2>

    <p><b>Channel-wise cancellation count</b></p>

    {table_html}

    <br>
    <p style="color:red;"><b>⚠️ Please review high cancellation stores.</b></p>
    """

    msg = MIMEText(body, "html")
    msg["Subject"] = "📊 Cancellation Summary Report"
    msg["From"] = EMAIL_USER
    msg["To"] = CC_EMAIL   # sending only to CC list

    receivers = [e.strip() for e in CC_EMAIL.split(",") if e.strip()]

    try:
        server = smtplib.SMTP("smtp.gmail.com", 587)
        server.starttls()
        server.login(EMAIL_USER, EMAIL_PASS)
        server.sendmail(EMAIL_USER, receivers, msg.as_string())
        server.quit()

        print(f"📩 Summary mail sent → {receivers}")

    except Exception as e:
        print(f"❌ Summary email error: {e}")

# =========================================================
# 📊 CHANNEL + REASON SUMMARY
# =========================================================

summary_df = (
    final_df
    .groupby(["channel", "Cancel_Reason", "branchName"])
    .size()
    .reset_index(name="Cancel_Count")
    .sort_values(by="Cancel_Count", ascending=False)
)

print(summary_df.head())

# =========================================================
# 📤 SEND SUMMARY MAIL
# =========================================================
send_summary_email(summary_df)

        
# =========================================================
# 📊 SAVE ALERT HISTORY
# =========================================================

final_df["createdAt"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
final_df["emailSent"] = "YES"

# Only tracking columns
tracking_df = final_df[[
    "invoiceNumber",
    "branchName",
    "channel",
    "Cancel_Reason",
    "createdAt"
]].copy()

tracking_df = (
    tracking_df
    .replace([np.inf, -np.inf], 0)
    .fillna("")
    .astype(str)
)

# =========================================================
# 🧾 ADD HEADER IF SHEET EMPTY
# =========================================================

existing_rows = raw_ws.get_all_values()

if len(existing_rows) == 0:
    raw_ws.append_row(tracking_df.columns.tolist())

# =========================================================
# ➕ APPEND DATA
# =========================================================

raw_ws.append_rows(
    tracking_df.values.tolist(),
    value_input_option="USER_ENTERED"
)

print("✅ Tracking data saved")
print("🎉 Flow Completed")


