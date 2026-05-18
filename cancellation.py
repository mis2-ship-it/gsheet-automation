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
# 📡 FETCH SALES DATA (LATEST 5 MINUTES)
# =========================================================
df_list = []

for branch in branches:

    try:

        # =====================================================
        # ⏰ LAST 5 MINUTES WINDOW
        # =====================================================
        to_time = datetime.utcnow()

        from_time = to_time - timedelta(minutes=5)

        from_time_str = from_time.strftime("%Y-%m-%d %H:%M:%S")
        to_time_str = to_time.strftime("%Y-%m-%d %H:%M:%S")

        print(f"🔍 Fetching {branch} | {from_time_str} → {to_time_str}")

        # =====================================================
        # 📡 API CALL
        # =====================================================
        r = requests.get(
        "https://api.ristaapps.com/v1/sales/page",
        headers=headers(),
        params={
            "branch": branch,
            "day": today,
            "fromDate": from_time_str,
            "toDate": to_time_str,
            "page": 1,
            "pageSize": 500,
            "sort": "desc"
        },
        timeout=30
    )

        # =====================================================
        # ❌ STATUS CHECK
        # =====================================================
        if r.status_code != 200:

            print(f"❌ API Error {branch}: {r.status_code}")
            print(r.text)

            continue

        # =====================================================
        # 📦 RESPONSE
        # =====================================================
        resp = r.json()

        data = []

        # CASE 1 → DICT RESPONSE
        if isinstance(resp, dict):

            # /sales/page format
            if "data" in resp:

                if isinstance(resp["data"], dict):

                    data = resp["data"].get("rows", [])

                elif isinstance(resp["data"], list):

                    data = resp["data"]

        # CASE 2 → LIST RESPONSE
        elif isinstance(resp, list):

            data = resp

        # =====================================================
        # 📊 DATAFRAME
        # =====================================================
        if data:

            temp_df = pd.json_normalize(data)

            # Add branchName if missing
            if "branchName" not in temp_df.columns:

                temp_df["branchName"] = branch

            df_list.append(temp_df)

            print(f"✅ {branch} → {len(temp_df)} rows")

        else:

            print(f"⚠️ No data → {branch}")

    except Exception as e:

        print(f"❌ Error {branch}: {e}")

# =========================================================
# 🔗 COMBINE ALL DATA
# =========================================================
if not df_list:

    print("❌ No data fetched")
    exit()

df = pd.concat(df_list, ignore_index=True)

print("✅ Total rows fetched:", len(df))

# =========================================================
# 🔻 FILTER CANCELLED ONLY
# =========================================================
status_col = None

possible_status_cols = [
    "status",
    "orderStatus",
    "invoiceStatus"
]

for col in possible_status_cols:

    if col in df.columns:

        status_col = col
        break

if not status_col:

    print("❌ Status column not found")
    print(df.columns.tolist())
    exit()

cancel_df = df[
    df[status_col]
    .astype(str)
    .str.lower()
    .isin(["voided", "cancel"])
].copy()

print("🚨 Cancellation Found:", len(cancel_df))

# =========================================================
# 🧾 CANCEL REASON EXTRACTION
# =========================================================

if "statusInfo.reason" in cancel_df.columns:

    cancel_df["cancelReason"] = (
        cancel_df["statusInfo.reason"]
        .fillna("")
        .astype(str)
        .str.strip()
    )

else:
    cancel_df["cancelReason"] = ""

# Debug check
print(cancel_df[
    ["invoiceNumber", "cancelReason"]
].head())

# =========================================================
# 🔁 STANDARDIZE INVOICE NUMBER
# =========================================================
invoice_col = None

possible_invoice_cols = [
    "invoiceNumber",
    "invoiceNo",
    "orderId",
    "billNo"
]

for col in possible_invoice_cols:

    if col in cancel_df.columns:

        invoice_col = col
        break

if not invoice_col:

    print("❌ Invoice column not found")
    print(cancel_df.columns.tolist())
    exit()

cancel_df["invoiceNumber"] = (
    cancel_df[invoice_col]
    .astype(str)
    .str.strip()
)

# =========================================================
# 🚫 REMOVE DUPLICATE ALERTS
# =========================================================
try:

    existing = pd.DataFrame(raw_ws.get_all_records())

except:

    existing = pd.DataFrame()

if (
    not existing.empty
    and "invoiceNumber" in existing.columns
):

    existing["invoiceNumber"] = (
        existing["invoiceNumber"]
        .astype(str)
        .str.strip()
    )

    sent_ids = set(existing["invoiceNumber"])

    before_count = len(cancel_df)

    cancel_df = cancel_df[
        ~cancel_df["invoiceNumber"].isin(sent_ids)
    ]

    after_count = len(cancel_df)

    print(f"🚫 Duplicate removed: {before_count - after_count}")

# =========================================================
# ✅ FINAL CHECK
# =========================================================
if cancel_df.empty:

    print("✅ No new cancellations")
    exit()

print("✅ New cancellations:", len(cancel_df))

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
    "statusInfo.reason"
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


