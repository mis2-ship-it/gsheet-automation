# =========================================================
# 📱 AI MIS WHATSAPP WEBHOOK
# =========================================================

from flask import Flask, request, jsonify
import os
import requests
import json
from datetime import datetime
from whatsapp_recipients import (
    WHATSAPP_USERS,
    get_user_access,
    user_can_access_store
)

app = Flask(__name__)


# =========================================================
# 🔐 ENVIRONMENT VARIABLES
# =========================================================

VERIFY_TOKEN = os.environ.get(
    "WHATSAPP_VERIFY_TOKEN"
)

PHONE_NUMBER_ID = os.environ.get(
    "WHATSAPP_PHONE_NUMBER_ID"
)

ACCESS_TOKEN = os.environ.get(
    "WHATSAPP_ACCESS_TOKEN"
)

WHATSAPP_DATA_SECRET = os.environ.get(
    "WHATSAPP_DATA_SECRET"
)

GRAPH_API_VERSION = "v23.0"


# =========================================================
# 📊 LIVE SALES SNAPSHOT
# =========================================================

LIVE_SALES_DATA = {}


# =========================================================
# 🚀 STARTUP CHECK
# =========================================================

print("=" * 60)
print("🚀 AI MIS WHATSAPP WEBHOOK")
print("=" * 60)

print(
    "PHONE_NUMBER_ID exists :",
    bool(PHONE_NUMBER_ID)
)

print(
    "ACCESS_TOKEN exists    :",
    bool(ACCESS_TOKEN)
)

print(
    "VERIFY_TOKEN exists    :",
    bool(VERIFY_TOKEN)
)

print(
    "WHATSAPP_DATA_SECRET exists :",
    bool(WHATSAPP_DATA_SECRET)
)

print("=" * 60)

# =========================================================
# 🔐 DEBUG USER ACCESS
# =========================================================

def debug_user_access(sender):

    print("=" * 60)
    print("🔐 WHATSAPP ACCESS DEBUG")
    print("=" * 60)

    user = get_user_access(sender)

    if not user:

        print(
            "❌ User not mapped:",
            sender
        )

        print("=" * 60)

        return None

    print(
        "Sender :",
        sender
    )

    print(
        "Role   :",
        user.get("role")
    )

    print(
        "Region :",
        user.get("region")
    )

    print(
        "Patch  :",
        user.get("patch")
    )

    print(
        "Stores :",
        user.get("stores")
    )

    print("=" * 60)

    return user

# =========================================================
# 🏠 HOME
# =========================================================

@app.route("/", methods=["GET"])
def home():

    return (
        "AI MIS WhatsApp Webhook is running",
        200
    )


# =========================================================
# 🔐 META WEBHOOK VERIFICATION
# =========================================================

@app.route(
    "/webhook",
    methods=["GET"]
)
def verify_webhook():

    mode = request.args.get(
        "hub.mode"
    )

    token = request.args.get(
        "hub.verify_token"
    )

    challenge = request.args.get(
        "hub.challenge"
    )

    print("=" * 60)
    print("🔐 META WEBHOOK VERIFICATION")
    print("Mode     :", mode)
    print(
        "Token OK :",
        token == VERIFY_TOKEN
    )
    print("=" * 60)

    if (
        mode == "subscribe"
        and token == VERIFY_TOKEN
    ):

        print(
            "✅ META WEBHOOK VERIFIED"
        )

        return challenge, 200

    print(
        "❌ WEBHOOK VERIFICATION FAILED"
    )

    return "Forbidden", 403


# =========================================================
# 📤 SEND WHATSAPP TEXT MESSAGE
# =========================================================

def send_whatsapp_message(
    recipient,
    message
):

    print("=" * 60)
    print("📤 SENDING WHATSAPP MESSAGE")
    print("To:", recipient)
    print("Message:")
    print(message)
    print("=" * 60)

    if not PHONE_NUMBER_ID:

        print(
            "❌ WHATSAPP_PHONE_NUMBER_ID missing"
        )

        return False

    if not ACCESS_TOKEN:

        print(
            "❌ WHATSAPP_ACCESS_TOKEN missing"
        )

        return False

    url = (
        f"https://graph.facebook.com/"
        f"{GRAPH_API_VERSION}/"
        f"{PHONE_NUMBER_ID}/messages"
    )

    headers = {
        "Authorization":
            f"Bearer {ACCESS_TOKEN}",

        "Content-Type":
            "application/json"
    }

    payload = {

        "messaging_product":
            "whatsapp",

        "to":
            recipient,

        "type":
            "text",

        "text": {

            "preview_url":
                False,

            "body":
                message
        }
    }

    try:

        response = requests.post(
            url,
            headers=headers,
            json=payload,
            timeout=30
        )

        print(
            "Meta Status   :",
            response.status_code
        )

        print(
            "Meta Response :",
            response.text
        )

        if response.ok:

            print(
                "✅ WhatsApp message sent"
            )

            return True

        print(
            "❌ WhatsApp message failed"
        )

        return False

    except Exception as e:

        print(
            "❌ WhatsApp API error:",
            str(e)
        )

        return False


# =========================================================
# 📥 RECEIVE RISTA LIVE SALES SNAPSHOT
# =========================================================

@app.route(
    "/update-sales-data",
    methods=["POST"]
)
def update_sales_data():

    global LIVE_SALES_DATA

    print("=" * 60)
    print("📥 RISTA LIVE SALES DATA RECEIVED")
    print("=" * 60)

    # -----------------------------------------------------
    # SECURITY CHECK
    # -----------------------------------------------------

    incoming_secret = request.headers.get(
        "X-WhatsApp-Data-Secret"
    )

    print(
        "Data Secret configured:",
        bool(WHATSAPP_DATA_SECRET)
    )

    print(
        "Incoming Secret received:",
        bool(incoming_secret)
    )

    if not WHATSAPP_DATA_SECRET:

        print(
            "❌ WHATSAPP_DATA_SECRET "
            "is not configured"
        )

        return jsonify({

            "success": False,

            "error":
                "Server secret not configured"

        }), 500

    if incoming_secret != WHATSAPP_DATA_SECRET:

        print(
            "❌ Invalid WhatsApp data secret"
        )

        return jsonify({

            "success": False,

            "error":
                "Unauthorized"

        }), 401

    # -----------------------------------------------------
    # READ JSON
    # -----------------------------------------------------

    data = request.get_json(
        silent=True
    )

    if not data:

        print(
            "❌ Empty sales data received"
        )

        return jsonify({

            "success": False,

            "error":
                "Empty JSON payload"

        }), 400

    # -----------------------------------------------------
    # DEBUG
    # -----------------------------------------------------

    print(
        "Received data keys:",
        list(data.keys())
    )

    print(
        "Report Date:",
        data.get("date")
    )

    print(
        "Report Time:",
        data.get("report_time")
    )

    print(
        "Sections:",
        list(data.keys())
    )

    # -----------------------------------------------------
    # STORE SNAPSHOT
    # -----------------------------------------------------

    LIVE_SALES_DATA = data

    print(
        "✅ WhatsApp sales snapshot updated"
    )

    print(
        "LIVE_SALES_DATA keys:",
        list(LIVE_SALES_DATA.keys())
    )

    print("=" * 60)

    return jsonify({

        "success": True,

        "message":
            "Sales data updated successfully"

    }), 200


# =========================================================
# 🔍 CHECK CURRENT SALES SNAPSHOT
# =========================================================

@app.route(
    "/sales-data",
    methods=["GET"]
)
def sales_data():

    print("=" * 60)
    print("🔍 SALES DATA REQUEST")
    print("=" * 60)

    print(
        "LIVE_SALES_DATA empty:",
        not bool(LIVE_SALES_DATA)
    )

    print(
        "LIVE_SALES_DATA keys:",
        list(LIVE_SALES_DATA.keys())
    )

    if not LIVE_SALES_DATA:

        return jsonify({

            "success": False,

            "message":
                "No sales data available"

        }), 404

    return jsonify({

        "success": True,

        "data":
            LIVE_SALES_DATA

    }), 200


# =========================================================
# 📊 FTD SALES DATA
# =========================================================

def get_ftd_sales():

    print("=" * 60)
    print("📊 GET FTD SALES FROM SNAPSHOT")
    print("=" * 60)

    if not LIVE_SALES_DATA:

        print(
            "❌ LIVE_SALES_DATA is empty"
        )

        return None

    overall = LIVE_SALES_DATA.get(
        "overall",
        {}
    )

    result = {

        "date":
            LIVE_SALES_DATA.get(
                "date",
                datetime.now().strftime(
                    "%d-%b-%y"
                )
            ),

        "report_time":
            LIVE_SALES_DATA.get(
                "report_time",
                ""
            ),

        "net":
            float(
                overall.get(
                    "net",
                    0
                )
            ),

        "txn":
            float(
                overall.get(
                    "txn",
                    0
                )
            ),

        "aov":
            float(
                overall.get(
                    "aov",
                    0
                )
            ),

        "discount":
            float(
                overall.get(
                    "discount",
                    0
                )
            ),

        "gross":
            float(
                overall.get(
                    "gross",
                    0
                )
            ),

        "lw_net":
            float(
                overall.get(
                    "lw_net",
                    0
                )
            ),

        "lw_growth":
            float(
                overall.get(
                    "lw_growth",
                    0
                )
            )
    }

    print(
        "Date:",
        result["date"]
    )

    print(
        "Report Time:",
        result["report_time"]
    )

    print(
        "Net:",
        result["net"]
    )

    print(
        "Transactions:",
        result["txn"]
    )

    print(
        "AOV:",
        result["aov"]
    )

    print(
        "Discount:",
        result["discount"]
    )

    print("=" * 60)

    return result

# =========================================================
# 📈 SALES VS LAST WEEK DATA
# =========================================================

def get_sales_vs_lw():

    if not LIVE_SALES_DATA:

        raise Exception(
            "Live sales backend data is not available"
        )

    overall_data = LIVE_SALES_DATA.get(
        "overall",
        {}
    )

    return {
        "today_net": float(
            overall_data.get(
                "net",
                0
            ) or 0
        ),

        "lw_net": float(
            overall_data.get(
                "lw_net",
                0
            ) or 0
        ),

        "growth": float(
            overall_data.get(
                "lw_growth",
                0
            ) or 0
        ),

        "today_txn": float(
            overall_data.get(
                "txn",
                0
            ) or 0
        ),

        "lw_txn": 0.0,

        "today_aov": float(
            overall_data.get(
                "aov",
                0
            ) or 0
        ),

        "lw_aov": 0.0,

        "today_discount": float(
            overall_data.get(
                "discount",
                0
            ) or 0
        ),

        "lw_discount": 0.0
    }


# =========================================================
# 📊 GET BRAND DATA
# =========================================================

def get_brand_data():
    brands = LIVE_SALES_DATA.get("brands", {})
    print("Brands available:", list(brands.keys()))
    return brands


# =========================================================
# 📊 GET SOURCE DATA
# =========================================================

def get_source_data():
    sources = LIVE_SALES_DATA.get("sources", {})
    print("Sources available:", list(sources.keys()))
    return sources


# =========================================================
# 🌍 FIND REGION SNAPSHOT KEY
# =========================================================

def find_region_snapshot_key(requested_region):
    regions = LIVE_SALES_DATA.get("regions", {})
    requested = str(requested_region or "").strip().lower()

    for region_name in regions.keys():
        if str(region_name).strip().lower() == requested:
            return region_name

    if requested in {"kerala", "kerela"}:
        for region_name in regions.keys():
            normalized = str(region_name).strip().lower()
            if normalized in {"kerala", "kerela"}:
                return region_name

    return None


# =========================================================
# 📊 GENERIC PERIOD METRICS
# =========================================================

def _safe_float(value, default=0.0):
    try:
        if value is None or value == "":
            return float(default)
        return float(value)
    except (TypeError, ValueError):
        return float(default)


def _growth(today, previous):
    if previous == 0:
        return 0.0 if today == 0 else 100.0
    return ((today - previous) / previous) * 100


def _period_ref_key(period):
    return {
        "lw": "lw",
        "lm": "lm",
        "ly": "ly",
    }[period]


def get_overall_period_metrics(period):
    """Return overall Today / comparison-period sales and growth.
    Prefers explicit overall fields and falls back to store aggregation.
    """
    overall = LIVE_SALES_DATA.get("overall", {})
    today = _safe_float(overall.get("net"))

    if period == "lw":
        previous = _safe_float(overall.get("lw_net"))
        growth = overall.get("lw_growth")
        if previous == 0:
            previous = sum(
                _safe_float(v.get("lw"))
                for v in LIVE_SALES_DATA.get("stores", {}).values()
            )
    else:
        field = f"{period}_net"
        growth_field = f"{period}_growth"
        previous = _safe_float(overall.get(field))
        growth = overall.get(growth_field)

        if previous == 0:
            previous = sum(
                _safe_float(v.get(period))
                for v in LIVE_SALES_DATA.get("stores", {}).values()
            )

    growth = _safe_float(growth, _growth(today, previous))

    return today, previous, growth


def aggregate_store_periods(store_names=None):
    stores = LIVE_SALES_DATA.get("stores", {})

    if store_names is None:
        selected = stores
    else:
        allowed = {str(x).strip().lower() for x in store_names}
        selected = {
            name: data
            for name, data in stores.items()
            if str(name).strip().lower() in allowed
        }

    totals = {
        "today": 0.0,
        "lw": 0.0,
        "lm": 0.0,
        "ly": 0.0,
    }

    for data in selected.values():
        for key in totals:
            totals[key] += _safe_float(data.get(key))

    return totals, selected


# =========================================================
# 📊 FTD SALES RESPONSE
# =========================================================

def send_ftd_sales(sender):
    sales = get_ftd_sales()

    if not sales:
        send_whatsapp_message(sender, "⚠️ Sales data is not available right now.")
        return

    brands = get_brand_data()

    reply_lines = [
        "📊 *AI MIS | FTD SALES*",
        sales["date"],
        "",
        f"💰 Net Revenue: ₹{sales['net'] / 100000:.2f}L",
        f"🧾 Transactions: {int(round(sales['txn'])):,}",
        f"🧺 AOV: ₹{int(round(sales['aov'])):,}",
        f"📉 Discount: {abs(sales['discount']):.1f}%",
    ]

    if brands:
        values = {
            str(name): _safe_float(data.get("today"))
            for name, data in brands.items()
        }
        total = sum(values.values())

        reply_lines += ["", "🏪 *Brand Contribution*"]
        for name, value in sorted(values.items(), key=lambda x: x[1], reverse=True):
            contribution = (value / max(total, 1.0)) * 100
            reply_lines.append(f"• {name}: {contribution:.0f}%")

    reply = "\n".join(reply_lines)
    print(reply)
    send_whatsapp_message(sender, reply)


# =========================================================
# 📈 PERIOD COMPARISON
# =========================================================

def _send_period_comparison(sender, period):
    user = get_user_access(sender)
    if not user:
        send_whatsapp_message(sender, "❌ Your mobile number is not mapped for AI MIS access.")
        return

    role = str(user.get("role", "")).strip().lower()
    period_labels = {"lw": "Last Week", "lm": "Last Month", "ly": "Last Year"}
    label = period_labels[period]

    if not LIVE_SALES_DATA:
        send_whatsapp_message(sender, "⚠️ Sales data is not available right now.")
        return

    if role == "ops leader":
        today, previous, growth = get_overall_period_metrics(period)
        scope_line = "📍 Overall"

    elif role == "region manager":
        region_name = find_region_snapshot_key(user.get("region", ""))
        region_data = LIVE_SALES_DATA.get("regions", {}).get(region_name, {}) if region_name else {}

        if not region_name:
            send_whatsapp_message(sender, "⚠️ Region data is not available for your mapped region.")
            return

        today = _safe_float(region_data.get("today"))
        previous = _safe_float(region_data.get(period))

        if previous == 0:
            totals, selected = aggregate_store_periods()
            selected = {
                name: data for name, data in selected.items()
                if str(data.get("region", "")).strip().lower() == str(region_name).strip().lower()
            }
            today = sum(_safe_float(v.get("today")) for v in selected.values())
            previous = sum(_safe_float(v.get(period)) for v in selected.values())

        growth = _safe_float(
            region_data.get(f"{period}_growth"),
            _growth(today, previous),
        )
        scope_line = f"📍 Region: {region_name}"

    elif role == "area manager":
        allowed_stores = user.get("stores", [])
        if not isinstance(allowed_stores, list):
            allowed_stores = [allowed_stores]

        if any(str(x).strip().lower() == "all" for x in allowed_stores):
            totals, _ = aggregate_store_periods()
        else:
            totals, _ = aggregate_store_periods(allowed_stores)

        today = totals["today"]
        previous = totals[period]
        growth = _growth(today, previous)
        scope_line = f"📍 Patch: {user.get('patch', '')}"

    else:
        send_whatsapp_message(sender, "❌ Your AI MIS role is not configured.")
        return

    performance = (
        "🚀 Strong Growth" if growth > 5
        else "📈 Growth" if growth > 0
        else "🔻 Decline" if growth < -5
        else "➡️ Stable"
    )

    reply = (
        f"📊 *AI MIS | SALES vs {label.upper()}*\n"
        f"{LIVE_SALES_DATA.get('date', '')}\n"
        f"{LIVE_SALES_DATA.get('report_time', '')}\n\n"
        f"{scope_line}\n\n"
        f"💰 *Today:* ₹{today / 100000:.2f}L\n"
        f"📅 *{label}:* ₹{previous / 100000:.2f}L\n"
        f"📈 *Growth:* {growth:+.1f}%\n"
        f"🧠 *Performance:* {performance}"
    )

    print(reply)
    send_whatsapp_message(sender, reply)


def send_sales_vs_lw(sender):
    _send_period_comparison(sender, "lw")


def send_sales_vs_lm(sender):
    _send_period_comparison(sender, "lm")


def send_sales_vs_ly(sender):
    _send_period_comparison(sender, "ly")


# =========================================================
# 🏪 STORE SALES QUERY
# =========================================================

def send_store_sales_query(sender, store_name):
    user = get_user_access(sender)

    if not user:
        send_whatsapp_message(sender, "❌ Your mobile number is not mapped for AI MIS access.")
        return

    stores = LIVE_SALES_DATA.get("stores", {})
    requested = str(store_name).strip().lower()

    matched_store = next(
        (
            name for name in stores
            if str(name).strip().lower() == requested
        ),
        None,
    )

    if not matched_store:
        send_whatsapp_message(sender, f"❌ Store not found: {store_name}")
        return

    data = stores[matched_store]
    role = str(user.get("role", "")).strip().lower()

    allowed = False

    if role == "ops leader":
        allowed = True

    elif role == "region manager":
        allowed = (
            str(user.get("region", "")).strip().lower()
            ==
            str(data.get("region", "")).strip().lower()
        )

    elif role == "area manager":
        allowed_stores = user.get("stores", [])
        if not isinstance(allowed_stores, list):
            allowed_stores = [allowed_stores]
        allowed_names = {str(x).strip().lower() for x in allowed_stores}
        allowed = "all" in allowed_names or matched_store.strip().lower() in allowed_names

    if not allowed:
        send_whatsapp_message(sender, f"❌ You don't have access to *{matched_store}*.")
        return

    today = _safe_float(data.get("today"))
    lw = _safe_float(data.get("lw"))
    lm = _safe_float(data.get("lm"))
    ly = _safe_float(data.get("ly"))

    growth_lw = _safe_float(data.get("growth"), _growth(today, lw))
    growth_lm = _safe_float(data.get("lm_growth"), _growth(today, lm))
    growth_ly = _safe_float(data.get("ly_growth"), _growth(today, ly))

    reply = (
        "🏪 *AI MIS | STORE SALES*\n"
        f"{LIVE_SALES_DATA.get('date', '')}\n"
        f"{LIVE_SALES_DATA.get('report_time', '')}\n\n"
        f"🏪 *{matched_store}*\n"
        f"📍 Region: {data.get('region', 'UNKNOWN')}\n\n"
        f"💰 Today: ₹{today / 1000:.1f}K\n"
        f"📅 Last Week: ₹{lw / 1000:.1f}K\n"
        f"📆 Last Month: ₹{lm / 1000:.1f}K\n"
        f"📆 Last Year: ₹{ly / 1000:.1f}K\n\n"
        f"📈 vs LW: {growth_lw:+.1f}%\n"
        f"📊 vs LM: {growth_lm:+.1f}%\n"
        f"📊 vs LY: {growth_ly:+.1f}%"
    )

    print(reply)
    send_whatsapp_message(sender, reply)


# =========================================================
# 🌍 REGION SALES QUERY
# =========================================================

def send_region_sales_query(sender, region_name):
    user = get_user_access(sender)

    if not user:
        send_whatsapp_message(sender, "❌ Your mobile number is not mapped for AI MIS access.")
        return

    role = str(user.get("role", "")).strip().lower()
    requested_region = find_region_snapshot_key(region_name)

    if not requested_region:
        send_whatsapp_message(sender, f"❌ Region not found: {region_name}")
        return

    if role == "region manager":
        user_region = find_region_snapshot_key(user.get("region", ""))
        if user_region != requested_region:
            send_whatsapp_message(sender, f"❌ You don't have access to *{requested_region}* region.")
            return
    elif role == "area manager":
        send_whatsapp_message(sender, "❌ Area Managers have store-level access. Ask for a specific store.")
        return
    elif role != "ops leader":
        send_whatsapp_message(sender, "❌ Your AI MIS role is not configured.")
        return

    region_data = LIVE_SALES_DATA.get("regions", {}).get(requested_region, {})
    today = _safe_float(region_data.get("today"))
    lw = _safe_float(region_data.get("lw"))
    lm = _safe_float(region_data.get("lm"))
    ly = _safe_float(region_data.get("ly"))

    if today == 0 and lw == 0 and lm == 0 and ly == 0:
        selected = {
            name: data for name, data in LIVE_SALES_DATA.get("stores", {}).items()
            if str(data.get("region", "")).strip().lower() == str(requested_region).strip().lower()
        }
        today = sum(_safe_float(v.get("today")) for v in selected.values())
        lw = sum(_safe_float(v.get("lw")) for v in selected.values())
        lm = sum(_safe_float(v.get("lm")) for v in selected.values())
        ly = sum(_safe_float(v.get("ly")) for v in selected.values())

    reply = (
        "🌍 *AI MIS | REGION SALES*\n"
        f"{LIVE_SALES_DATA.get('date', '')}\n"
        f"{LIVE_SALES_DATA.get('report_time', '')}\n\n"
        f"📍 *Region: {requested_region}*\n\n"
        f"💰 Today: ₹{today / 100000:.2f}L\n"
        f"📅 Last Week: ₹{lw / 100000:.2f}L\n"
        f"📆 Last Month: ₹{lm / 100000:.2f}L\n"
        f"📆 Last Year: ₹{ly / 100000:.2f}L\n\n"
        f"📈 vs LW: {_growth(today, lw):+.1f}%\n"
        f"📊 vs LM: {_growth(today, lm):+.1f}%\n"
        f"📊 vs LY: {_growth(today, ly):+.1f}%"
    )

    print(reply)
    send_whatsapp_message(sender, reply)


# =========================================================
# 🔐 ROLE BASED FTD SALES
# =========================================================

def send_role_based_ftd_sales(sender):
    user = get_user_access(sender)

    if not user:
        send_whatsapp_message(sender, "❌ Your mobile number is not mapped for AI MIS access.")
        return

    role = str(user.get("role", "")).strip().lower()

    print("Sender:", sender)
    print("Role:", user.get("role"))
    print("Region:", user.get("region"))
    print("Patch:", user.get("patch"))
    print("Stores:", user.get("stores"))

    if role == "ops leader":
        print("👔 Ops Leader → Overall Sales")
        send_ftd_sales(sender)
        return

    if role == "region manager":
        print("🌍 Region Manager → Region Sales")
        send_region_ftd_sales(sender, user)
        return

    if role == "area manager":
        print("🏪 Area Manager → Assigned Store Sales")
        send_area_manager_ftd_sales(sender, user)
        return

    send_whatsapp_message(sender, "❌ Your AI MIS role is not configured.")


# =========================================================
# 🏪 AREA MANAGER FTD SALES
# =========================================================

def send_area_manager_ftd_sales(sender, user):
    allowed = user.get("stores", [])
    if not isinstance(allowed, list):
        allowed = [allowed]

    if any(str(x).strip().lower() == "all" for x in allowed):
        totals, selected = aggregate_store_periods()
    else:
        totals, selected = aggregate_store_periods(allowed)

    if not selected:
        send_whatsapp_message(sender, "⚠️ No sales data found for your mapped stores.")
        return

    today = totals["today"]
    lw = totals["lw"]
    lm = totals["lm"]
    ly = totals["ly"]

    store_lines = []
    for name, data in sorted(selected.items(), key=lambda x: _safe_float(x[1].get("today")), reverse=True):
        store_today = _safe_float(data.get("today"))
        store_lw = _safe_float(data.get("lw"))
        store_growth = _safe_float(data.get("growth"), _growth(store_today, store_lw))
        store_lines.append(f"• {name}: ₹{store_today / 1000:.1f}K ({store_growth:+.1f}% vs LW)")

    reply = (
        "🏪 *AI MIS | AREA SALES*\n"
        f"{LIVE_SALES_DATA.get('date', '')}\n"
        f"{LIVE_SALES_DATA.get('report_time', '')}\n\n"
        f"📍 Patch: {user.get('patch', '')}\n"
        f"🏪 Stores: {len(selected)}\n\n"
        f"💰 Today: ₹{today / 100000:.2f}L\n"
        f"📅 Last Week: ₹{lw / 100000:.2f}L\n"
        f"📆 Last Month: ₹{lm / 100000:.2f}L\n"
        f"📆 Last Year: ₹{ly / 100000:.2f}L\n\n"
        f"📈 vs LW: {_growth(today, lw):+.1f}%\n"
        f"📊 vs LM: {_growth(today, lm):+.1f}%\n"
        f"📊 vs LY: {_growth(today, ly):+.1f}%\n\n"
        "🏪 *STORE PERFORMANCE*\n" + "\n".join(store_lines)
    )

    print(reply)
    send_whatsapp_message(sender, reply)


# =========================================================
# 👋 PROCESS MESSAGE
# =========================================================

def process_message(sender, message_text):
    import re

    message = " ".join(str(message_text).strip().lower().split())
    message = re.sub(r"[’'`]", "", message)
    message = re.sub(r"\s+", " ", message).strip()

    debug_user_access(sender)

    print("=" * 60)
    print("🧠 PROCESSING MESSAGE")
    print("Sender     :", sender)
    print("Original   :", message_text)
    print("Normalized :", message)
    print("=" * 60)

    if message in {"hi", "hello", "hey", "hii", "hiii", "good morning", "good afternoon", "good evening"}:
        send_whatsapp_message(
            sender,
            "👋 *AI MIS WhatsApp*\n\n📊 sales\n📈 sales vs lw\n📈 sales vs lm\n📈 sales vs ly\n🏪 Store Name sales\n🌍 TN region\n❓ help"
        )
        return

    if message == "help":
        send_whatsapp_message(
            sender,
            "🤖 *AI MIS WhatsApp*\n\n"
            "📊 *sales*\n"
            "📈 *sales vs lw*\n"
            "📈 *sales vs lm*\n"
            "📈 *sales vs ly*\n"
            "🏪 *Store Name sales*\n"
            "🌍 *TN region*\n"
            "🌍 *sales in TN region*\n\n"
            "Examples:\n"
            "• Byculla sales\n"
            "• Tata Sherwood sales\n"
            "• TN region\n"
            "• sales in TN region"
        )
        return

    # Exact FTD first
    if message in {
        "sales", "sales today", "today sales", "today sales report",
        "todays sales", "ftd", "ftd sales"
    }:
        try:
            send_role_based_ftd_sales(sender)
        except Exception as e:
            print("❌ FTD ERROR:", str(e))
            send_whatsapp_message(sender, f"❌ Error while generating FTD Sales report.\n\nDebug: {e}")
        return

    # Period commands
    if message in {"sales vs lw", "sales vs last week", "sales versus last week", "today vs lw", "today vs last week", "sales comparison"}:
        try:
            send_sales_vs_lw(sender)
        except Exception as e:
            print("❌ SALES VS LW ERROR:", str(e))
            send_whatsapp_message(sender, f"❌ Error while generating Sales vs Last Week report.\n\nDebug: {e}")
        return

    if message in {"sales vs lm", "sales vs last month", "sales versus last month", "today vs lm", "today vs last month"}:
        try:
            send_sales_vs_lm(sender)
        except Exception as e:
            print("❌ SALES VS LM ERROR:", str(e))
            send_whatsapp_message(sender, f"❌ Error while generating Sales vs Last Month report.\n\nDebug: {e}")
        return

    if message in {"sales vs ly", "sales vs last year", "sales versus last year", "today vs ly", "today vs last year"}:
        try:
            send_sales_vs_ly(sender)
        except Exception as e:
            print("❌ SALES VS LY ERROR:", str(e))
            send_whatsapp_message(sender, f"❌ Error while generating Sales vs Last Year report.\n\nDebug: {e}")
        return

    # Natural comparisons
    if "sales" in message and ("last week" in message or "lw" in message):
        try:
            send_sales_vs_lw(sender)
        except Exception as e:
            send_whatsapp_message(sender, f"❌ Error while generating Sales vs Last Week report.\n\nDebug: {e}")
        return

    if "sales" in message and ("last month" in message or "lm" in message):
        try:
            send_sales_vs_lm(sender)
        except Exception as e:
            send_whatsapp_message(sender, f"❌ Error while generating Sales vs Last Month report.\n\nDebug: {e}")
        return

    if "sales" in message and ("last year" in message or "ly" in message):
        try:
            send_sales_vs_ly(sender)
        except Exception as e:
            send_whatsapp_message(sender, f"❌ Error while generating Sales vs Last Year report.\n\nDebug: {e}")
        return

    # Region queries. Supports: TN region, sales in TN region, TN sales
    region_match = re.match(r"^(?:sales\s+in\s+)?([a-z]{2,10})\s+region$", message)
    if region_match:
        send_region_sales_query(sender, region_match.group(1))
        return

    region_match = re.match(r"^(?:sales\s+in\s+)?([a-z]{2,10})\s+sales$", message)
    if region_match and find_region_snapshot_key(region_match.group(1)):
        send_region_sales_query(sender, region_match.group(1))
        return

    # Store query: Byculla sales
    if "sales" in message or "sale" in message:
        store_query = message.replace("sales", "").replace("sale", "").strip()
        if store_query:
            print("🏪 STORE SALES QUERY DETECTED:", store_query)
            try:
                send_store_sales_query(sender, store_query)
            except Exception as e:
                print("❌ STORE SALES QUERY ERROR:", str(e))
                send_whatsapp_message(sender, f"❌ Error while generating store sales report.\n\nDebug: {e}")
            return

    send_whatsapp_message(
        sender,
        "🤖 AI MIS received your message:\n\n"
        f"\"{message_text}\"\n\n"
        "Type *help* to see available commands."
    )


# =========================================================
# 📩 META WEBHOOK RECEIVER
# =========================================================

@app.route("/webhook", methods=["POST"])
def webhook():
    data = request.get_json(silent=True)

    print("=" * 60)
    print("📩 WHATSAPP WEBHOOK RECEIVED")
    print("=" * 60)
    print(json.dumps(data, indent=2, ensure_ascii=False))
    print("=" * 60)

    if not data:
        print("⚠️ Empty webhook payload")
        return "EVENT_RECEIVED", 200

    if data.get("object") != "whatsapp_business_account":
        print("⚠️ Not a WhatsApp Business Account event")
        return "EVENT_RECEIVED", 200

    for entry in data.get("entry", []):
        for change in entry.get("changes", []):
            field = change.get("field")
            value = change.get("value", {})

            print("Webhook field:", field)

            if field != "messages":
                print("ℹ️ Other webhook event:", field)
                continue

            messages = value.get("messages", [])
            print("Number of messages:", len(messages))

            for incoming_message in messages:
                message_type = incoming_message.get("type")
                sender = incoming_message.get("from")

                print("Message type:", message_type)
                print("Sender:", sender)

                if message_type == "text":
                    text_data = incoming_message.get("text", {})
                    message_text = text_data.get("body", "")
                    print("💬 Incoming text:", message_text)

                    if sender and message_text:
                        try:
                            process_message(sender, message_text)
                            print("✅ process_message completed")
                        except Exception as e:
                            print("❌ process_message ERROR:", str(e))
                            send_whatsapp_message(
                                sender,
                                "❌ AI MIS encountered an error while processing your request."
                            )

                else:
                    print("⚠️ Non-text message:", message_type)
                    if sender:
                        send_whatsapp_message(
                            sender,
                            "🤖 AI MIS currently supports text messages only."
                        )

    return "EVENT_RECEIVED", 200

# 👔 TEST SEND — OPS LEADERS ONLY
# =========================================================

@app.route(
    "/test-ops-leaders",
    methods=["GET"]
)
def test_ops_leaders():

    print("=" * 60)
    print("👔 OPS LEADER WHATSAPP TEST")
    print("=" * 60)

    ops_leaders = []

    for mobile, user in WHATSAPP_USERS.items():

        role = str(
            user.get(
                "role",
                ""
            )
        ).strip().lower()

        if role == "ops leader":

            recipient = (
                str(mobile)
                .replace("+", "")
                .replace(" ", "")
                .replace("-", "")
            )

            ops_leaders.append(
                recipient
            )

    ops_leaders = list(
        dict.fromkeys(
            ops_leaders
        )
    )

    print(
        "Ops Leaders found:",
        len(ops_leaders)
    )

    print(
        "Ops Leader numbers:",
        ops_leaders
    )

    if not ops_leaders:

        return jsonify({
            "success": False,
            "message":
                "No Ops Leaders found"
        }), 404

    message = (

        "👔 *AI MIS | OPS LEADER TEST*\n\n"

        "✅ Your WhatsApp access mapping "
        "is working.\n\n"

        "Role: Ops Leader\n"

        "Access: Overall"
    )

    results = []

    for recipient in ops_leaders:

        print(
            "➡️ Sending to:",
            recipient
        )

        try:

            success = (
                send_whatsapp_message(
                    recipient,
                    message
                )
            )

            results.append({

                "recipient":
                    recipient,

                "success":
                    success
            })

        except Exception as e:

            print(
                "❌ Send error:",
                recipient,
                str(e)
            )

            results.append({

                "recipient":
                    recipient,

                "success":
                    False,

                "error":
                    str(e)
            })

    success_count = sum(
        1
        for item in results
        if item["success"]
    )

    failed_count = (
        len(results)
        -
        success_count
    )

    print("=" * 60)

    print(
        "Success:",
        success_count
    )

    print(
        "Failed:",
        failed_count
    )

    print("=" * 60)

    return jsonify({

        "success":
            failed_count == 0,

        "ops_leaders":
            ops_leaders,

        "results":
            results
    }), 200


# =========================================================
# 🧪 TEST SEND
# =========================================================

@app.route(
    "/test-send",
    methods=["GET"]
)
def test_send():

    recipients = [

        "919750820509",
        "919535075140",
        "918892390985",
        "919620952646"

    ]

    print("=" * 60)
    print("📤 AI MIS WHATSAPP TEST SEND")
    print("=" * 60)

    success_count = 0
    failed_count = 0

    for recipient in recipients:

        success = send_whatsapp_message(

            recipient,

            "🤖 AI MIS webhook test message"
        )

        if success:

            success_count += 1

        else:

            failed_count += 1

    print("=" * 60)

    print(
        "WhatsApp Success:",
        success_count
    )

    print(
        "WhatsApp Failed:",
        failed_count
    )

    print("=" * 60)

    return jsonify({

        "success":
            failed_count == 0,

        "results": {

            "success_count":
                success_count,

            "failed_count":
                failed_count,

            "recipients":
                recipients
        }

    }), 200


# =========================================================
# 🚀 LOCAL RUN
# =========================================================

if __name__ == "__main__":

    port = int(
        os.environ.get(
            "PORT",
            5000
        )
    )

    app.run(
        host="0.0.0.0",
        port=port
    )
