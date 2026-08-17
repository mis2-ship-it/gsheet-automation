# =========================================================
# 📱 AI MIS WHATSAPP WEBHOOK
# =========================================================

from flask import Flask, request, jsonify
import os
import json
import re
import requests
from datetime import datetime

from whatsapp_recipients import (
    WHATSAPP_USERS,
    get_user_access,
)

# =========================================================
# 📚 HISTORICAL SALES ENGINE
# =========================================================

from historical_sales import (
    get_last_n_months_performance,
    get_store_performance,
    get_brand_performance,
    get_region_performance,
    get_seasonality,
    get_best_worst,
    compare_periods,
    classify_historical_query,
    format_history_summary,
)

# =========================================================
# 📱 WHATSAPP GUIDED MENU
# =========================================================

from whatsapp_menu import (
    start_menu,
    handle_menu_selection,
    build_list_message,
    build_button_message,
    clear_session,
)

app = Flask(__name__)


# =========================================================
# 🔐 ENVIRONMENT VARIABLES
# =========================================================

VERIFY_TOKEN = os.environ.get("WHATSAPP_VERIFY_TOKEN")
PHONE_NUMBER_ID = os.environ.get("WHATSAPP_PHONE_NUMBER_ID")
ACCESS_TOKEN = os.environ.get("WHATSAPP_ACCESS_TOKEN")
WHATSAPP_DATA_SECRET = os.environ.get("WHATSAPP_DATA_SECRET")
GRAPH_API_VERSION = os.environ.get("WHATSAPP_GRAPH_API_VERSION", "v23.0")

# Local snapshot cache. This prevents an ordinary process restart
# from immediately losing the last pushed sales snapshot.
SNAPSHOT_FILE = os.environ.get(
    "WHATSAPP_SNAPSHOT_FILE",
    "whatsapp_sales_snapshot.json"
)

LIVE_SALES_DATA = {}



# =========================================================
# 💾 SALES SNAPSHOT PERSISTENCE
# =========================================================

def save_sales_snapshot(data):

    global LIVE_SALES_DATA

    LIVE_SALES_DATA = data

    try:

        with open(
            SNAPSHOT_FILE,
            "w",
            encoding="utf-8"
        ) as f:

            json.dump(
                data,
                f,
                ensure_ascii=False
            )

        print(
            "✅ Sales snapshot saved locally"
        )

    except Exception as e:

        print(
            "❌ Failed to save sales snapshot:",
            str(e)
        )


def load_sales_snapshot():

    global LIVE_SALES_DATA

    try:

        if not os.path.exists(
            SNAPSHOT_FILE
        ):

            print(
                "⚠️ No saved sales snapshot found"
            )

            return

        with open(
            SNAPSHOT_FILE,
            "r",
            encoding="utf-8"
        ) as f:

            LIVE_SALES_DATA = json.load(f)

        print("=" * 60)
        print("✅ SAVED SALES SNAPSHOT LOADED")
        print(
            "Date:",
            LIVE_SALES_DATA.get("date")
        )
        print(
            "Time:",
            LIVE_SALES_DATA.get("report_time")
        )
        print(
            "Stores:",
            len(
                LIVE_SALES_DATA.get(
                    "stores",
                    {}
                )
            )
        )
        print("=" * 60)

    except Exception as e:

        print(
            "❌ Failed to load saved sales snapshot:",
            str(e)
        )

        LIVE_SALES_DATA = {}


# =========================================================
# 🚀 STARTUP CHECK
# =========================================================

print("=" * 60)
print("🚀 AI MIS WHATSAPP WEBHOOK")
print("=" * 60)
print("PHONE_NUMBER_ID exists       :", bool(PHONE_NUMBER_ID))
print("ACCESS_TOKEN exists          :", bool(ACCESS_TOKEN))
print("VERIFY_TOKEN exists          :", bool(VERIFY_TOKEN))
print("WHATSAPP_DATA_SECRET exists  :", bool(WHATSAPP_DATA_SECRET))
print("SNAPSHOT_FILE                :", SNAPSHOT_FILE)
print("GUIDED MENU ENGINE           :", True)
print("=" * 60)


# =========================================================
# 🧮 HELPERS
# =========================================================

def _safe_float(value, default=0.0):
    try:
        if value is None or value == "":
            return float(default)
        return float(value)
    except (TypeError, ValueError):
        return float(default)


def _growth(today, previous):
    today = _safe_float(today)
    previous = _safe_float(previous)

    if previous == 0:
        if today == 0:
            return 0.0
        return 100.0

    return ((today - previous) / previous) * 100.0


def _performance(growth):
    growth = _safe_float(growth)

    if growth > 5:
        return "🚀 Strong Growth"
    if growth > 0:
        return "📈 Growth"
    if growth < -5:
        return "🔻 Decline"
    return "➡️ Stable"


def _normalize_text(value):
    value = str(value or "").strip().lower()
    value = re.sub(r"[’'`]", "", value)
    value = re.sub(r"[^a-z0-9]+", " ", value)
    value = re.sub(r"\s+", " ", value).strip()
    return value


def _normalize_store_name(value):
    """Normalize store names while keeping meaningful words."""
    value = _normalize_text(value)

    # Remove common operational suffixes that users often omit.
    tokens = [
        token
        for token in value.split()
        if token not in {"ck", "cf"}
    ]

    return " ".join(tokens)


def _normalize_region_name(value):
    return _normalize_text(value).replace(" ", "")


def _format_lacs(value):
    return f"₹{_safe_float(value) / 100000:.2f}L"


def _format_thousands(value):
    return f"₹{_safe_float(value) / 1000:.1f}K"


# =========================================================
# 💾 SNAPSHOT PERSISTENCE
# =========================================================

def save_sales_snapshot(data):
    global LIVE_SALES_DATA

    LIVE_SALES_DATA = data or {}

    try:
        with open(
            SNAPSHOT_FILE,
            "w",
            encoding="utf-8"
        ) as f:
            json.dump(
                LIVE_SALES_DATA,
                f,
                ensure_ascii=False
            )

        print("✅ Sales snapshot saved locally")

    except Exception as e:
        print(
            "❌ Failed to save sales snapshot:",
            str(e)
        )


def load_sales_snapshot():
    global LIVE_SALES_DATA

    try:
        if not os.path.exists(SNAPSHOT_FILE):
            print("⚠️ No saved sales snapshot found")
            return

        with open(
            SNAPSHOT_FILE,
            "r",
            encoding="utf-8"
        ) as f:
            LIVE_SALES_DATA = json.load(f)

        print("=" * 60)
        print("✅ SAVED SALES SNAPSHOT LOADED")
        print("Date   :", LIVE_SALES_DATA.get("date"))
        print("Time   :", LIVE_SALES_DATA.get("report_time"))
        print(
            "Stores :",
            len(LIVE_SALES_DATA.get("stores", {}))
        )
        print("=" * 60)

    except Exception as e:
        print(
            "❌ Failed to load saved sales snapshot:",
            str(e)
        )
        LIVE_SALES_DATA = {}


# Load before serving requests.
load_sales_snapshot()


# =========================================================
# 🔐 USER ACCESS DEBUG
# =========================================================

def debug_user_access(sender):
    print("=" * 60)
    print("🔐 WHATSAPP ACCESS DEBUG")
    print("=" * 60)

    user = get_user_access(sender)

    if not user:
        print("❌ User not mapped:", sender)
        print("=" * 60)
        return None

    print("Sender :", sender)
    print("Role   :", user.get("role"))
    print("Region :", user.get("region"))
    print("Patch  :", user.get("patch"))
    print("Stores :", user.get("stores"))
    print("=" * 60)

    return user


# =========================================================
# 🌍 REGION HELPERS
# =========================================================

def find_region_snapshot_key(requested_region):
    """Map TN/KA/MH/KL and common names to the actual snapshot key."""
    regions = LIVE_SALES_DATA.get("regions", {}) or {}
    requested = _normalize_region_name(requested_region)

    aliases = {
        "tn": {"tn", "tamilnadu"},
        "ka": {"ka", "karnataka"},
        "mh": {"mh", "maharashtra"},
        "kl": {"kl", "kerala", "kerela"},
        "overall": {"overall", "all"},
    }

    # Direct normalized match first.
    for region_name in regions.keys():
        if _normalize_region_name(region_name) == requested:
            return region_name

    # Alias match.
    for canonical, alias_set in aliases.items():
        if requested in {
            _normalize_region_name(x)
            for x in alias_set
        }:
            for region_name in regions.keys():
                normalized = _normalize_region_name(region_name)
                if normalized in {
                    _normalize_region_name(x)
                    for x in alias_set
                }:
                    return region_name

    return None


def _region_from_stores(region_name):
    """Aggregate region data from store snapshots when region totals are missing."""
    requested = _normalize_text(region_name)
    selected = {}

    for name, data in (LIVE_SALES_DATA.get("stores", {}) or {}).items():
        store_region = _normalize_text(data.get("region", ""))
        if store_region == requested:
            selected[name] = data

    return selected


def _region_sales_values(region_name):
    """Return today/LW/LM/LY for a region with store-level fallback."""
    regions = LIVE_SALES_DATA.get("regions", {}) or {}
    actual_region = find_region_snapshot_key(region_name)

    if actual_region:
        region_data = regions.get(actual_region, {}) or {}
    else:
        region_data = {}

    today = _safe_float(region_data.get("today"))
    lw = _safe_float(region_data.get("lw"))
    lm = _safe_float(region_data.get("lm"))
    ly = _safe_float(region_data.get("ly"))

    # If any comparison period is missing, aggregate from store snapshot.
    selected = _region_from_stores(actual_region or region_name)

    if selected:
        if today == 0:
            today = sum(_safe_float(v.get("today")) for v in selected.values())
        if lw == 0:
            lw = sum(_safe_float(v.get("lw")) for v in selected.values())
        if lm == 0:
            lm = sum(_safe_float(v.get("lm")) for v in selected.values())
        if ly == 0:
            ly = sum(_safe_float(v.get("ly")) for v in selected.values())

    return {
        "region": actual_region,
        "today": today,
        "lw": lw,
        "lm": lm,
        "ly": ly,
    }


# =========================================================
# 🏪 STORE HELPERS
# =========================================================

def _find_store_snapshot(store_query):
    stores = LIVE_SALES_DATA.get("stores", {}) or {}

    requested = _normalize_store_name(store_query)

    if not requested:
        return None, None

    # 1. Exact normalized match.
    for actual_name, data in stores.items():
        if _normalize_store_name(actual_name) == requested:
            return actual_name, data

    # 2. Containment match for common abbreviations.
    candidates = []
    for actual_name, data in stores.items():
        actual_normalized = _normalize_store_name(actual_name)
        if requested in actual_normalized or actual_normalized in requested:
            candidates.append((actual_name, data))

    if len(candidates) == 1:
        return candidates[0]

    # 3. Token overlap for queries like "malad ck" -> "malad cf ck".
    requested_tokens = set(requested.split())
    scored = []

    for actual_name, data in stores.items():
        actual_tokens = set(
            _normalize_store_name(actual_name).split()
        )

        overlap = len(
            requested_tokens & actual_tokens
        )

        if overlap > 0:
            scored.append(
                (overlap, len(actual_tokens), actual_name, data)
            )

    if scored:
        scored.sort(
            key=lambda x: (x[0], -x[1]),
            reverse=True
        )

        best = scored[0]

        # Accept only if all requested tokens are represented.
        if requested_tokens.issubset(
            set(_normalize_store_name(best[2]).split())
        ):
            return best[2], best[3]

    return None, None


def extract_store_query(message):
    """Extract a store name from common natural-language requests."""
    text = _normalize_text(message)

    patterns = [
        r"^(.+?) sales$",
        r"^(.+?) sale$",
        r"^sales of (.+)$",
        r"^sales for (.+)$",
        r"^show (.+?) sales$",
        r"^show me (.+?) sales$",
        r"^what is (.+?) sales$",
        r"^what are (.+?) sales$",
        r"^(.+?) revenue$",
        r"^(.+?) today sales$",
        r"^(.+?) sales today$",
        r"^(.+?) today$",
    ]

    for pattern in patterns:
        match = re.match(pattern, text)
        if match:
            return match.group(1).strip()

    return None


def extract_region_query(message):
    """Extract region query from TN region / sales in TN / Tamil Nadu sales."""
    text = _normalize_text(message)

    # Explicit "region" requests.
    if text.endswith(" region"):
        return text[:-7].strip()

    if text.endswith(" region sales"):
        return text[:-12].strip()

    if text.startswith("sales in "):
        return text[9:].strip()

    if text.startswith("sales for "):
        return text[10:].strip()

    # Direct short aliases / names.
    if text in {
        "tn", "ka", "mh", "kl",
        "tamil nadu", "tamilnadu",
        "karnataka", "maharashtra",
        "kerala", "kerela"
    }:
        return text

    # "tn sales", "tamil nadu sales"
    if text.endswith(" sales"):
        candidate = text[:-6].strip()
        if find_region_snapshot_key(candidate):
            return candidate

    return None


# =========================================================
# 🏠 HOME
# =========================================================

@app.route("/", methods=["GET"])
def home():
    return "AI MIS WhatsApp Webhook is running", 200


# =========================================================
# 🔐 META WEBHOOK VERIFICATION
# =========================================================

@app.route("/webhook", methods=["GET"])
def verify_webhook():
    mode = request.args.get("hub.mode")
    token = request.args.get("hub.verify_token")
    challenge = request.args.get("hub.challenge")

    print("=" * 60)
    print("🔐 META WEBHOOK VERIFICATION")
    print("Mode     :", mode)
    print("Token OK :", token == VERIFY_TOKEN)
    print("=" * 60)

    if mode == "subscribe" and token == VERIFY_TOKEN:
        print("✅ META WEBHOOK VERIFIED")
        return challenge, 200

    print("❌ WEBHOOK VERIFICATION FAILED")
    return "Forbidden", 403


# =========================================================
# 📤 SEND WHATSAPP TEXT MESSAGE
# =========================================================

def send_whatsapp_message(recipient, message):
    print("=" * 60)
    print("📤 SENDING WHATSAPP MESSAGE")
    print("To:", recipient)
    print("Message:")
    print(message)
    print("=" * 60)

    if not PHONE_NUMBER_ID:
        print("❌ WHATSAPP_PHONE_NUMBER_ID missing")
        return False

    if not ACCESS_TOKEN:
        print("❌ WHATSAPP_ACCESS_TOKEN missing")
        return False

    url = (
        f"https://graph.facebook.com/"
        f"{GRAPH_API_VERSION}/"
        f"{PHONE_NUMBER_ID}/messages"
    )

    headers = {
        "Authorization": f"Bearer {ACCESS_TOKEN}",
        "Content-Type": "application/json",
    }

    payload = {
        "messaging_product": "whatsapp",
        "to": recipient,
        "type": "text",
        "text": {
            "preview_url": False,
            "body": message,
        },
    }

    try:
        response = requests.post(
            url,
            headers=headers,
            json=payload,
            timeout=30,
        )

        print("Meta Status   :", response.status_code)
        print("Meta Response :", response.text)

        if response.ok:
            print("✅ WhatsApp message sent")
            return True

        print("❌ WhatsApp message failed")
        return False

    except Exception as e:
        print("❌ WhatsApp API error:", str(e))
        return False


# =========================================================
# 📱 SEND WHATSAPP INTERACTIVE LIST
# =========================================================

def send_whatsapp_interactive(
    recipient,
    body_text,
    options,
    button_text="Select",
    section_title="Options",
):
    print("=" * 60)
    print("📱 SENDING WHATSAPP INTERACTIVE MENU")
    print("To:", recipient)
    print("Body:", body_text)
    print("=" * 60)

    if not PHONE_NUMBER_ID:
        print("❌ WHATSAPP_PHONE_NUMBER_ID missing")
        return False

    if not ACCESS_TOKEN:
        print("❌ WHATSAPP_ACCESS_TOKEN missing")
        return False

    payload = build_list_message(
        recipient=recipient,
        body_text=body_text,
        options=options,
        button_text=button_text,
        section_title=section_title,
    )

    url = (
        f"https://graph.facebook.com/"
        f"{GRAPH_API_VERSION}/"
        f"{PHONE_NUMBER_ID}/messages"
    )

    headers = {
        "Authorization": f"Bearer {ACCESS_TOKEN}",
        "Content-Type": "application/json",
    }

    try:
        response = requests.post(
            url,
            headers=headers,
            json=payload,
            timeout=30,
        )

        print("Meta Interactive Status:", response.status_code)
        print("Meta Interactive Response:", response.text)

        if response.ok:
            print("✅ Interactive menu sent")
            return True

        print("❌ Interactive menu failed")
        return False

    except Exception as e:
        print("❌ Interactive menu error:", str(e))
        return False


# =========================================================
# 🤖 START GUIDED AI MIS MENU
# =========================================================

def send_main_menu(sender):
    result = start_menu(sender)
    options = result.get("options", [])

    if not options:
        send_whatsapp_message(
            sender,
            "❌ Your mobile number is not mapped for AI MIS access."
        )
        return

    send_whatsapp_interactive(
        recipient=sender,
        body_text=(
            "👋 *AI MIS WhatsApp*\n\n"
            "What are you looking for?"
        ),
        options=options,
        button_text="Select",
        section_title="Sales Analysis",
    )


# =========================================================
# 📥 RECEIVE RISTA LIVE SALES SNAPSHOT
# =========================================================

@app.route("/update-sales-data", methods=["POST"])
def update_sales_data():

    print("=" * 60)
    print("📥 RISTA LIVE SALES DATA RECEIVED")
    print("=" * 60)

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

        return jsonify({
            "success": False,
            "error":
                "Server secret not configured",
        }), 500

    if incoming_secret != WHATSAPP_DATA_SECRET:

        print(
            "❌ Invalid WhatsApp data secret"
        )

        return jsonify({
            "success": False,
            "error":
                "Unauthorized",
        }), 401

    data = request.get_json(
        silent=True
    )

    if not data:

        return jsonify({
            "success": False,
            "error":
                "Empty JSON payload",
        }), 400

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

    # ✅ THIS IS CORRECT
    save_sales_snapshot(data)

    print(
        "LIVE_SALES_DATA keys:",
        list(
            LIVE_SALES_DATA.keys()
        )
    )

    print("=" * 60)

    return jsonify({
        "success": True,
        "message":
            "Sales data updated successfully",
    }), 200


# =========================================================
# 🔍 CURRENT SALES SNAPSHOT
# =========================================================

@app.route("/sales-data", methods=["GET"])
def sales_data():
    print("=" * 60)
    print("🔍 SALES DATA REQUEST")
    print("=" * 60)
    print("LIVE_SALES_DATA empty:", not bool(LIVE_SALES_DATA))
    print("LIVE_SALES_DATA keys:", list(LIVE_SALES_DATA.keys()))

    if not LIVE_SALES_DATA:
        return jsonify({
            "success": False,
            "message": "No sales data available",
        }), 404

    return jsonify({
        "success": True,
        "data": LIVE_SALES_DATA,
    }), 200


# =========================================================
# 📊 FTD SALES DATA
# =========================================================

def get_ftd_sales():
    print("=" * 60)
    print("📊 GET FTD SALES FROM SNAPSHOT")
    print("=" * 60)

    if not LIVE_SALES_DATA:
        print("❌ LIVE_SALES_DATA is empty")
        return None

    overall = LIVE_SALES_DATA.get("overall", {}) or {}

    result = {
        "date": LIVE_SALES_DATA.get("date", ""),
        "report_time": LIVE_SALES_DATA.get("report_time", ""),
        "net": _safe_float(overall.get("net")),
        "txn": _safe_float(overall.get("txn")),
        "aov": _safe_float(overall.get("aov")),
        "discount": _safe_float(overall.get("discount")),
    }

    print("Date:", result["date"])
    print("Report Time:", result["report_time"])
    print("Net:", result["net"])
    print("Transactions:", result["txn"])
    print("AOV:", result["aov"])
    print("Discount:", result["discount"])
    print("=" * 60)

    return result

# =========================================================
# 📊 FTD SALES RESPONSE
# =========================================================

def send_ftd_sales(sender):

    print("=" * 60)
    print("📊 FTD SALES RESPONSE")
    print("=" * 60)

    sales = get_ftd_sales()

    if not sales:

        send_whatsapp_message(
            sender,
            "⚠️ Sales data is not available right now."
        )

        return

    net = float(
        sales.get(
            "net",
            0
        ) or 0
    )

    txn = float(
        sales.get(
            "txn",
            0
        ) or 0
    )

    aov = float(
        sales.get(
            "aov",
            0
        ) or 0
    )

    discount = float(
        sales.get(
            "discount",
            0
        ) or 0
    )

    report_date = sales.get(
        "date",
        ""
    )

    brands = LIVE_SALES_DATA.get(
        "brands",
        {}
    )

    reply_lines = [

        "📊 *AI MIS | FTD SALES*",

        str(
            report_date
        ),

        "",

        f"💰 Net Revenue: "
        f"₹{net / 100000:.2f}L",

        f"🧾 Transactions: "
        f"{int(txn):,}",

        f"🧺 AOV: "
        f"₹{int(round(aov)):,}",

        f"📉 Discount: "
        f"{abs(discount):.1f}%"
    ]

    # -----------------------------------------------------
    # BRAND CONTRIBUTION
    # -----------------------------------------------------

    if brands:

        reply_lines.extend(
            [
                "",
                "🏪 *Brand Contribution*"
            ]
        )

        brand_values = {}

        for brand_name, brand_data in brands.items():

            value = float(
                brand_data.get(
                    "today",
                    0
                ) or 0
            )

            brand_values[
                brand_name
            ] = value

        total_brand = sum(
            brand_values.values()
        )

        for brand_name, value in brand_values.items():

            contribution = (
                value
                /
                max(
                    total_brand,
                    1
                )
                *
                100
            )

            reply_lines.append(
                f"• {brand_name}: "
                f"{contribution:.0f}%"
            )

    reply = "\n".join(
        reply_lines
    )

    print(
        "WhatsApp FTD Reply:"
    )

    print(reply)

    print("=" * 60)

    send_whatsapp_message(
        sender,
        reply
    )


def get_brand_data():
    return LIVE_SALES_DATA.get("brands", {}) or {}


def get_source_data():
    return LIVE_SALES_DATA.get("sources", {}) or {}


# =========================================================
# 📈 SCOPED COMPARISON DATA
# =========================================================

def get_scoped_sales(user):
    """Return today/LW/LM/LY according to the user's role."""
    role = _normalize_text(user.get("role", ""))

    if role == "ops leader":
        overall = LIVE_SALES_DATA.get("overall", {}) or {}
        return {
            "scope": "Overall",
            "today": _safe_float(overall.get("net")),
            "lw": _safe_float(overall.get("lw_net")),
            "lm": _safe_float(overall.get("lm_net")),
            "ly": _safe_float(overall.get("ly_net")),
        }

    if role == "region manager":
        region_name = user.get("region", "")
        actual = find_region_snapshot_key(region_name)
        if not actual:
            return None
        values = _region_sales_values(actual)
        return {
            "scope": actual,
            "today": values["today"],
            "lw": values["lw"],
            "lm": values["lm"],
            "ly": values["ly"],
        }

    if role == "area manager":
        stores = LIVE_SALES_DATA.get("stores", {}) or {}
        allowed = user.get("stores", [])
        if not isinstance(allowed, list):
            allowed = [allowed]

        matched = {}
        for allowed_store in allowed:
            if _normalize_text(allowed_store) == "all":
                matched = dict(stores)
                break
            name, data = _find_store_snapshot(allowed_store)
            if name:
                matched[name] = data

        if not matched:
            return None

        return {
            "scope": user.get("patch") or "Your Stores",
            "today": sum(_safe_float(v.get("today")) for v in matched.values()),
            "lw": sum(_safe_float(v.get("lw")) for v in matched.values()),
            "lm": sum(_safe_float(v.get("lm")) for v in matched.values()),
            "ly": sum(_safe_float(v.get("ly")) for v in matched.values()),
        }

    return None


def send_comparison(sender, period):
    user = get_user_access(sender)

    if not user:
        send_whatsapp_message(
            sender,
            "❌ Your mobile number is not mapped for AI MIS access."
        )
        return

    if not LIVE_SALES_DATA:
        send_whatsapp_message(
            sender,
            "⚠️ Sales data is not available right now."
        )
        return

    scoped = get_scoped_sales(user)

    if not scoped:
        role = _normalize_text(user.get("role", ""))
        if role == "area manager":
            message = "⚠️ No sales data found for your mapped stores."
        elif role == "region manager":
            message = "⚠️ Sales data is not available for your mapped region."
        else:
            message = "⚠️ Sales data is not available right now."
        send_whatsapp_message(sender, message)
        return

    today = scoped["today"]
    comparison_key = period.lower()
    comparison = scoped.get(comparison_key, 0.0)

    if comparison_key == "lw":
        label = "LAST WEEK"
        emoji = "📅"
    elif comparison_key == "lm":
        label = "LAST MONTH"
        emoji = "📆"
    else:
        label = "LAST YEAR"
        emoji = "📆"

    growth = _growth(today, comparison)
    performance = _performance(growth)

    reply = (
        f"📊 *AI MIS | SALES vs {label}*\n"
        f"{LIVE_SALES_DATA.get('date', '')}\n"
        f"{LIVE_SALES_DATA.get('report_time', '')}\n\n"
        f"📍 *{scoped['scope']}*\n"
        f"💰 *Today:* {_format_lacs(today)}\n"
        f"{emoji} *{label.title()}:* {_format_lacs(comparison)}\n"
        f"📈 *Growth:* {growth:+.1f}%\n"
        f"🧠 *Performance:* {performance}"
    )

    print(reply)
    send_whatsapp_message(sender, reply)




# =========================================================
# 📈 LAST WEEK COMPATIBILITY FUNCTION
# =========================================================

def get_sales_vs_lw():
    if not LIVE_SALES_DATA:
        raise Exception("Live sales backend data is not available")

    overall = LIVE_SALES_DATA.get("overall", {}) or {}

    return {
        "today_net": _safe_float(overall.get("net")),
        "lw_net": _safe_float(overall.get("lw_net")),
        "growth": _safe_float(
            overall.get("lw_growth"),
            _growth(
                overall.get("net"),
                overall.get("lw_net")
            )
        ),
        "today_txn": _safe_float(overall.get("txn")),
        "lw_txn": _safe_float(overall.get("lw_txn")),
        "today_aov": _safe_float(overall.get("aov")),
        "lw_aov": _safe_float(overall.get("lw_aov")),
        "today_discount": _safe_float(overall.get("discount")),
        "lw_discount": _safe_float(overall.get("lw_discount")),
    }


def send_sales_vs_lw(sender):
    send_comparison(sender, "lw")


def send_sales_vs_lm(sender):
    send_comparison(sender, "lm")


def send_sales_vs_ly(sender):
    send_comparison(sender, "ly")


# =========================================================
# 🏪 AREA MANAGER FTD SALES
# =========================================================

def send_area_manager_ftd_sales(sender, user):
    if not LIVE_SALES_DATA:
        send_whatsapp_message(sender, "⚠️ Sales data is not available right now.")
        return

    stores = LIVE_SALES_DATA.get("stores", {}) or {}
    allowed_stores = user.get("stores", [])
    if not isinstance(allowed_stores, list):
        allowed_stores = [allowed_stores]

    matched = {}
    not_found = []

    for allowed_store in allowed_stores:
        if _normalize_text(allowed_store) == "all":
            matched = dict(stores)
            not_found = []
            break

        name, data = _find_store_snapshot(allowed_store)
        if name:
            matched[name] = data
        else:
            not_found.append(allowed_store)

    print("Mapped Stores:", allowed_stores)
    print("Matched Stores:", list(matched.keys()))

    if not matched:
        send_whatsapp_message(
            sender,
            "⚠️ No sales data found for your mapped stores."
        )
        return

    today = sum(_safe_float(v.get("today")) for v in matched.values())
    lw = sum(_safe_float(v.get("lw")) for v in matched.values())
    lm = sum(_safe_float(v.get("lm")) for v in matched.values())

    growth_lw = _growth(today, lw)
    growth_lm = _growth(today, lm)

    lines = [
        "🏪 *AI MIS | AREA SALES*",
        LIVE_SALES_DATA.get("date", ""),
        LIVE_SALES_DATA.get("report_time", ""),
        "",
        f"📍 *Patch:* {user.get('patch', '')}",
        f"🏪 *Stores:* {len(matched)}",
        "",
        "💰 *NET REVENUE*",
        f"🟢 Today: {_format_lacs(today)}",
        f"🔵 Last Week: {_format_lacs(lw)}",
        f"🟣 Last Month: {_format_lacs(lm)}",
        f"📈 vs LW: {growth_lw:+.1f}%",
        f"📊 vs LM: {growth_lm:+.1f}%",
        f"🧠 Performance: {_performance(growth_lw)}",
        "",
        "🏪 *STORE PERFORMANCE*",
    ]

    for store_name, data in sorted(
        matched.items(),
        key=lambda item: _safe_float(item[1].get("today")),
        reverse=True
    ):
        store_today = _safe_float(data.get("today"))
        store_lw = _safe_float(data.get("lw"))
        lines.append(
            f"• {store_name}: {_format_thousands(store_today)} "
            f"({_growth(store_today, store_lw):+.1f}% vs LW)"
        )

    send_whatsapp_message(sender, "\n".join(lines))


# =========================================================
# 🔐 ROLE BASED FTD SALES
# =========================================================

def send_role_based_ftd_sales(sender):
    user = get_user_access(sender)

    if not user:
        send_whatsapp_message(
            sender,
            "❌ Your mobile number is not mapped for AI MIS access."
        )
        return

    role = _normalize_text(user.get("role", ""))

    print("Sender:", sender)
    print("Role:", user.get("role"))
    print("Region:", user.get("region"))
    print("Patch:", user.get("patch"))
    print("Stores:", user.get("stores"))

    if role == "ops leader":
        send_ftd_sales(sender)
        return

    if role == "region manager":
        send_region_ftd_sales(sender, user)
        return

    if role == "area manager":
        send_area_manager_ftd_sales(sender, user)
        return

    send_whatsapp_message(
        sender,
        "❌ Your AI MIS role is not configured."
    )


# =========================================================
# 🌍 REGION MANAGER FTD SALES
# =========================================================

def send_region_ftd_sales(sender, user):
    region_name = user.get("region", "")
    actual_region = find_region_snapshot_key(region_name)

    if not actual_region:
        available = ", ".join(
            (LIVE_SALES_DATA.get("regions", {}) or {}).keys()
        )
        send_whatsapp_message(
            sender,
            (
                "⚠️ Region data is not available for your mapped region.\n\n"
                f"Mapped Region: {region_name}\n"
                f"Available Regions: {available}"
            )
        )
        return

    values = _region_sales_values(actual_region)
    today = values["today"]
    lw = values["lw"]

    reply = (
        "🌍 *AI MIS | REGION SALES*\n"
        f"{LIVE_SALES_DATA.get('date', '')}\n"
        f"{LIVE_SALES_DATA.get('report_time', '')}\n\n"
        f"📍 *Region:* {actual_region}\n\n"
        f"💰 *NET REVENUE*\n"
        f"🟢 Today: {_format_lacs(today)}\n"
        f"🔵 Last Week: {_format_lacs(lw)}\n"
        f"📈 Growth: {_growth(today, lw):+.1f}%"
    )

    send_whatsapp_message(sender, reply)


# =========================================================
# 🌍 REGION SALES QUERY
# =========================================================

def send_region_sales_query(sender, region_name):
    user = get_user_access(sender)

    if not user:
        send_whatsapp_message(
            sender,
            "❌ Your mobile number is not mapped for AI MIS access."
        )
        return

    role = _normalize_text(user.get("role", ""))
    requested_region = find_region_snapshot_key(region_name)

    if not requested_region:
        available = ", ".join(
            (LIVE_SALES_DATA.get("regions", {}) or {}).keys()
        )
        send_whatsapp_message(
            sender,
            (
                f"❌ Region not found: {region_name}\n\n"
                f"Available: {available}"
            )
        )
        return

    if role == "region manager":
        user_region = find_region_snapshot_key(
            user.get("region", "")
        )
        if user_region != requested_region:
            send_whatsapp_message(
                sender,
                f"❌ You don't have access to *{requested_region}* region."
            )
            return

    elif role == "area manager":
        send_whatsapp_message(
            sender,
            (
                "❌ Area Managers have store-level access.\n"
                "Please ask for a specific store, for example:\n"
                "*Byculla sales*"
            )
        )
        return

    elif role != "ops leader":
        send_whatsapp_message(
            sender,
            "❌ Your AI MIS role is not configured."
        )
        return

    values = _region_sales_values(requested_region)

    today = values["today"]
    lw = values["lw"]
    lm = values["lm"]
    ly = values["ly"]

    reply = (
        "🌍 *AI MIS | REGION SALES*\n"
        f"{LIVE_SALES_DATA.get('date', '')}\n"
        f"{LIVE_SALES_DATA.get('report_time', '')}\n\n"
        f"📍 *Region:* {requested_region}\n\n"
        f"💰 Today: {_format_lacs(today)}\n"
        f"📅 Last Week: {_format_lacs(lw)}\n"
        f"📆 Last Month: {_format_lacs(lm)}\n"
        f"📆 Last Year: {_format_lacs(ly)}\n\n"
        f"📈 vs LW: {_growth(today, lw):+.1f}%\n"
        f"📊 vs LM: {_growth(today, lm):+.1f}%\n"
        f"📊 vs LY: {_growth(today, ly):+.1f}%"
    )

    send_whatsapp_message(sender, reply)



# =========================================================
# 🏪 STORE SALES QUERY
# =========================================================

def send_store_sales_query(sender, store_name):
    user = get_user_access(sender)

    if not user:
        send_whatsapp_message(
            sender,
            "❌ Your mobile number is not mapped for AI MIS access."
        )
        return

    stores = LIVE_SALES_DATA.get("stores", {}) or {}
    matched_store, data = _find_store_snapshot(store_name)

    if not matched_store:
        available = ", ".join(list(stores.keys())[:50])
        send_whatsapp_message(
            sender,
            (
                f"❌ Store not found: {store_name}\n\n"
                f"Try the full store name.\n"
                f"Examples: Byculla sales, AECS Layout sales."
            )
        )
        print("Available store count:", len(stores))
        print("Available sample:", available)
        return

    role = _normalize_text(user.get("role", ""))
    allowed = False

    if role == "ops leader":
        allowed = True

    elif role == "region manager":
        allowed = (
            _normalize_text(user.get("region", ""))
            ==
            _normalize_text(data.get("region", ""))
        )

    elif role == "area manager":
        allowed_stores = user.get("stores", [])
        if not isinstance(allowed_stores, list):
            allowed_stores = [allowed_stores]

        requested_norm = _normalize_store_name(matched_store)
        allowed_norm = {
            _normalize_store_name(x)
            for x in allowed_stores
        }

        allowed = (
            "all" in allowed_norm
            or requested_norm in allowed_norm
        )

        # Fallback using the same fuzzy store matching against the mapped names.
        if not allowed:
            for allowed_name in allowed_stores:
                mapped_name, _ = _find_store_snapshot(allowed_name)
                if mapped_name and mapped_name == matched_store:
                    allowed = True
                    break

    if not allowed:
        send_whatsapp_message(
            sender,
            f"❌ You don't have access to *{matched_store}*."
        )
        return

    today = _safe_float(data.get("today"))
    lw = _safe_float(data.get("lw"))
    lm = _safe_float(data.get("lm"))
    ly = _safe_float(data.get("ly"))

    growth_lw = _safe_float(
        data.get("growth"),
        _growth(today, lw)
    )
    growth_lm = _safe_float(
        data.get("lm_growth"),
        _growth(today, lm)
    )
    growth_ly = _safe_float(
        data.get("ly_growth"),
        _growth(today, ly)
    )

    reply = (
        "🏪 *AI MIS | STORE SALES*\n"
        f"{LIVE_SALES_DATA.get('date', '')}\n"
        f"{LIVE_SALES_DATA.get('report_time', '')}\n\n"
        f"🏪 *{matched_store}*\n"
        f"📍 Region: {data.get('region', 'UNKNOWN')}\n\n"
        f"💰 Today: {_format_thousands(today)}\n"
        f"📅 Last Week: {_format_thousands(lw)}\n"
        f"📆 Last Month: {_format_thousands(lm)}\n"
        f"📆 Last Year: {_format_thousands(ly)}\n\n"
        f"📈 vs LW: {growth_lw:+.1f}%\n"
        f"📊 vs LM: {growth_lm:+.1f}%\n"
        f"📊 vs LY: {growth_ly:+.1f}%"
    )

    send_whatsapp_message(sender, reply)

# =========================================================
# 📚 HISTORICAL ACCESS HELPERS
# =========================================================

def _normalize_query_value(value):
    import re

    value = str(value or "").strip().lower()

    value = re.sub(
        r"[^a-z0-9]+",
        " ",
        value
    )

    return re.sub(
        r"\s+",
        " ",
        value
    ).strip()


def _dimension_matches(
    user_value,
    requested_value
):
    a = _normalize_query_value(
        user_value
    )

    b = _normalize_query_value(
        requested_value
    )

    return (
        a == b
        or a in b
        or b in a
    )


def _historical_user_can_access(
    sender,
    dimension,
    value
):
    user = get_user_access(
        sender
    )

    if not user:
        return False, None

    role = (
        str(
            user.get(
                "role",
                ""
            )
        )
        .strip()
        .lower()
    )

    # -----------------------------------------------------
    # OPS LEADER
    # -----------------------------------------------------

    if role == "ops leader":

        return True, user

    # -----------------------------------------------------
    # REGION MANAGER
    # -----------------------------------------------------

    if role == "region manager":

        if dimension == "region":

            allowed = _dimension_matches(
                user.get(
                    "region",
                    ""
                ),
                value
            )

            return allowed, user

        if dimension == "store":

            # Check current store snapshot first.
            stores = (
                LIVE_SALES_DATA.get(
                    "stores",
                    {}
                )
            )

            for store_name, data in stores.items():

                if _dimension_matches(
                    store_name,
                    value
                ):

                    allowed = _dimension_matches(
                        user.get(
                            "region",
                            ""
                        ),
                        data.get(
                            "region",
                            ""
                        )
                    )

                    return allowed, user

            # Historical file may contain the store
            # even when it isn't in today's snapshot.
            return False, user

        # Brand / overall are not automatically
        # unrestricted for a Region Manager.
        return False, user

    # -----------------------------------------------------
    # AREA MANAGER
    # -----------------------------------------------------

    if role == "area manager":

        if dimension != "store":
            return False, user

        allowed_stores = user.get(
            "stores",
            []
        )

        if not isinstance(
            allowed_stores,
            list
        ):

            allowed_stores = [
                allowed_stores
            ]

        allowed = any(
            _dimension_matches(
                store,
                value
            )
            for store in allowed_stores
        )

        return allowed, user

    return False, user


# =========================================================
# 📚 HISTORICAL SALES RESPONSE
# =========================================================

def send_historical_sales_query(
    sender,
    message
):

    print("=" * 60)
    print("📚 HISTORICAL SALES QUERY")
    print("=" * 60)

    query = classify_historical_query(
        message
    )

    if not query:

        if is_historical_query(message):
            query = {
                "intent": "historical_performance",
                "dimension": None,
                "value": None,
                "months": 6,
            }
        else:
            return False

    intent = query.get(
        "intent"
    )

    dimension = query.get(
        "dimension"
    )

    value = query.get(
        "value"
    )

    months = int(
        query.get(
            "months",
            6
        )
    )

    print(
        "Historical Intent:",
        intent
    )

    print(
        "Dimension:",
        dimension
    )

    print(
        "Value:",
        value
    )

    print(
        "Months:",
        months
    )

    # =====================================================
    # LAST N MONTHS - OVERALL
    # =====================================================

    if (
        intent == "historical_performance"
        and not dimension
    ):

        allowed, user = (
            _historical_user_can_access(
                sender,
                "overall",
                "Overall"
            )
        )

        if not allowed:

            send_whatsapp_message(
                sender,
                (
                    "❌ Historical Overall sales "
                    "are not available for your role."
                )
            )

            return True

        result = (
            get_last_n_months_performance(
                months=months
            )
        )

        result["scope"] = "Overall"

        reply = format_history_summary(
            {
                "scope": "Overall",
                "months": months,
                **result
            }
        )

        send_whatsapp_message(
            sender,
            reply
        )

        return True

    # =====================================================
    # STORE / REGION / BRAND PERFORMANCE
    # =====================================================

    if (
        intent == "historical_performance"
        and dimension
        and value
    ):

        allowed, user = (
            _historical_user_can_access(
                sender,
                dimension,
                value
            )
        )

        if not allowed:

            role = (
                str(
                    user.get(
                        "role",
                        ""
                    )
                )
                if user
                else ""
            )

            if (
                role.lower()
                == "area manager"
            ):

                message_text = (
                    "❌ Area Managers can access "
                    "historical data only for "
                    "their assigned stores."
                )

            elif (
                role.lower()
                == "region manager"
            ):

                message_text = (
                    "❌ Region Managers can access "
                    "historical data only for "
                    "their assigned region/stores."
                )

            else:

                message_text = (
                    "❌ You don't have access "
                    "to this historical data."
                )

            send_whatsapp_message(
                sender,
                message_text
            )

            return True

        if dimension == "store":

            df = get_store_performance(
                value,
                months=months
            )

        elif dimension == "region":

            df = get_region_performance(
                value,
                months=months
            )

        elif dimension == "brand":

            df = get_brand_performance(
                value,
                months=months
            )

        else:

            send_whatsapp_message(
                sender,
                "❌ Unsupported historical dimension."
            )

            return True

        if df.empty:

            send_whatsapp_message(
                sender,
                (
                    f"❌ No historical sales found "
                    f"for *{value}*."
                )
            )

            return True

        total_net = float(
            df["net"].sum()
        )

        avg_net = float(
            df["net"].mean()
        )

        best_row = df.loc[
            df["net"].idxmax()
        ]

        worst_row = df.loc[
            df["net"].idxmin()
        ]

        if len(df) >= 2:

            first_net = float(
                df.iloc[0]["net"]
            )

            last_net = float(
                df.iloc[-1]["net"]
            )

            growth = (
                (
                    last_net
                    -
                    first_net
                )
                /
                first_net
                *
                100
            ) if first_net else 0.0

        else:

            growth = 0.0

        if growth > 5:

            performance = "🚀 Strong Growth"

        elif growth > 0:

            performance = "📈 Growth"

        elif growth < -5:

            performance = "🔻 Decline"

        else:

            performance = "➡️ Stable"

        reply_lines = [

            "📚 *AI MIS | HISTORICAL SALES*",

            "",

            f"📍 *{dimension.title()}: {value}*",

            f"📅 Last {months} Months",

            "",

            "💰 *PERFORMANCE*",

            "",

            f"💵 Total Net Revenue: "
            f"₹{total_net / 100000:.2f}L",

            f"📊 Average Monthly: "
            f"₹{avg_net / 100000:.2f}L",

            f"📈 Period Growth: "
            f"{growth:+.1f}%",

            f"🧠 Performance: "
            f"{performance}",

            "",

            "🏆 *BEST MONTH*",

            f"{best_row['month']}: "
            f"₹{float(best_row['net']) / 100000:.2f}L",

            "",

            "⚠️ *LOWEST MONTH*",

            f"{worst_row['month']}: "
            f"₹{float(worst_row['net']) / 100000:.2f}L",

            "",

            "📅 *MONTHLY TREND*"
        ]

        for _, row in df.iterrows():

            row_growth = float(
                row.get(
                    "growth_pct",
                    0
                )
                or 0
            )

            reply_lines.append(
                f"• {row['month']}: "
                f"₹{float(row['net']) / 100000:.2f}L "
                f"({row_growth:+.1f}%)"
            )

        send_whatsapp_message(
            sender,
            "\n".join(
                reply_lines
            )
        )

        return True

    # =====================================================
    # SEASONALITY
    # =====================================================

    if intent == "seasonality":

        allowed, user = (
            _historical_user_can_access(
                sender,
                "overall",
                "Overall"
            )
        )

        if not allowed:

            send_whatsapp_message(
                sender,
                (
                    "❌ Seasonality analysis "
                    "is not available for your role."
                )
            )

            return True

        seasonal = get_seasonality(
            years=2
        )

        if seasonal.empty:

            send_whatsapp_message(
                sender,
                "❌ No historical seasonality data found."
            )

            return True

        best = seasonal.loc[
            seasonal["average_net"].idxmax()
        ]

        worst = seasonal.loc[
            seasonal["average_net"].idxmin()
        ]

        reply = (
            "📚 *AI MIS | SALES SEASONALITY*\n\n"

            "📊 Based on available historical months\n\n"

            f"🏆 Strongest Month: "
            f"*{best['month']}* "
            f"₹{best['average_net'] / 100000:.2f}L avg\n\n"

            f"⚠️ Weakest Month: "
            f"*{worst['month']}* "
            f"₹{worst['average_net'] / 100000:.2f}L avg\n\n"

            "📅 *MONTHLY PATTERN*\n"
        )

        for _, row in seasonal.iterrows():

            reply += (
                f"• {row['month']}: "
                f"₹{row['average_net'] / 100000:.2f}L\n"
            )

        send_whatsapp_message(
            sender,
            reply
        )

        return True

    # =====================================================
    # STORE / BRAND / REGION RANKING
    # =====================================================

    if intent in [
        "store_ranking",
        "brand_ranking"
    ]:

        dimension = (
            "store"
            if intent == "store_ranking"
            else "brand"
        )

        user = get_user_access(
            sender
        )

        role = (
            str(
                user.get(
                    "role",
                    ""
                )
            )
            .strip()
            .lower()
            if user
            else ""
        )

        if role == "area manager":

            allowed_stores = user.get(
                "stores",
                []
            )

            if not isinstance(
                allowed_stores,
                list
            ):

                allowed_stores = [
                    allowed_stores
                ]

            result = get_best_worst(
                "store",
                months=months,
                top_n=10
            )

            allowed_norm = {
                _normalize_query_value(
                    x
                )
                for x in allowed_stores
            }

            result["best"] = [
                x for x in result["best"]
                if _normalize_query_value(
                    x.get("store")
                ) in allowed_norm
            ]

            result["worst"] = [
                x for x in result["worst"]
                if _normalize_query_value(
                    x.get("store")
                ) in allowed_norm
            ]

        else:

            result = get_best_worst(
                dimension,
                months=months,
                top_n=10
            )

        reply_lines = [
            "📊 *AI MIS | HISTORICAL RANKING*",
            "",
            f"📅 Last {months} Months",
            "",
            "🏆 *TOP PERFORMERS*",
        ]

        for row in result["best"]:

            label = row.get(
                dimension,
                "Unknown"
            )

            reply_lines.append(
                f"• {label}: "
                f"₹{float(row.get('net', 0)) / 100000:.2f}L"
            )

        reply_lines.extend(
            [
                "",
                "⚠️ *LOWEST PERFORMERS*",
            ]
        )

        for row in result["worst"]:

            label = row.get(
                dimension,
                "Unknown"
            )

            reply_lines.append(
                f"• {label}: "
                f"₹{float(row.get('net', 0)) / 100000:.2f}L"
            )

        send_whatsapp_message(
            sender,
            "\n".join(reply_lines)
        )

        return True

    return False

# =========================================================
# 📚 HISTORICAL QUERY DETECTOR
# =========================================================

def is_historical_query(message):

    text = (
        str(message)
        .strip()
        .lower()
    )

    historical_phrases = [
        "last month",
        "last 2 months",
        "last 3 months",
        "last 6 months",
        "last 12 months",
        "last year",
        "historical",
        "history",
        "seasonality",
        "seasonal",
        "historical performance",
        "monthly performance",
        "monthly trend",
        "sales trend",
        "last six months",
        "last twelve months",
    ]

    return any(
        phrase in text
        for phrase in historical_phrases
    )

# =========================================================
# 📊 HANDLE GUIDED MENU REPORT ACTION
# =========================================================

def handle_menu_report_action(sender, action, session):
    print("=" * 60)
    print("📊 MENU REPORT ACTION")
    print("Sender :", sender)
    print("Action :", action)
    print("Session:", session)
    print("=" * 60)

    if not session:
        return

    if action == "today_sales":
        send_role_based_ftd_sales(sender)
        clear_session(sender)
        return

    if action == "last_week_sales":
        send_menu_period_report(sender, session, "last_week")
        clear_session(sender)
        return

    if action == "last_month_sales":
        send_menu_period_report(sender, session, "last_month")
        clear_session(sender)
        return

    if action == "last_year_sales":
        send_menu_period_report(sender, session, "last_year")
        clear_session(sender)
        return

    if action == "historical":
        send_menu_period_report(sender, session, "last_6_months")
        clear_session(sender)
        return

    if action in {
        "generate_overall",
        "generate_brand",
        "generate_region",
        "generate_store",
        "generate_source",
        "generate_ranking",
    }:
        send_menu_period_report(sender, session, session.period)
        clear_session(sender)
        return

    print("⚠️ Unknown menu action:", action)


# =========================================================
# 📊 MENU PERIOD REPORT
# =========================================================

def _menu_scope_values(session):
    if not LIVE_SALES_DATA or not session:
        return None

    analysis = getattr(session, "analysis", None)

    if analysis in {None, "overall"}:
        overall = LIVE_SALES_DATA.get("overall", {}) or {}
        return {
            "scope": "Overall",
            "today": _safe_float(overall.get("net")),
            "lw": _safe_float(overall.get("lw_net")),
            "lm": _safe_float(overall.get("lm_net")),
            "ly": _safe_float(overall.get("ly_net")),
        }

    if analysis == "brand":
        brands = LIVE_SALES_DATA.get("brands", {}) or {}
        requested = _normalize_text(getattr(session, "brand", ""))
        for name, data in brands.items():
            if _normalize_text(name) == requested:
                return {
                    "scope": str(name),
                    "today": _safe_float(data.get("today")),
                    "lw": _safe_float(data.get("lw")),
                    "lm": _safe_float(data.get("lm")),
                    "ly": _safe_float(data.get("ly")),
                }
        return None

    if analysis == "region":
        values = _region_sales_values(getattr(session, "region", ""))
        if not values:
            return None
        return {
            "scope": values.get("region") or getattr(session, "region", ""),
            "today": values.get("today", 0.0),
            "lw": values.get("lw", 0.0),
            "lm": values.get("lm", 0.0),
            "ly": values.get("ly", 0.0),
        }

    if analysis == "store":
        name, data = _find_store_snapshot(getattr(session, "store", ""))
        if not name:
            return None
        return {
            "scope": str(name),
            "today": _safe_float(data.get("today")),
            "lw": _safe_float(data.get("lw")),
            "lm": _safe_float(data.get("lm")),
            "ly": _safe_float(data.get("ly")),
        }

    if analysis == "source":
        sources = LIVE_SALES_DATA.get("sources", {}) or {}
        requested = _normalize_text(getattr(session, "source", ""))
        for name, data in sources.items():
            if _normalize_text(name) == requested:
                return {
                    "scope": str(name),
                    "today": _safe_float(data.get("today")),
                    "lw": _safe_float(data.get("lw")),
                    "lm": _safe_float(data.get("lm")),
                    "ly": _safe_float(data.get("ly")),
                }
        return None

    return None


def send_menu_period_report(sender, session, period):
    print("=" * 60)
    print("📊 MENU PERIOD REPORT")
    print("Sender:", sender)
    print("Period:", period)
    print("Session:", session)
    print("=" * 60)

    if period == "today":
        send_role_based_ftd_sales(sender)
        return

    if period in {"last_week", "last_month", "last_year"}:
        scoped = _menu_scope_values(session)
        if not scoped:
            send_whatsapp_message(sender, "⚠️ Sales data is not available for the selected scope.")
            return

        key = {
            "last_week": "lw",
            "last_month": "lm",
            "last_year": "ly",
        }[period]
        previous = _safe_float(scoped.get(key))
        today = _safe_float(scoped.get("today"))

        label = {
            "last_week": "LAST WEEK",
            "last_month": "LAST MONTH",
            "last_year": "LAST YEAR",
        }[period]
        emoji = "📅" if period == "last_week" else "📆"

        if previous == 0:
            growth_text = "N/A"
            performance = "⚠️ No comparison data"
            previous_text = "No data"
        else:
            growth = _growth(today, previous)
            growth_text = f"{growth:+.1f}%"
            performance = _performance(growth)
            previous_text = _format_lacs(previous)

        reply = (
            f"📊 *AI MIS | {label} SALES*\n"
            f"{LIVE_SALES_DATA.get('date', '')}\n"
            f"{LIVE_SALES_DATA.get('report_time', '')}\n\n"
            f"📍 *{scoped['scope']}*\n\n"
            f"💰 *Today:* {_format_lacs(today)}\n"
            f"{emoji} *{label.title()}:* {previous_text}\n"
            f"📈 *Growth:* {growth_text}\n"
            f"🧠 *Performance:* {performance}"
        )
        send_whatsapp_message(sender, reply)
        return

    if period in {"last_3_months", "last_6_months", "last_12_months"}:
        months = {"last_3_months": 3, "last_6_months": 6, "last_12_months": 12}[period]
        analysis = getattr(session, "analysis", None)
        value = None
        if analysis == "brand":
            value = getattr(session, "brand", None)
        elif analysis == "region":
            value = getattr(session, "region", None)
        elif analysis == "store":
            value = getattr(session, "store", None)

        historical_message = (
            f"{value} last {months} months"
            if value and analysis in {"brand", "region", "store"}
            else f"last {months} months"
        )
        try:
            handled = send_historical_sales_query(sender, historical_message)
            if not handled:
                send_whatsapp_message(sender, "⚠️ Historical report is not available right now.")
        except Exception as e:
            print("❌ Historical menu error:", str(e))
            send_whatsapp_message(sender, f"❌ Error while generating historical report.\n\nDebug: {str(e)}")
        return

    send_whatsapp_message(sender, "⚠️ Report period not supported yet.")


# =========================================================
# 👋 PROCESS MESSAGE
# =========================================================

def process_message(sender, message_text):
    message = " ".join(
        str(message_text or "")
        .strip()
        .lower()
        .split()
    )

    message = re.sub(r"[’'`]", "", message)
    message = re.sub(r"\s+", " ", message).strip()

    debug_user_access(sender)

    print("=" * 60)
    print("🧠 PROCESSING MESSAGE")
    print("Sender     :", sender)
    print("Original   :", message_text)
    print("Normalized :", message)
    print("=" * 60)

    # -----------------------------------------------------
    # GREETINGS
    # -----------------------------------------------------
    if message in {
        "hi", "hello", "hey", "hii", "hiii",
        "good morning", "good afternoon", "good evening"
    }:
        send_main_menu(sender)
        return

    # -----------------------------------------------------
    # HELP
    # -----------------------------------------------------
    if message == "help":
        send_main_menu(sender)
        return

    # -----------------------------------------------------
    # SALES VS LW / LM / LY
    # -----------------------------------------------------
    comparison_patterns = {
        "lw": [
            "sales vs lw",
            "sales vs last week",
            "sales versus last week",
            "today vs last week",
            "today vs lw",
            "last week sales",
            "sales comparison",
            "compare sales",
            "compare sales last week",
            "compare sales with last week",
            "last weeks sales vs this weeks sales",
            "last week sales vs this week sales",
            "sales vs this week and last week",
            "sales for my patch last week",
            "last week sales for my patch",
        ],
        "lm": [
            "sales vs lm",
            "sales vs last month",
            "sales versus last month",
            "today vs last month",
            "today vs lm",
            "last month sales",
        ],
        "ly": [
            "sales vs ly",
            "sales vs last year",
            "sales versus last year",
            "today vs last year",
            "today vs ly",
            "last year sales",
        ],
    }

    for period, commands in comparison_patterns.items():
        if message in commands:
            try:
                if period == "lw":
                    send_sales_vs_lw(sender)
                elif period == "lm":
                    send_sales_vs_lm(sender)
                else:
                    send_sales_vs_ly(sender)
            except Exception as e:
                print("❌ Comparison ERROR:", str(e))
                send_whatsapp_message(
                    sender,
                    f"❌ Error while generating comparison report.\n\nDebug: {str(e)}"
                )
            return

    # Natural comparison: sales + period phrase.
    if "sales" in message and "last week" in message:
        try:
            send_sales_vs_lw(sender)
        except Exception as e:
            send_whatsapp_message(
                sender,
                f"❌ Error while generating Sales vs Last Week report.\n\nDebug: {str(e)}"
            )
        return

    if "sales" in message and "last month" in message:
        try:
            send_sales_vs_lm(sender)
        except Exception as e:
            send_whatsapp_message(
                sender,
                f"❌ Error while generating Sales vs Last Month report.\n\nDebug: {str(e)}"
            )
        return

    if "sales" in message and "last year" in message:
        try:
            send_sales_vs_ly(sender)
        except Exception as e:
            send_whatsapp_message(
                sender,
                f"❌ Error while generating Sales vs Last Year report.\n\nDebug: {str(e)}"
            )
        return

    # =====================================================
    # 📚 HISTORICAL SALES QUERY
    # =====================================================

    if is_historical_query(message):

        historical_intent = classify_historical_query(
            message
        )

        if not historical_intent:
            historical_intent = {
                "intent": "historical_performance",
                "dimension": None,
                "value": None,
                "months": 6,
            }

        print(
            "📚 HISTORICAL QUERY DETECTED:",
            historical_intent
        )

        try:

            handled = send_historical_sales_query(
                sender,
                message
            )

            if handled:
                print("✅ Historical query completed")
                return

        except Exception as e:

            print(
                "❌ HISTORICAL QUERY ERROR:",
                str(e)
            )

            send_whatsapp_message(
                sender,
                (
                    "❌ Error while generating "
                    "historical sales analysis.\n\n"
                    f"Debug: {str(e)}"
                )
            )

            return

    # -----------------------------------------------------
    # STORE QUERY
    # -----------------------------------------------------
    store_query = extract_store_query(message)

    if store_query:
        print("🏪 STORE SALES QUERY DETECTED:", store_query)
        try:
            send_store_sales_query(sender, store_query)
        except Exception as e:
            print("❌ STORE QUERY ERROR:", str(e))
            send_whatsapp_message(
                sender,
                f"❌ Error while generating Store Sales report.\n\nDebug: {str(e)}"
            )
        return

    # Also allow a bare store name if it uniquely matches.
    if message and LIVE_SALES_DATA.get("stores"):
        matched_store, _ = _find_store_snapshot(message)
        if matched_store:
            send_store_sales_query(sender, matched_store)
            return
    
    # -----------------------------------------------------
    # REGION QUERY
    # -----------------------------------------------------
    region_query = extract_region_query(message)

    if region_query:
        print("🌍 REGION QUERY DETECTED:", region_query)
        try:
            send_region_sales_query(sender, region_query)
        except Exception as e:
            print("❌ REGION QUERY ERROR:", str(e))
            send_whatsapp_message(
                sender,
                f"❌ Error while generating Region Sales report.\n\nDebug: {str(e)}"
            )
        return

    # -----------------------------------------------------
    # NORMAL SALES COMMAND
    # -----------------------------------------------------
    sales_commands = {
        "sales",
        "sales today",
        "today sales",
        "today sale",
        "todays sales",
        "todays sale",
        "ftd",
        "ftd sales",
        "sales for today",
        "what is todays sales",
        "what are todays sales",
        "how was sales today",
        "how are sales today",
    }

    if message in sales_commands:
        try:
            send_role_based_ftd_sales(sender)
        except Exception as e:
            print("❌ FTD ERROR:", str(e))
            send_whatsapp_message(
                sender,
                f"❌ Error while generating FTD Sales report.\n\nDebug: {str(e)}"
            )
        return

    # -----------------------------------------------------
    # UNKNOWN
    # -----------------------------------------------------
    send_whatsapp_message(
        sender,
        (
            "🤖 AI MIS received your message:\n\n"
            f"\"{message_text}\"\n\n"
            "Type *help* to see available commands."
        )
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

    if not data:
        print("⚠️ Empty webhook payload")
        return "EVENT_RECEIVED", 200

    if data.get("object") != "whatsapp_business_account":
        print("⚠️ Not WhatsApp Business Account event")
        return "EVENT_RECEIVED", 200

    for entry in data.get("entry", []):
        for change in entry.get("changes", []):
            field = change.get("field")
            value = change.get("value", {}) or {}

            print("Webhook field:", field)

            if field != "messages":
                continue

            messages = value.get("messages", []) or []
            print("Number of messages:", len(messages))

            for incoming in messages:
                message_type = incoming.get("type")
                sender = incoming.get("from")

                print("Message type:", message_type)
                print("Sender:", sender)

                if message_type == "text":
                    message_text = (incoming.get("text", {}) or {}).get("body", "")
                    print("💬 Incoming text:", message_text)

                    if sender and message_text:
                        try:
                            process_message(sender, message_text)
                            print("✅ process_message completed")
                        except Exception as e:
                            print("❌ process_message ERROR:", str(e))
                            send_whatsapp_message(
                                sender,
                                (
                                    "❌ AI MIS encountered an error "
                                    "while processing your request.\n\n"
                                    f"Debug: {str(e)}"
                                )
                            )

                elif message_type == "interactive":
                    interactive = incoming.get("interactive", {}) or {}
                    interactive_type = interactive.get("type")
                    selection_id = None

                    if interactive_type == "list_reply":
                        selection_id = (interactive.get("list_reply", {}) or {}).get("id")
                    elif interactive_type == "button_reply":
                        selection_id = (interactive.get("button_reply", {}) or {}).get("id")

                    print("📱 Interactive type:", interactive_type)
                    print("📱 Selection ID:", selection_id)

                    if sender and selection_id:
                        try:
                            result = handle_menu_selection(
                                sender=sender,
                                selection=selection_id,
                                live_snapshot=LIVE_SALES_DATA,
                            )

                            print("📱 Menu Action:", result.get("action"))

                            next_menu = result.get("next_menu")

                            if next_menu:
                                options = next_menu.get("options", [])
                                if options:
                                    send_whatsapp_interactive(
                                        recipient=sender,
                                        body_text=next_menu.get("text", "Please select an option."),
                                        options=options,
                                        button_text="Select",
                                        section_title="Options",
                                    )
                                else:
                                    send_whatsapp_message(
                                        sender,
                                        next_menu.get("text", "No options available.")
                                    )
                            else:
                                action = result.get("action")
                                if action:
                                    handle_menu_report_action(
                                        sender,
                                        action,
                                        result.get("session"),
                                    )

                        except Exception as e:
                            print("❌ INTERACTIVE MENU ERROR:", str(e))
                            send_whatsapp_message(
                                sender,
                                (
                                    "❌ Error while processing your selection.\n\n"
                                    f"Debug: {str(e)}"
                                )
                            )

                else:
                    print("⚠️ Non-text/interactive message:", message_type)
                    if sender:
                        send_whatsapp_message(
                            sender,
                            "🤖 AI MIS currently supports text messages and guided menus."
                        )

    return "EVENT_RECEIVED", 200


# =========================================================
# 👔 TEST OPS LEADERS
# =========================================================

@app.route("/test-ops-leaders", methods=["GET"])
def test_ops_leaders():
    recipients = []

    for mobile, user in WHATSAPP_USERS.items():
        if _normalize_text(user.get("role", "")) == "ops leader":
            recipient = (
                str(mobile)
                .replace("+", "")
                .replace(" ", "")
                .replace("-", "")
            )
            recipients.append(recipient)

    recipients = list(dict.fromkeys(recipients))

    if not recipients:
        return jsonify({
            "success": False,
            "message": "No Ops Leaders found",
        }), 404

    results = []

    for recipient in recipients:
        try:
            success = send_whatsapp_message(
                recipient,
                (
                    "👔 *AI MIS | OPS LEADER TEST*\n\n"
                    "✅ Your WhatsApp access mapping is working.\n\n"
                    "Role: Ops Leader\n"
                    "Access: Overall"
                )
            )
            results.append({
                "recipient": recipient,
                "success": success,
            })
        except Exception as e:
            results.append({
                "recipient": recipient,
                "success": False,
                "error": str(e),
            })

    return jsonify({
        "success": all(item["success"] for item in results),
        "results": results,
    }), 200


# =========================================================
# 🧪 TEST SEND
# =========================================================

@app.route("/test-send", methods=["GET"])
def test_send():
    recipients = [
        "919750820509",
        "919535075140",
        "918892390985",
        "919620952646",
        "919959347168",
    ]

    results = []

    for recipient in recipients:
        try:
            success = send_whatsapp_message(
                recipient,
                "🤖 AI MIS webhook test message"
            )
            results.append({
                "recipient": recipient,
                "success": success,
            })
        except Exception as e:
            results.append({
                "recipient": recipient,
                "success": False,
                "error": str(e),
            })

    return jsonify({
        "success": all(item["success"] for item in results),
        "results": results,
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
