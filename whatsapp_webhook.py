
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
# 📊 GET CURRENT FTD SALES
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
# 📊 GET BRAND DATA
# =========================================================

def get_brand_data():

    brands = LIVE_SALES_DATA.get(
        "brands",
        {}
    )

    print(
        "Brands available:",
        list(brands.keys())
    )

    return brands


# =========================================================
# 📊 GET SOURCE DATA
# =========================================================

def get_source_data():

    sources = LIVE_SALES_DATA.get(
        "sources",
        {}
    )

    print(
        "Sources available:",
        list(sources.keys())
    )

    return sources

# =========================================================
# 🏪 AREA MANAGER FTD SALES
# =========================================================

def send_area_manager_ftd_sales(
    sender,
    user
):

    print("=" * 60)
    print("🏪 AREA MANAGER FTD SALES")
    print("=" * 60)

    # -----------------------------------------------------
    # USER STORE MAPPING
    # -----------------------------------------------------

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

    allowed_stores = [

        str(store)
        .strip()

        for store in allowed_stores

        if str(store).strip()
    ]

    print(
        "Sender:",
        sender
    )

    print(
        "Role:",
        user.get("role")
    )

    print(
        "Region:",
        user.get("region")
    )

    print(
        "Patch:",
        user.get("patch")
    )

    print(
        "Mapped Stores:",
        allowed_stores
    )

    # -----------------------------------------------------
    # CHECK SNAPSHOT
    # -----------------------------------------------------

    if not LIVE_SALES_DATA:

        print(
            "❌ LIVE_SALES_DATA is empty"
        )

        send_whatsapp_message(
            sender,
            (
                "⚠️ Sales data is not "
                "available right now."
            )
        )

        return

    # -----------------------------------------------------
    # GET STORE SNAPSHOT
    # -----------------------------------------------------

    snapshot_stores = (
        LIVE_SALES_DATA.get(
            "stores",
            {}
        )
    )

    if not snapshot_stores:

        print(
            "❌ Store snapshot is empty"
        )

        send_whatsapp_message(
            sender,
            (
                "⚠️ Store-level sales data "
                "is not available right now."
            )
        )

        return

    # -----------------------------------------------------
    # NORMALIZED STORE LOOKUP
    # -----------------------------------------------------

    normalized_snapshot = {}

    for store_name, store_data in (
        snapshot_stores.items()
    ):

        normalized_name = (
            str(store_name)
            .strip()
            .lower()
        )

        normalized_snapshot[
            normalized_name
        ] = {
            "name":
                store_name,

            "data":
                store_data
        }

    # -----------------------------------------------------
    # MATCH USER STORES
    # -----------------------------------------------------

    matched_stores = {}

    not_found_stores = []

    for allowed_store in allowed_stores:

        normalized_allowed = (
            allowed_store
            .strip()
            .lower()
        )

        # ---------------------------------------------
        # ALL ACCESS
        # ---------------------------------------------

        if normalized_allowed == "all":

            matched_stores = (
                snapshot_stores.copy()
            )

            not_found_stores = []

            break

        # ---------------------------------------------
        # EXACT MATCH
        # ---------------------------------------------

        matched = (
            normalized_snapshot.get(
                normalized_allowed
            )
        )

        if matched:

            matched_stores[
                matched["name"]
            ] = matched["data"]

        else:

            not_found_stores.append(
                allowed_store
            )

    # -----------------------------------------------------
    # DEBUG MATCHING
    # -----------------------------------------------------

    print(
        "Matched Stores:",
        list(
            matched_stores.keys()
        )
    )

    if not_found_stores:

        print(
            "⚠️ Stores not found "
            "in snapshot:",
            not_found_stores
        )

    # -----------------------------------------------------
    # NO MATCHED STORES
    # -----------------------------------------------------

    if not matched_stores:

        send_whatsapp_message(
            sender,
            (
                "⚠️ No sales data found "
                "for your mapped stores."
            )
        )

        return

    # -----------------------------------------------------
    # TOTAL AREA SALES
    # -----------------------------------------------------

    today_total = 0.0
    lw_total = 0.0

    for store_name, store_data in (
        matched_stores.items()
    ):

        today_total += float(
            store_data.get(
                "today",
                0
            )
            or 0
        )

        lw_total += float(
            store_data.get(
                "lw",
                0
            )
            or 0
        )

    area_growth = (

        (
            today_total
            -
            lw_total
        )
        /
        max(
            lw_total,
            1
        )

    ) * 100

    # -----------------------------------------------------
    # PERFORMANCE
    # -----------------------------------------------------

    if area_growth > 5:

        performance = (
            "🚀 Strong Growth"
        )

    elif area_growth > 0:

        performance = (
            "📈 Growth"
        )

    elif area_growth < -5:

        performance = (
            "🔻 Decline"
        )

    else:

        performance = (
            "➡️ Stable"
        )

    # -----------------------------------------------------
    # DATE / TIME
    # -----------------------------------------------------

    report_date = (
        LIVE_SALES_DATA.get(
            "date",
            ""
        )
    )

    report_time = (
        LIVE_SALES_DATA.get(
            "report_time",
            ""
        )
    )

    # -----------------------------------------------------
    # STORE BREAKDOWN
    # -----------------------------------------------------

    store_lines = []

    # Sort by today's sales
    sorted_stores = sorted(

        matched_stores.items(),

        key=lambda item:
            float(
                item[1].get(
                    "today",
                    0
                )
                or 0
            ),

        reverse=True
    )

    for store_name, store_data in (
        sorted_stores
    ):

        today_sales = float(
            store_data.get(
                "today",
                0
            )
            or 0
        )

        lw_sales = float(
            store_data.get(
                "lw",
                0
            )
            or 0
        )

        growth = (

            (
                today_sales
                -
                lw_sales
            )
            /
            max(
                lw_sales,
                1
            )

        ) * 100

        store_lines.append(

            f"• {store_name}: "
            f"₹{today_sales / 1000:.1f}K "
            f"({growth:+.1f}%)"
        )

    # -----------------------------------------------------
    # RESPONSE
    # -----------------------------------------------------

    reply_parts = [

        "🏪 *AI MIS | AREA SALES*",

        f"{report_date}",

        f"{report_time}",

        "",

        f"📍 *Patch:* "
        f"{user.get('patch', '')}",

        f"🏪 *Stores:* "
        f"{len(matched_stores)}",

        "",

        "💰 *NET REVENUE*",

        f"🟢 Today: "
        f"₹{today_total / 100000:.2f}L",

        f"🔵 Last Week: "
        f"₹{lw_total / 100000:.2f}L",

        f"📈 Growth: "
        f"{area_growth:+.1f}%",

        f"🧠 Performance: "
        f"{performance}",

        "",

        "🏪 *STORE PERFORMANCE*",

    ]

    reply_parts.extend(
        store_lines
    )

    reply = "\n".join(
        reply_parts
    )

    # -----------------------------------------------------
    # DEBUG
    # -----------------------------------------------------

    print(
        "Area Today:",
        today_total
    )

    print(
        "Area LW:",
        lw_total
    )

    print(
        "Area Growth:",
        area_growth
    )

    print(
        "Area Manager Reply:"
    )

    print(reply)

    print("=" * 60)

    # -----------------------------------------------------
    # SEND
    # -----------------------------------------------------

    send_whatsapp_message(
        sender,
        reply
    )

# =========================================================
# 📊 FTD SALES RESPONSE
# =========================================================

def send_role_based_ftd_sales(sender):

    print("=" * 60)
    print("📊 FTD SALES REQUEST")
    print("=" * 60)

    sales = get_ftd_sales()

    if not sales:

        send_whatsapp_message(

            sender,

            "⚠️ Sales data is not available right now."
        )

        return

    net = sales["net"]
    txn = sales["txn"]
    aov = sales["aov"]
    discount = sales["discount"]

    date = sales["date"]

    # -----------------------------------------------------
    # BRAND DATA
    # -----------------------------------------------------

    brands = get_brand_data()

    # -----------------------------------------------------
    # BUILD MESSAGE
    # -----------------------------------------------------

    reply = (

        f"📊 *AI MIS | FTD SALES*\n"

        f"{date}\n\n"

        f"💰 Net Revenue: "
        f"₹{net / 100000:.2f}L\n"

        f"🧾 Transactions: "
        f"{int(txn):,}\n"

        f"🧺 AOV: "
        f"₹{int(round(aov)):,}\n"

        f"📉 Discount: "
        f"{abs(discount):.1f}%"
    )

    # -----------------------------------------------------
    # BRAND CONTRIBUTION
    # -----------------------------------------------------

    if brands:

        reply += (
            "\n\n🏪 *Brand Contribution*"
        )

        brand_values = {}

        for brand_name, brand_data in brands.items():

            today_value = float(
                brand_data.get(
                    "today",
                    0
                )
            )

            brand_values[
                brand_name
            ] = today_value

        total_brand = sum(
            brand_values.values()
        )

        for brand_name, value in brand_values.items():

            contribution = (

                value
                /
                max(total_brand, 1)
                *
                100

            )

            reply += (

                f"\n• {brand_name}: "
                f"{contribution:.0f}%"
            )

    # -----------------------------------------------------
    # DEBUG
    # -----------------------------------------------------

    print(
        "WhatsApp FTD Reply:"
    )

    print(reply)

    print("=" * 60)

    # -----------------------------------------------------
    # SEND
    # -----------------------------------------------------

    send_whatsapp_message(
        sender,
        reply
    )

# =========================================================
# 📊 FTD SALES — OVERALL / OPS LEADER
# =========================================================

def send_ftd_sales(sender):

    print("=" * 60)
    print("📊 FTD SALES REQUEST")
    print("=" * 60)

    try:

        # -------------------------------------------------
        # GET SNAPSHOT DATA
        # -------------------------------------------------

        if not LIVE_SALES_DATA:

            print(
                "❌ LIVE_SALES_DATA is empty"
            )

            send_whatsapp_message(
                sender,
                "⚠️ Sales data is not available right now."
            )

            return

        overall = LIVE_SALES_DATA.get(
            "overall",
            {}
        )

        brands = LIVE_SALES_DATA.get(
            "brands",
            {}
        )

        # -------------------------------------------------
        # OVERALL VALUES
        # -------------------------------------------------

        net = float(
            overall.get(
                "net",
                0
            )
            or 0
        )

        txn = float(
            overall.get(
                "txn",
                0
            )
            or 0
        )

        aov = float(
            overall.get(
                "aov",
                0
            )
            or 0
        )

        discount = float(
            overall.get(
                "discount",
                0
            )
            or 0
        )

        report_date = LIVE_SALES_DATA.get(
            "date",
            ""
        )

        report_time = LIVE_SALES_DATA.get(
            "report_time",
            ""
        )

        # -------------------------------------------------
        # BRAND CONTRIBUTION
        # -------------------------------------------------

        brand_values = {}

        for brand_name, brand_data in brands.items():

            brand_values[
                str(brand_name)
            ] = float(
                brand_data.get(
                    "today",
                    0
                )
                or 0
            )

        total_brand = sum(
            brand_values.values()
        )

        # -------------------------------------------------
        # RESPONSE
        # -------------------------------------------------

        reply_lines = [

            "📊 *AI MIS | FTD SALES*",

            f"{report_date}",

            "",

            f"💰 Net Revenue: "
            f"₹{net / 100000:.2f}L",

            f"🧾 Transactions: "
            f"{int(round(txn)):,}",

            f"🧺 AOV: "
            f"₹{int(round(aov)):,}",

            f"📉 Discount: "
            f"{abs(discount):.1f}%"

        ]

        # -------------------------------------------------
        # BRAND CONTRIBUTION
        # -------------------------------------------------

        if brand_values:

            reply_lines.extend([
                "",
                "🏪 *Brand Contribution*"
            ])

            # Sort largest first
            sorted_brands = sorted(
                brand_values.items(),
                key=lambda x: x[1],
                reverse=True
            )

            for brand_name, revenue in sorted_brands:

                contribution = (
                    revenue
                    /
                    max(
                        total_brand,
                        1
                    )
                ) * 100

                reply_lines.append(
                    f"• {brand_name}: "
                    f"{contribution:.0f}%"
                )

        reply = "\n".join(
            reply_lines
        )

        # -------------------------------------------------
        # DEBUG
        # -------------------------------------------------

        print(
            "Report Date:",
            report_date
        )

        print(
            "Report Time:",
            report_time
        )

        print(
            "Net:",
            net
        )

        print(
            "Transactions:",
            txn
        )

        print(
            "AOV:",
            aov
        )

        print(
            "Discount:",
            discount
        )

        print(
            "Brands:",
            list(
                brand_values.keys()
            )
        )

        print(
            "WhatsApp FTD Reply:"
        )

        print(reply)

        print("=" * 60)

        # -------------------------------------------------
        # SEND
        # -------------------------------------------------

        send_whatsapp_message(
            sender,
            reply
        )

    except Exception as e:

        print(
            "❌ FTD SALES ERROR:",
            str(e)
        )

        send_whatsapp_message(
            sender,
            (
                "❌ Error while generating "
                "FTD Sales report.\n\n"
                f"Debug: {str(e)}"
            )
        )

# =========================================================
# 📊 SALES VS LAST WEEK
# =========================================================

def send_sales_vs_lw(sender):

    print("=" * 60)
    print("📊 SALES VS LAST WEEK")
    print("=" * 60)

    # =====================================================
    # GET USER ACCESS
    # =====================================================

    user = get_user_access(
        sender
    )

    if not user:

        print(
            "❌ User not mapped:",
            sender
        )

        send_whatsapp_message(
            sender,
            (
                "❌ Your mobile number is "
                "not mapped for AI MIS access."
            )
        )

        return

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

    print(
        "Sender:",
        sender
    )

    print(
        "Role:",
        user.get("role")
    )

    print(
        "Region:",
        user.get("region")
    )

    print(
        "Patch:",
        user.get("patch")
    )

    # =====================================================
    # 👔 OPS LEADER
    # =====================================================

    if role == "ops leader":

        print(
            "👔 Ops Leader → Overall Sales vs LW"
        )

        sales = get_sales_vs_lw()

        today_net = float(
            sales.get(
                "today_net",
                0
            )
            or 0
        )

        lw_net = float(
            sales.get(
                "lw_net",
                0
            )
            or 0
        )

        growth = float(
            sales.get(
                "growth",
                0
            )
            or 0
        )

        if growth > 5:

            performance = (
                "🚀 Strong Growth"
            )

        elif growth > 0:

            performance = (
                "📈 Growth"
            )

        elif growth < -5:

            performance = (
                "🔻 Decline"
            )

        else:

            performance = (
                "➡️ Stable"
            )

        report_date = (
            LIVE_SALES_DATA.get(
                "date",
                ""
            )
        )

        reply = (

            f"📊 *AI MIS | SALES vs LW*\n"
            f"{report_date}\n\n"

            f"💰 Today: "
            f"₹{today_net / 100000:.2f}L\n"

            f"📊 Last Week: "
            f"₹{lw_net / 100000:.2f}L\n"

            f"📈 Growth: "
            f"{growth:+.1f}%\n\n"

            f"🧠 Performance: "
            f"{performance}"
        )

        print(reply)

        send_whatsapp_message(
            sender,
            reply
        )

        return

    # =====================================================
    # 🌍 REGION MANAGER
    # =====================================================

    if role == "region manager":

        region_name = (
            str(
                user.get(
                    "region",
                    ""
                )
            )
            .strip()
        )

        print(
            "🌍 Region Manager →",
            region_name
        )

        # -------------------------------------------------
        # CHECK SNAPSHOT
        # -------------------------------------------------

        if not LIVE_SALES_DATA:

            print(
                "❌ LIVE_SALES_DATA is empty"
            )

            send_whatsapp_message(
                sender,
                (
                    "⚠️ Sales data is not "
                    "available right now."
                )
            )

            return

        # -------------------------------------------------
        # FIND REGION
        # -------------------------------------------------

        snapshot_region = (
            find_region_snapshot_key(
                region_name
            )
        )

        if not snapshot_region:

            available_regions = (
                ", ".join(
                    LIVE_SALES_DATA.get(
                        "regions",
                        {}
                    ).keys()
                )
            )

            print(
                "❌ Region not found:",
                region_name
            )

            send_whatsapp_message(
                sender,
                (
                    "⚠️ Region data is not "
                    "available for your mapped region.\n\n"
                    f"Mapped Region: {region_name}\n"
                    f"Available Regions: {available_regions}"
                )
            )

            return

        # -------------------------------------------------
        # GET REGION DATA
        # -------------------------------------------------

        region_data = (
            LIVE_SALES_DATA
            .get(
                "regions",
                {}
            )
            .get(
                snapshot_region,
                {}
            )
        )

        today_net = float(
            region_data.get(
                "today",
                0
            )
            or 0
        )

        lw_net = float(
            region_data.get(
                "lw",
                0
            )
            or 0
        )

        growth = float(
            region_data.get(
                "growth",
                0
            )
            or 0
        )

        # -------------------------------------------------
        # PERFORMANCE
        # -------------------------------------------------

        if growth > 5:

            performance = (
                "🚀 Strong Growth"
            )

        elif growth > 0:

            performance = (
                "📈 Growth"
            )

        elif growth < -5:

            performance = (
                "🔻 Decline"
            )

        else:

            performance = (
                "➡️ Stable"
            )

        if growth > 0:

            growth_icon = "📈"

        elif growth < 0:

            growth_icon = "🔻"

        else:

            growth_icon = "➡️"

        # -------------------------------------------------
        # DATE / TIME
        # -------------------------------------------------

        report_date = (
            LIVE_SALES_DATA.get(
                "date",
                ""
            )
        )

        report_time = (
            LIVE_SALES_DATA.get(
                "report_time",
                ""
            )
        )

        # -------------------------------------------------
        # RESPONSE
        # -------------------------------------------------

        reply = (

            f"🌍 *AI MIS | REGION SALES vs LW*\n"
            f"{report_date}\n"
            f"{report_time}\n\n"

            f"📍 *Region:* "
            f"{snapshot_region}\n\n"

            f"💰 *NET REVENUE*\n"

            f"🟢 Today: "
            f"₹{today_net / 100000:.2f}L\n"

            f"🔵 Last Week: "
            f"₹{lw_net / 100000:.2f}L\n"

            f"{growth_icon} Growth: "
            f"{growth:+.1f}%\n\n"

            f"🧠 Performance: "
            f"{performance}"
        )

        print(
            "🌍 Region Snapshot:"
        )

        print(
            "Region:",
            snapshot_region
        )

        print(
            "Today Net:",
            today_net
        )

        print(
            "LW Net:",
            lw_net
        )

        print(
            "Growth:",
            growth
        )

        print(
            "Region Manager Sales vs LW Reply:"
        )

        print(reply)

        print("=" * 60)

        send_whatsapp_message(
            sender,
            reply
        )

        return

    # -----------------------------------------------------
    # AREA MANAGER
    # -----------------------------------------------------
    
    if role == "area manager":
    
        print(
            "🏪 Area Manager → "
            "Patch Store Sales"
        )
    
        try:
    
            send_area_manager_ftd_sales(
                sender,
                user
            )
    
            print(
                "✅ send_area_manager_ftd_sales() completed"
            )
    
        except Exception as e:
    
            print(
                "❌ send_area_manager_ftd_sales() ERROR:",
                str(e)
            )
    
            send_whatsapp_message(
                sender,
                (
                    "❌ Error while generating "
                    "Area Manager Sales report.\n\n"
                    f"Debug: {str(e)}"
                )
            )
    
        return

    # =====================================================
    # ❌ UNKNOWN ROLE
    # =====================================================

    print(
        "❌ Unknown role:",
        user.get("role")
    )

    send_whatsapp_message(
        sender,
        "❌ Your AI MIS role is not configured."
    )


# =========================================================
# 🔐 DEBUG USER ACCESS
# =========================================================

def debug_user_access(sender):

    print("=" * 60)
    print("🔐 WHATSAPP ACCESS DEBUG")
    print("=" * 60)

    user = get_user_access(
        sender
    )

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
# 🌍 FIND REGION IN WHATSAPP SNAPSHOT
# =========================================================

def find_region_snapshot_key(
    requested_region
):

    regions = (
        LIVE_SALES_DATA.get(
            "regions",
            {}
        )
    )

    requested = (
        str(
            requested_region
        )
        .strip()
        .lower()
    )

    # -----------------------------------------------------
    # DIRECT MATCH
    # -----------------------------------------------------

    for region_name in regions.keys():

        normalized = (
            str(
                region_name
            )
            .strip()
            .lower()
        )

        if normalized == requested:

            return region_name

    # -----------------------------------------------------
    # KERALA / KERELA
    # -----------------------------------------------------

    if requested in [
        "kerala",
        "kerela"
    ]:

        for region_name in regions.keys():

            normalized = (
                str(
                    region_name
                )
                .strip()
                .lower()
            )

            if normalized in [
                "kerala",
                "kerela"
            ]:

                return region_name

    return None


# =========================================================
# 🌍 REGION MANAGER FTD SALES
# =========================================================

def send_region_ftd_sales(
    sender,
    user
):

    print("=" * 60)
    print("🌍 REGION MANAGER FTD SALES")
    print("=" * 60)

    region_name = (
        str(
            user.get(
                "region",
                ""
            )
        )
        .strip()
    )

    print(
        "Sender :",
        sender
    )

    print(
        "Role   :",
        user.get("role")
    )

    print(
        "Region requested:",
        region_name
    )

    # -----------------------------------------------------
    # CHECK SNAPSHOT
    # -----------------------------------------------------

    if not LIVE_SALES_DATA:

        print(
            "❌ LIVE_SALES_DATA is empty"
        )

        send_whatsapp_message(
            sender,
            (
                "⚠️ Sales data is "
                "not available right now."
            )
        )

        return

    # -----------------------------------------------------
    # FIND REGION
    # -----------------------------------------------------

    snapshot_region = (
        find_region_snapshot_key(
            region_name
        )
    )

    if not snapshot_region:

        available_regions = (
            ", ".join(
                LIVE_SALES_DATA.get(
                    "regions",
                    {}
                ).keys()
            )
        )

        print(
            "❌ Region not found:",
            region_name
        )

        send_whatsapp_message(
            sender,
            (
                "⚠️ Region data is not "
                "available for your mapped region.\n\n"
                f"Mapped Region: {region_name}\n"
                f"Available Regions: {available_regions}"
            )
        )

        return

    # -----------------------------------------------------
    # REGION DATA
    # -----------------------------------------------------

    region_data = (
        LIVE_SALES_DATA
        .get(
            "regions",
            {}
        )
        .get(
            snapshot_region,
            {}
        )
    )

    today_net = float(
        region_data.get(
            "today",
            0
        )
        or 0
    )

    lw_net = float(
        region_data.get(
            "lw",
            0
        )
        or 0
    )

    growth = float(
        region_data.get(
            "growth",
            0
        )
        or 0
    )

    # -----------------------------------------------------
    # GROWTH ICON
    # -----------------------------------------------------

    if growth > 0:

        growth_icon = "📈"

    elif growth < 0:

        growth_icon = "🔻"

    else:

        growth_icon = "➡️"

    report_date = (
        LIVE_SALES_DATA.get(
            "date",
            ""
        )
    )

    report_time = (
        LIVE_SALES_DATA.get(
            "report_time",
            ""
        )
    )

    # -----------------------------------------------------
    # RESPONSE
    # -----------------------------------------------------

    reply = (

        f"🌍 *AI MIS | REGION SALES*\n"
        f"{report_date}\n"
        f"{report_time}\n\n"

        f"📍 *Region:* "
        f"{snapshot_region}\n\n"

        f"💰 *NET REVENUE*\n"

        f"🟢 Today: "
        f"₹{today_net / 100000:.2f}L\n"

        f"🔵 Last Week: "
        f"₹{lw_net / 100000:.2f}L\n"

        f"{growth_icon} Growth: "
        f"{growth:+.1f}%"
    )

    print(
        "🌍 Region Snapshot:"
    )

    print(
        "Region:",
        snapshot_region
    )

    print(
        "Today Net:",
        today_net
    )

    print(
        "LW Net:",
        lw_net
    )

    print(
        "Growth:",
        growth
    )

    print(
        "Region Manager Reply:"
    )

    print(reply)

    print("=" * 60)

    send_whatsapp_message(
        sender,
        reply
    )


# =========================================================
# 🔐 ROLE BASED FTD SALES
# =========================================================

def send_role_based_ftd_sales(
    sender
):

    print("=" * 60)
    print("🔐 ROLE BASED FTD SALES")
    print("=" * 60)

    user = get_user_access(
        sender
    )

    if not user:

        print(
            "❌ User not mapped:",
            sender
        )

        send_whatsapp_message(
            sender,
            (
                "❌ Your mobile number is "
                "not mapped for AI MIS access."
            )
        )

        return

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

    print(
        "Sender:",
        sender
    )

    print(
        "Role:",
        user.get("role")
    )

    print(
        "Region:",
        user.get("region")
    )

    print(
        "Patch:",
        user.get("patch")
    )

    print(
        "Stores:",
        user.get("stores")
    )

    # -----------------------------------------------------
    # OPS LEADER
    # -----------------------------------------------------

    if role == "ops leader":

        print(
            "👔 Ops Leader → Overall Sales"
        )

        send_ftd_sales(
            sender
        )

        return

    # -----------------------------------------------------
    # REGION MANAGER
    # -----------------------------------------------------

    if role == "region manager":

        print(
            "🌍 Region Manager → Region Sales"
        )

        send_region_ftd_sales(
            sender,
            user
        )

        return

    # -----------------------------------------------------
    # AREA MANAGER
    # -----------------------------------------------------

    if role == "area manager":

        print(
            "🏪 Area Manager → "
            "Store filtering not enabled yet"
        )

        send_whatsapp_message(
            sender,
            (
                "⚠️ Area Manager store-level "
                "sales access is being configured next."
            )
        )

        return

    # -----------------------------------------------------
    # UNKNOWN ROLE
    # -----------------------------------------------------

    print(
        "❌ Unknown role:",
        role
    )

    send_whatsapp_message(
        sender,
        "❌ Your AI MIS role is not configured."
    )


# =========================================================
# 👋 PROCESS MESSAGE
# =========================================================

def process_message(
    sender,
    message_text
):

    message = " ".join(
        message_text
        .strip()
        .lower()
        .split()
    )

    # =====================================================
    # 🔐 USER ACCESS DEBUG
    # =====================================================

    debug_user_access(
        sender
    )

    print("=" * 60)
    print("🧠 PROCESSING MESSAGE")
    print("Sender     :", sender)
    print("Original   :", message_text)
    print("Normalized :", message)
    print("=" * 60)

    # =====================================================
    # 👋 GREETING
    # =====================================================

    if message in [

        "hi",
        "hello",
        "hey",
        "hii",
        "hiii",
        "good morning",
        "good afternoon",
        "good evening"

    ]:

        reply = (

            "👋 Hi! Welcome to "
            "AI MIS WhatsApp.\n\n"

            "You can try:\n\n"

            "📊 *sales*\n"

            "📈 *sales vs lw*\n"

            "❓ *help*"
        )

        send_whatsapp_message(
            sender,
            reply
        )

        return

    # =====================================================
    # ❓ HELP
    # =====================================================

    if message == "help":

        reply = (

            "🤖 *AI MIS WhatsApp*\n\n"

            "Available commands:\n\n"

            "📊 *sales*\n"

            "📊 *sales today*\n"

            "📈 *sales vs lw*\n"

            "❓ *help*"
        )

        send_whatsapp_message(
            sender,
            reply
        )

        return

    # =====================================================
    # 📊 SALES
    # =====================================================

    sales_keywords = [

        "sales",
        "sales today",
        "today sales",
        "today's sales",
        "todays sales",
        "ftd",
        "ftd sales",
        "sales for today",
        "today's sale",
        "todays sale",

        "what is today's sales",
        "what is todays sales",
        "what are today's sales",
        "what are todays sales",

        "how was the sales today",
        "how was sales today",
        "how are sales today",
        "how was the sale today",
        "how are the sales today",
        "how is the sales today"

    ]

    # =====================================================
    # 📊 SALES
    # =====================================================
    
    if message in sales_keywords:
    
        print(
            "📊 FTD SALES COMMAND DETECTED"
        )
    
        try:
    
            send_role_based_ftd_sales(
                sender
            )
    
            print(
                "✅ send_role_based_ftd_sales() completed"
            )
    
        except Exception as e:
    
            print(
                "❌ send_role_based_ftd_sales() ERROR:",
                str(e)
            )
    
            send_whatsapp_message(
                sender,
                (
                    "❌ Error while generating "
                    "FTD Sales report.\n\n"
                    f"Debug: {str(e)}"
                )
            )
    
        return

    # =====================================================
    # 📈 SALES VS LW
    # =====================================================

    sales_lw_keywords = [

        "sales vs lw",
        "sales vs last week",
        "sales versus last week",
        "today vs last week",
        "today vs lw",
        "last week sales",
        "compare sales",
        "sales comparison",
        "compare sales last week",
        "compare sales with last week",
        "how are sales vs last week",
        "how is sales vs last week",
        "how was sales vs last week"

    ]

    if message in sales_lw_keywords:

        print(
            "📈 SALES VS LW COMMAND DETECTED"
        )
    
        try:
    
            send_sales_vs_lw(
                sender
            )
    
            print(
                "✅ send_sales_vs_lw() completed"
            )
    
        except Exception as e:
    
            print(
                "❌ send_sales_vs_lw() ERROR:",
                str(e)
            )
    
            send_whatsapp_message(
                sender,
                (
                    "❌ Error while generating "
                    "Sales vs Last Week report.\n\n"
                    f"Debug: {str(e)}"
                )
            )
    
        return

    # =====================================================
    # 📈 NATURAL SALES VS LW
    # =====================================================
    
    if (
        "sales" in message
        and (
            "last week" in message
            or "lw" in message
        )
    ):
    
        print(
            "📈 NATURAL SALES VS LW "
            "QUESTION DETECTED"
        )
    
        try:
    
            send_sales_vs_lw(
                sender
            )
    
            print(
                "✅ Natural Sales vs LW completed"
            )
    
        except Exception as e:
    
            print(
                "❌ Natural Sales vs LW ERROR:",
                str(e)
            )
    
            send_whatsapp_message(
                sender,
                (
                    "❌ Error while generating "
                    "Sales vs Last Week report."
                )
            )
    
        return

    # =====================================================
    # ❌ UNKNOWN MESSAGE
    # =====================================================

    reply = (

        "🤖 AI MIS received your message:\n\n"

        f"\"{message_text}\"\n\n"

        "Type *help* to see "
        "available commands."
    )

    send_whatsapp_message(
        sender,
        reply
    )


# =========================================================
# 📩 META WEBHOOK RECEIVER
# =========================================================

@app.route(
    "/webhook",
    methods=["POST"]
)
def webhook():

    data = request.get_json(
        silent=True
    )

    print("=" * 60)
    print("📩 WHATSAPP WEBHOOK RECEIVED")
    print("=" * 60)

    print(
        json.dumps(
            data,
            indent=2,
            ensure_ascii=False
        )
    )

    print("=" * 60)

    if not data:

        print(
            "⚠️ Empty webhook payload"
        )

        return (
            "EVENT_RECEIVED",
            200
        )

    if data.get(
        "object"
    ) != "whatsapp_business_account":

        print(
            "⚠️ Not a WhatsApp Business "
            "Account event"
        )

        return (
            "EVENT_RECEIVED",
            200
        )

    for entry in data.get(
        "entry",
        []
    ):

        for change in entry.get(
            "changes",
            []
        ):

            field = change.get(
                "field"
            )

            value = change.get(
                "value",
                {}
            )

            print(
                "Webhook field:",
                field
            )

            # =================================================
            # MESSAGE EVENT
            # =================================================

            if field == "messages":

                messages = value.get(
                    "messages",
                    []
                )

                print(
                    "Number of messages:",
                    len(messages)
                )

                for message_data in messages:

                    message_type = (
                        message_data.get(
                            "type"
                        )
                    )

                    sender = (
                        message_data.get(
                            "from"
                        )
                    )

                    print(
                        "Message type:",
                        message_type
                    )

                    print(
                        "Sender:",
                        sender
                    )

                    # -----------------------------------------
                    # TEXT
                    # -----------------------------------------

                    if message_type == "text":

                        text_data = (
                            message_data.get(
                                "text",
                                {}
                            )
                        )

                        message_text = (
                            text_data.get(
                                "body",
                                ""
                            )
                        )

                        print(
                            "💬 Incoming text:",
                            message_text
                        )

                        if (
                            sender
                            and
                            message_text
                        ):

                            try:

                                process_message(
                                    sender,
                                    message_text
                                )

                                print(
                                    "✅ process_message "
                                    "completed"
                                )

                            except Exception as e:

                                print(
                                    "❌ process_message ERROR:",
                                    str(e)
                                )

                                send_whatsapp_message(
                                    sender,
                                    (
                                        "❌ AI MIS encountered "
                                        "an error while processing "
                                        "your request."
                                    )
                                )

                    # -----------------------------------------
                    # NON-TEXT
                    # -----------------------------------------

                    else:

                        print(
                            "⚠️ Non-text message:",
                            message_type
                        )

                        if sender:

                            send_whatsapp_message(

                                sender,

                                (
                                    "🤖 AI MIS currently "
                                    "supports text messages only."
                                )
                            )

            # =================================================
            # STATUS / OTHER EVENTS
            # =================================================

            else:

                print(
                    "ℹ️ Other webhook event:",
                    field
                )

    return (
        "EVENT_RECEIVED",
        200
    )


# =========================================================
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

