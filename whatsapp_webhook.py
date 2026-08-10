
from flask import Flask, request
import os
import requests
import json

# =========================================================
# FLASK APP
# =========================================================

app = Flask(__name__)

# =========================================================
# ENVIRONMENT VARIABLES
# =========================================================

VERIFY_TOKEN = os.environ.get("WHATSAPP_VERIFY_TOKEN")
PHONE_NUMBER_ID = os.environ.get("WHATSAPP_PHONE_NUMBER_ID")
ACCESS_TOKEN = os.environ.get("WHATSAPP_ACCESS_TOKEN")

GRAPH_API_VERSION = "v23.0"

# =========================================================
# BASIC CHECK
# =========================================================

print("=" * 60)
print("🚀 AI MIS WHATSAPP WEBHOOK")
print("=" * 60)

print("PHONE_NUMBER_ID exists :", bool(PHONE_NUMBER_ID))
print("ACCESS_TOKEN exists    :", bool(ACCESS_TOKEN))
print("VERIFY_TOKEN exists    :", bool(VERIFY_TOKEN))

print("=" * 60)


# =========================================================
# HOME
# =========================================================

@app.route("/", methods=["GET"])
def home():
    return "AI MIS WhatsApp Webhook is running", 200


# =========================================================
# META WEBHOOK VERIFICATION
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
# SEND WHATSAPP TEXT MESSAGE
# =========================================================

def send_whatsapp_message(recipient, message):

    if not PHONE_NUMBER_ID:
        print("❌ WHATSAPP_PHONE_NUMBER_ID is missing")
        return False

    if not ACCESS_TOKEN:
        print("❌ WHATSAPP_ACCESS_TOKEN is missing")
        return False

    url = (
        f"https://graph.facebook.com/"
        f"{GRAPH_API_VERSION}/"
        f"{PHONE_NUMBER_ID}/messages"
    )

    headers = {
        "Authorization": f"Bearer {ACCESS_TOKEN}",
        "Content-Type": "application/json"
    }

    payload = {
        "messaging_product": "whatsapp",
        "to": recipient,
        "type": "text",
        "text": {
            "preview_url": False,
            "body": message
        }
    }

    print("=" * 60)
    print("📤 SENDING WHATSAPP MESSAGE")
    print("To:", recipient)
    print("Message:", message)
    print("=" * 60)

    try:

        response = requests.post(
            url,
            headers=headers,
            json=payload,
            timeout=30
        )

        print("Meta Status   :", response.status_code)
        print("Meta Response :", response.text)
        print("=" * 60)

        if response.ok:
            print("✅ WhatsApp message sent")
            return True

        print("❌ WhatsApp message failed")

        return False

    except Exception as e:

        print("❌ WhatsApp API error:", str(e))

        return False


# =========================================================
# FTD SALES RESPONSE
# =========================================================

def send_ftd_sales(sender):

    reply = (
        "📊 *AI MIS | FTD SALES*\n"
        "10-Aug-26\n\n"
    
        "💰 Net Revenue: ₹XX.XXL\n"
        "🧾 Transactions: X,XXX\n"
        "🛒 Qty Sold: XX.XK\n"
        "🧺 AOV: ₹XXX\n"
        "📉 Discount: XX%\n\n"
    
        "🏪 *Brand Contribution*\n"
        "🍶 Frozen Bottle: XX%\n"
        "🥤 Madno: XX%\n"
        "🧋 Boba Bar: XX%"
    )
    send_whatsapp_message(sender, reply)


# =========================================================
# PROCESS INCOMING MESSAGE
# =========================================================

def process_message(sender, message_text):

    # Normalize message
    message = " ".join(
        message_text.strip().lower().split()
    )

    print("=" * 60)
    print("🧠 PROCESSING MESSAGE")
    print("Sender :", sender)
    print("Original:", message_text)
    print("Normalized:", message)
    print("=" * 60)

    # =====================================================
    # GREETING
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
            "👋 Hi! Welcome to AI MIS WhatsApp.\n\n"
            "✅ WhatsApp connection is working successfully.\n\n"
            "You can try:\n"
            "• sales today\n"
            "• sales\n"
            "• help"
        )

        send_whatsapp_message(sender, reply)

        return

    # =====================================================
    # HELP
    # =====================================================

    if message == "help":

        reply = (
            "🤖 AI MIS WhatsApp\n\n"
            "Available commands:\n\n"
            "📊 sales today\n"
            "📊 sales\n"
            "❓ help"
        )

        send_whatsapp_message(sender, reply)

        return

    # =====================================================
    # FTD SALES
    # =====================================================

    sales_keywords = [
        "sales",
        "sales today",
        "today sales",
        "how was the sales today",
        "how was sales today",
        "how are sales today",
        "how was the sale today",
        "how are the sales today",
        "how is the sales today",
        "what is today's sales",
        "what is todays sales",
        "what are today's sales",
        "what are todays sales",
        "today's sales",
        "todays sales",
        "ftd",
        "ftd sales",
        "sales for today",
        "today's sale",
        "todays sale"
    ]

    # Exact keyword match
    if message in sales_keywords:

        print("📊 FTD SALES COMMAND DETECTED")

        send_ftd_sales(sender)

        return

    # =====================================================
    # NATURAL SALES QUESTIONS
    # =====================================================

    # If the message contains "sales" + "today"
    if "sales" in message and "today" in message:

        print("📊 NATURAL FTD SALES QUESTION DETECTED")

        send_ftd_sales(sender)

        return

    # =====================================================
    # UNKNOWN MESSAGE
    # =====================================================

    reply = (
        "🤖 AI MIS received your message:\n\n"
        f"\"{message_text}\"\n\n"
        "Type *help* to see available commands."
    )

    send_whatsapp_message(sender, reply)


# =========================================================
# META WEBHOOK RECEIVER
# =========================================================

@app.route("/webhook", methods=["POST"])
def webhook():

    data = request.get_json(silent=True)

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

    # =====================================================
    # SAFETY CHECK
    # =====================================================

    if not data:

        print("⚠️ Empty webhook payload")

        return "EVENT_RECEIVED", 200

    # =====================================================
    # META OBJECT CHECK
    # =====================================================

    if data.get("object") != "whatsapp_business_account":

        print("⚠️ Not a WhatsApp Business Account event")

        return "EVENT_RECEIVED", 200

    # =====================================================
    # ENTRY
    # =====================================================

    for entry in data.get("entry", []):

        for change in entry.get("changes", []):

            field = change.get("field")

            value = change.get("value", {})

            print("Webhook field:", field)

            # =================================================
            # MESSAGE EVENT
            # =================================================

            if field == "messages":

                messages = value.get("messages", [])

                print(
                    "Number of messages:",
                    len(messages)
                )

                for message in messages:

                    message_type = message.get("type")

                    sender = message.get("from")

                    print(
                        "Message type:",
                        message_type
                    )

                    print(
                        "Sender:",
                        sender
                    )

                    # =========================================
                    # TEXT MESSAGE
                    # =========================================

                    if message_type == "text":

                        text_data = message.get(
                            "text",
                            {}
                        )

                        message_text = text_data.get(
                            "body",
                            ""
                        )

                        print(
                            "💬 Incoming text:",
                            message_text
                        )

                        if sender and message_text:

                            process_message(
                                sender,
                                message_text
                            )

                    # =========================================
                    # NON-TEXT MESSAGE
                    # =========================================

                    else:

                        print(
                            "⚠️ Non-text message received:",
                            message_type
                        )

                        if sender:

                            send_whatsapp_message(
                                sender,
                                "🤖 AI MIS currently supports text messages only."
                            )

            # =================================================
            # OTHER EVENTS
            # =================================================

            else:

                print(
                    "ℹ️ Other webhook event:",
                    field
                )

    # =====================================================
    # ALWAYS RETURN 200 TO META
    # =====================================================

    return "EVENT_RECEIVED", 200


# =========================================================
# TEST SEND
# =========================================================

@app.route("/test-send", methods=["GET"])
def test_send():

    recipient = "919750820509"

    print("=" * 60)
    print("📤 AI MIS WHATSAPP TEST SEND")
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
        "Sending to:",
        recipient
    )

    print(
        "Phone Number ID:",
        PHONE_NUMBER_ID
    )

    if not PHONE_NUMBER_ID:

        return {
            "success": False,
            "error":
                "WHATSAPP_PHONE_NUMBER_ID is missing in Render"
        }, 500

    if not ACCESS_TOKEN:

        return {
            "success": False,
            "error":
                "WHATSAPP_ACCESS_TOKEN is missing in Render"
        }, 500

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
                "AI MIS webhook test message"
        }
    }

    print(
        "Meta URL:",
        url
    )

    try:

        response = requests.post(
            url,
            headers=headers,
            json=payload,
            timeout=30
        )

        print(
            "Meta Status:",
            response.status_code
        )

        print(
            "Meta Response:",
            response.text
        )

        print("=" * 60)

        try:

            meta_response = response.json()

        except Exception:

            meta_response = response.text

        return {
            "success":
                response.ok,

            "meta_status":
                response.status_code,

            "meta_response":
                meta_response
        }, response.status_code

    except Exception as e:

        print(
            "❌ Test send error:",
            str(e)
        )

        return {
            "success":
                False,

            "error":
                str(e)
        }, 500


# =========================================================
# LOCAL RUN
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
