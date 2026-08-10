from flask import Flask, request
import os
import requests

# =====================================================
# FLASK APP
# =====================================================

app = Flask(__name__)

# =====================================================
# ENVIRONMENT VARIABLES
# =====================================================

VERIFY_TOKEN = os.environ.get("WHATSAPP_VERIFY_TOKEN")
PHONE_NUMBER_ID = os.environ.get("WHATSAPP_PHONE_NUMBER_ID")
ACCESS_TOKEN = os.environ.get("WHATSAPP_ACCESS_TOKEN")

# =====================================================
# HOME
# =====================================================

@app.route("/", methods=["GET"])
def home():
    return "AI MIS WhatsApp Webhook is running", 200


# =====================================================
# META WEBHOOK VERIFICATION
# =====================================================

@app.route("/webhook", methods=["GET"])
def verify_webhook():

    mode = request.args.get("hub.mode")
    token = request.args.get("hub.verify_token")
    challenge = request.args.get("hub.challenge")

    if mode == "subscribe" and token == VERIFY_TOKEN:
        print("✅ META WEBHOOK VERIFIED")
        return challenge, 200

    print("❌ WEBHOOK VERIFICATION FAILED")
    return "Forbidden", 403


# =====================================================
# META WEBHOOK RECEIVER
# =====================================================

@app.route("/webhook", methods=["POST"])
def webhook():

    data = request.get_json(silent=True)

    print("=" * 60)
    print("📩 WHATSAPP WEBHOOK RECEIVED")
    print(data)
    print("=" * 60)

    return "EVENT_RECEIVED", 200


# =====================================================
# TEST SEND
# =====================================================

@app.route("/test-send", methods=["GET"])
def test_send():

    print("=" * 60)
    print("📤 AI MIS WHATSAPP TEST SEND")
    print("=" * 60)

    # Check environment variables
    print("PHONE_NUMBER_ID exists :", bool(PHONE_NUMBER_ID))
    print("ACCESS_TOKEN exists    :", bool(ACCESS_TOKEN))
    print("VERIFY_TOKEN exists    :", bool(VERIFY_TOKEN))

    if not PHONE_NUMBER_ID:
        return {
            "success": False,
            "error": "WHATSAPP_PHONE_NUMBER_ID is missing in Render"
        }, 500

    if not ACCESS_TOKEN:
        return {
            "success": False,
            "error": "WHATSAPP_ACCESS_TOKEN is missing in Render"
        }, 500

    # -------------------------------------------------
    # RECIPIENT
    # -------------------------------------------------

    RECIPIENT = "919750820509"

    # -------------------------------------------------
    # META URL
    # -------------------------------------------------

    url = (
        f"https://graph.facebook.com/v23.0/"
        f"{PHONE_NUMBER_ID}/messages"
    )

    # -------------------------------------------------
    # HEADERS
    # -------------------------------------------------

    headers = {
        "Authorization": f"Bearer {ACCESS_TOKEN}",
        "Content-Type": "application/json"
    }

    # -------------------------------------------------
    # MESSAGE
    # -------------------------------------------------

    payload = {
        "messaging_product": "whatsapp",
        "to": RECIPIENT,
        "type": "text",
        "text": {
            "preview_url": False,
            "body": "AI MIS webhook test message"
        }
    }

    print("Sending to:", RECIPIENT)
    print("Phone Number ID:", PHONE_NUMBER_ID)
    print("Meta URL:", url)

    # -------------------------------------------------
    # SEND
    # -------------------------------------------------

    try:

        response = requests.post(
            url,
            headers=headers,
            json=payload,
            timeout=30
        )

        print("Meta Status:", response.status_code)
        print("Meta Response:", response.text)

        print("=" * 60)

        return {
            "success": response.ok,
            "meta_status": response.status_code,
            "meta_response": response.json()
                if response.headers.get("content-type", "").startswith("application/json")
                else response.text
        }, response.status_code

    except Exception as e:

        print("❌ TEST SEND ERROR:", str(e))

        return {
            "success": False,
            "error": str(e)
        }, 500


# =====================================================
# LOCAL RUN
# =====================================================

if __name__ == "__main__":

    port = int(os.environ.get("PORT", 5000))

    app.run(
        host="0.0.0.0",
        port=port
    )
