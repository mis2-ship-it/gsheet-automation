from flask import Flask, request

app = Flask(__name__)

VERIFY_TOKEN = "AI_MIS_WHATSAPP_VERIFY_2026"


@app.route("/webhook", methods=["GET"])
def verify_webhook():

    mode = request.args.get("hub.mode")
    token = request.args.get("hub.verify_token")
    challenge = request.args.get("hub.challenge")

    if mode == "subscribe" and token == VERIFY_TOKEN:
        return challenge, 200

    return "Verification failed", 403


@app.route("/webhook", methods=["POST"])
def receive_webhook():

    data = request.get_json(silent=True)

    print("=" * 60)
    print("WHATSAPP WEBHOOK RECEIVED")
    print("=" * 60)

    print(data)

    print("=" * 60)

    return "EVENT_RECEIVED", 200


@app.route("/", methods=["GET"])
def home():
    return "AI MIS WhatsApp Webhook is running", 200


if __name__ == "__main__":

    print("=" * 60)
    print("AI MIS WhatsApp Webhook")
    print("Port: 5000")
    print("=" * 60)

    app.run(
        host="0.0.0.0",
        port=5000
    )
