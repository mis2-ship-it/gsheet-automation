import requests


@app.route("/test-send", methods=["GET"])
def test_send():

    PHONE_NUMBER_ID = os.environ.get("WHATSAPP_PHONE_NUMBER_ID")
    ACCESS_TOKEN = os.environ.get("WHATSAPP_ACCESS_TOKEN")
    RECIPIENT = "919750820509"

    url = (
        f"https://graph.facebook.com/v23.0/"
        f"{PHONE_NUMBER_ID}/messages"
    )

    headers = {
        "Authorization": f"Bearer {ACCESS_TOKEN}",
        "Content-Type": "application/json"
    }

    payload = {
        "messaging_product": "whatsapp",
        "to": RECIPIENT,
        "type": "text",
        "text": {
            "preview_url": False,
            "body": "AI MIS webhook test message"
        }
    }

    response = requests.post(
        url,
        headers=headers,
        json=payload,
        timeout=30
    )

    print("=" * 60)
    print("WHATSAPP TEST SEND")
    print("Status:", response.status_code)
    print("Response:", response.text)
    print("=" * 60)

    return response.text, response.status_code
