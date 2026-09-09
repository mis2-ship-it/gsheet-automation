import requests

# Replace with your actual credentials to test
BOT_TOKEN = "8737214547:AAEpKIIWm9kFi8fJfYvBAKZTykZYxJqfqHg"
CHAT_ID = "8660083299"

message = (
    "🚀 *DSR Pipeline Connected!*\n\n"
    "Your Telegram Bot is set up and ready to send daily sales reports."
)

url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
payload = {
    "chat_id": CHAT_ID,
    "text": message,
    "parse_mode": "Markdown"
}

response = requests.post(url, json=payload)

if response.status_code == 200:
    print("✅ Success! Check your Telegram for the message.")
else:
    print(f"❌ Failed. Error code: {response.status_code}\n{response.text}")
