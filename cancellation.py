import requests
import pandas as pd

API_KEY = "API_KEY"
URL = "https://api.ristaapps.com/v1/orders"  # confirm endpoint from Rista

headers = {
    "Authorization": f"Bearer {API_KEY}",
    "Content-Type": "application/json"
}

params = {
    "from_date": "2026-05-04",
    "to_date": "2026-05-04"
}

response = requests.get(URL, headers=headers, params=params)

data = response.json()

df = pd.json_normalize(data["orders"])

# Keep only cancelled orders
cancel_df = df[df["status"] == "Cancelled"]

print(cancel_df.head())
