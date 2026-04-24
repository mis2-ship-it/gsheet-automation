print("Running latest version 🚀")
import os
import json
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials

print("Step 1: Script started")

# 🔐 Credentials
creds_dict = json.loads(os.environ["GOOGLE_CREDENTIALS"])

creds = Credentials.from_service_account_info(
    creds_dict,
    scopes=[
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive"
]
)

client = gspread.authorize(creds)

# 🔗 Open sheet
sheet = client.open_by_url("https://docs.google.com/spreadsheets/d/1ldgnNMdeubDx_ImtCC1uCD7FGz8gt_edmWEkmO_xRNk/edit?gid=0#gid=0&fvid=145648629").worksheet("Master")

# 👉 Your data logic
df = pd.DataFrame({
    "A": [1, 2, 3],
    "B": [4, 5, 6]
})

print("Step 2: Data loaded", df.shape)

# 🛑 Safety check
if df.empty:
    print("No data to update")
else:
    print("Step 3: Writing to sheet...")

    sheet.clear()
    sheet.update(
        'A1',
        [df.columns.values.tolist()] + df.values.tolist()
    )

    print("Step 4: Sheet updated successfully")
