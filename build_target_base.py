#!/usr/bin/env python3
"""
Target Planner data: daily sales by store, brand and channel (Offline = In Store, Online = everything else).
Built from the historical file and the completed days in the nightly cache, with the dashboard's own cleaning
(build_site.clean_sales), so the planner and the dashboard show the same numbers.

Writes data/target_base.csv, then the Target_Base tab of the Google Sheet whose ID is in TARGET_SHEET_ID.
Usage: python build_target_base.py [--from 2025-08-01] [--history ...] [--new ...] [--out ...]
"""
import argparse
import json
import os
import re

import pandas as pd

from build_site import clean_sales, read_if_present

COLUMNS = ["Date", "Branch", "Region", "Store Type", "Brand", "Channel", "Net", "Gross", "Discount", "Orders", "Quantity"]
DEFAULT_FROM = "2025-08-01"
SHEET_ID_PATTERN = re.compile(r"[A-Za-z0-9_-]{20,}")


def build(history, new, start):
    parts = [df for df in (read_if_present(history), read_if_present(new)) if df is not None]
    if not parts:
        raise SystemExit("No sales files found.")
    df = clean_sales(pd.concat(parts, ignore_index=True))
    df = df[df["Date"] >= pd.Timestamp(start)].copy()
    df["Channel"] = df["Source"].map(lambda s: "Offline" if s == "In Store" else "Online")
    df["Quantity"] = pd.to_numeric(df["Quantity"], errors="coerce").fillna(0)
    g = df.groupby(["Date", "Branch", "Region", "Store Type", "Brand Name", "Channel"], as_index=False).agg(
        Net=("Net Sales", "sum"), Gross=("Gross Sales", "sum"), Discount=("Discount", "sum"),
        Orders=("Orders", "sum"), Quantity=("Quantity", "sum"))
    g = g.rename(columns={"Brand Name": "Brand"})
    g["Date"] = g["Date"].dt.strftime("%Y-%m-%d")
    for c in ["Net", "Gross", "Discount"]:
        g[c] = g[c].round(2)
    return g[COLUMNS].sort_values(["Date", "Branch", "Brand", "Channel"]).reset_index(drop=True)


def sheet_id_from(text):
    """Accepts the bare ID, or a pasted sheet address, and returns the ID."""
    s = (text or "").strip().strip('"').strip("'").strip()
    m = re.search(r"/spreadsheets/d/([A-Za-z0-9_-]+)", s)
    return m.group(1) if m else s


def write_sheet(df):
    import gspread
    from google.oauth2.service_account import Credentials
    sid = sheet_id_from(os.environ.get("TARGET_SHEET_ID", ""))
    if not SHEET_ID_PATTERN.fullmatch(sid):
        raise SystemExit("TARGET_SHEET_ID is missing or is not a sheet ID. Paste only the ID: "
                         "the text between /d/ and /edit in the sheet's address.")
    creds = Credentials.from_service_account_info(
        json.loads(os.environ["GOOGLE_CREDENTIALS"]),
        scopes=["https://www.googleapis.com/auth/spreadsheets"])
    gc = gspread.authorize(creds)
    try:
        sh = gc.open_by_key(sid)
    except gspread.exceptions.APIError as e:
        status = getattr(getattr(e, "response", None), "status_code", "unknown")
        if status == 403:
            why = ("The Google Sheets API is not enabled for the project of this service account. "
                   "Enable it in Google Cloud Console under APIs & Services.")
        else:
            why = ("Google could not open the sheet. Usually the sheet is not shared with the service account "
                   "as Editor, or the ID is wrong.")
        raise SystemExit(
            "Could not open the Target Planner sheet.\n"
            f"  ID used: {sid[:6]}...{sid[-4:]} ({len(sid)} characters)\n"
            f"  Service account: {creds.service_account_email}\n"
            f"  Google status: {status}. {why}\n"
            f"  Share the sheet with {creds.service_account_email} as Editor, check TARGET_SHEET_ID, "
            "then run the workflow again.")
    try:
        ws = sh.worksheet("Target_Base")
        ws.clear()
    except gspread.WorksheetNotFound:
        ws = sh.add_worksheet("Target_Base", rows=10, cols=len(COLUMNS))
    values = [COLUMNS] + df.astype(object).where(pd.notnull(df), "").values.tolist()
    ws.resize(rows=len(values), cols=len(COLUMNS))
    for i in range(0, len(values), 20000):
        ws.update(range_name=f"A{i + 1}", values=values[i:i + 20000])
    print(f"Target_Base: {len(df)} rows written to the sheet")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--from", dest="start", default=DEFAULT_FROM)
    ap.add_argument("--history", default="historical_data/historical_sales.csv.gz")
    ap.add_argument("--new", default="data/new_sales.csv.gz")
    ap.add_argument("--out", default="data/target_base.csv")
    args = ap.parse_args()
    df = build(args.history, args.new, args.start)
    os.makedirs(os.path.dirname(args.out) or ".", exist_ok=True)
    df.to_csv(args.out, index=False)
    print(f"wrote {args.out}: {len(df)} rows, {df['Date'].min()} to {df['Date'].max()}")
    if os.environ.get("TARGET_SHEET_ID"):
        write_sheet(df)


if __name__ == "__main__":
    main()
