#!/usr/bin/env python3
"""
Discount Dashboard data: writes the Discount Code rows to a Google Sheet for the Apps Script dashboard.
  --rows  data/discount_rows.csv   completed days (from sync_discounts.py, nightly)
  --live  data/discount_live.csv   today so far (from sync_discounts.py --live), optional
Writes:
  Disc_Label  one row per day, brand, region, source, session and discount label
  Disc_Item   the same, with item and category
  Disc_Hour   one row per day, business hour, brand, region, source, session and label,
              for the last HOUR_DAYS business days (today included when --live is given)
Invoices are counted once per group. Disc_Label and Disc_Item keep the last KEEP_DAYS days.
Usage: python write_discount_sheet.py [--rows ...] [--live ...] [--keep-days 400] [--hour-days 9] [--only-hourly]
Needs DISCOUNT_SHEET_ID and GOOGLE_CREDENTIALS.
"""
import argparse
import json
import os
import re
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import pandas as pd

LABEL_COLS = ["Date", "Brand", "Region", "Source", "Session", "Label", "Invoices", "Qty", "Gross", "Discount", "Net", "Lines"]
ITEM_COLS = ["Date", "Brand", "Region", "Source", "Session", "Label", "Item", "Category", "Invoices", "Qty", "Gross", "Discount", "Net", "Lines"]
HOUR_COLS = ["Date", "Hour", "Brand", "Region", "Source", "Session", "Label", "Invoices", "Qty", "Gross", "Discount", "Net", "Lines"]
BASE_KEYS = ["day", "brand", "region", "source", "session", "label"]
HOUR_KEYS = ["day", "bh", "brand", "region", "source", "session", "label"]
RENAME_BASE = {"day": "Date", "brand": "Brand", "region": "Region", "source": "Source", "session": "Session", "label": "Label"}
AGG = dict(Invoices=("invoiceNumber", "nunique"), Qty=("qty", "sum"), Gross=("gross", "sum"),
           Discount=("disc", "sum"), Net=("net", "sum"), Lines=("item", "size"))
SHEET_ID_PATTERN = re.compile(r"[A-Za-z0-9_-]{20,}")


def read_csv_if_present(path):
    if not path or not os.path.exists(path) or os.path.getsize(path) == 0:
        return None
    return pd.read_csv(path, dtype=str, keep_default_na=False)


def clean_numbers(rows):
    rows = rows.copy()
    for c in ["qty", "gross", "disc", "net"]:
        rows[c] = pd.to_numeric(rows[c], errors="coerce").fillna(0.0)
    rows["disc"] = rows["disc"].abs()
    rows["day"] = rows["day"].astype(str).str[:10]
    return rows


def aggregate(rows, keep_days=0):
    """Returns (label rows, item rows) for the days kept."""
    rows = clean_numbers(rows)
    if keep_days:
        days = sorted(rows["day"].unique())
        if len(days) > keep_days:
            rows = rows[rows["day"] >= days[-keep_days]]
    lab = rows.groupby(BASE_KEYS).agg(**AGG).reset_index().rename(columns=RENAME_BASE)
    itm = (rows.groupby(BASE_KEYS + ["item", "category"]).agg(**AGG).reset_index()
           .rename(columns={**RENAME_BASE, "item": "Item", "category": "Category"}))
    for df in (lab, itm):
        for c in ["Qty", "Gross", "Discount", "Net"]:
            df[c] = df[c].astype(float).round(2)
        df["Invoices"] = df["Invoices"].astype(int)
        df["Lines"] = df["Lines"].astype(int)
    return lab[LABEL_COLS], itm[ITEM_COLS]


def aggregate_hour(rows, hour_days, today):
    """Hourly label rows for the last hour_days business days (orders with a known time only)."""
    empty = pd.DataFrame(columns=HOUR_COLS)
    if rows is None or rows.empty or "bh" not in rows.columns:
        return empty
    rows = clean_numbers(rows)
    rows["bh"] = pd.to_numeric(rows["bh"], errors="coerce").fillna(-1).astype(int)
    rows = rows[rows["bh"] >= 0]
    first = (today - timedelta(days=hour_days - 1)).strftime("%Y-%m-%d")
    rows = rows[rows["day"] >= first]
    if rows.empty:
        return empty
    hr = (rows.groupby(HOUR_KEYS).agg(**AGG).reset_index()
          .rename(columns={**RENAME_BASE, "bh": "Hour"}))
    hr["Hour"] = hr["Hour"].astype(int)
    for c in ["Qty", "Gross", "Discount", "Net"]:
        hr[c] = hr[c].astype(float).round(2)
    hr["Invoices"] = hr["Invoices"].astype(int)
    hr["Lines"] = hr["Lines"].astype(int)
    return hr[HOUR_COLS]


def sheet_id_from(text):
    s = (text or "").strip().strip('"').strip("'").strip()
    m = re.search(r"/spreadsheets/d/([A-Za-z0-9_-]+)", s)
    return m.group(1) if m else s


def write_tab(sh, name, df):
    import gspread
    try:
        ws = sh.worksheet(name)
        ws.clear()
    except gspread.WorksheetNotFound:
        ws = sh.add_worksheet(name, rows=10, cols=len(df.columns))
    values = [list(df.columns)] + df.astype(object).where(pd.notnull(df), "").values.tolist()
    ws.resize(rows=len(values), cols=len(df.columns))
    for i in range(0, len(values), 20000):
        ws.update(range_name=f"A{i + 1}", values=values[i:i + 20000])
    print(f"{name}: {len(df)} rows written")


def write_sheet(lab, itm, hour):
    import gspread
    from google.oauth2.service_account import Credentials
    sid = sheet_id_from(os.environ.get("DISCOUNT_SHEET_ID", ""))
    if not SHEET_ID_PATTERN.fullmatch(sid):
        raise SystemExit("DISCOUNT_SHEET_ID is missing or is not a sheet ID. Paste only the ID: "
                         "the text between /d/ and /edit in the sheet's address.")
    creds = Credentials.from_service_account_info(
        json.loads(os.environ["GOOGLE_CREDENTIALS"]),
        scopes=["https://www.googleapis.com/auth/spreadsheets"])
    try:
        sh = gspread.authorize(creds).open_by_key(sid)
    except gspread.exceptions.APIError:
        raise SystemExit(f"Could not open the Discount Dashboard sheet. Share it with "
                         f"{creds.service_account_email} as Editor, and check DISCOUNT_SHEET_ID.")
    if lab is not None:
        write_tab(sh, "Disc_Label", lab)
        write_tab(sh, "Disc_Item", itm)
    write_tab(sh, "Disc_Hour", hour)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rows", default="data/discount_rows.csv")
    ap.add_argument("--live", default="")
    ap.add_argument("--keep-days", type=int, default=400)
    ap.add_argument("--hour-days", type=int, default=9)
    ap.add_argument("--only-hourly", action="store_true", help="write only the hourly tab (live job)")
    args = ap.parse_args()
    today = (datetime.now(ZoneInfo("Asia/Kolkata")) - timedelta(hours=5)).date()
    rows = read_csv_if_present(args.rows)
    live = read_csv_if_present(args.live) if args.live else None
    if rows is None and live is None:
        raise SystemExit("No discount rows found. Run the Discount Code rows workflow first.")
    today_s = today.strftime("%Y-%m-%d")
    hourly = []
    if rows is not None:
        hourly.append(rows[rows["day"].astype(str).str[:10] != today_s] if live is not None else rows)
    if live is not None:
        hourly.append(live)
    hour = aggregate_hour(pd.concat(hourly, ignore_index=True), args.hour_days, today)
    print(f"hourly rows: {len(hour)}")
    lab = itm = None
    if not args.only_hourly:
        if rows is None:
            raise SystemExit("No completed-day rows. Run the Discount Code rows workflow first.")
        lab, itm = aggregate(rows, args.keep_days)
        print(f"{len(lab)} label rows, {len(itm)} item rows, days {lab['Date'].min()} to {lab['Date'].max()}")
    write_sheet(lab, itm, hour)


if __name__ == "__main__":
    main()
