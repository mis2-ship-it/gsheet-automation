#!/usr/bin/env python3
"""
Discount Dashboard data: writes the Discount Code rows to a Google Sheet for the Apps Script dashboard.
Reads data/discount_rows.csv (from sync_discounts.py), counts per day, and writes two tabs:
  Disc_Label  one row per day, brand, region, source, session and discount label
  Disc_Item   the same, with item and category
Invoices are counted once per group (unique invoice numbers). Only the last KEEP_DAYS days are kept.
Usage: python write_discount_sheet.py --rows data/discount_rows.csv [--keep-days 400]
Needs DISCOUNT_SHEET_ID and GOOGLE_CREDENTIALS.
"""
import argparse
import json
import os
import re

import pandas as pd

LABEL_COLS = ["Date", "Brand", "Region", "Source", "Session", "Label", "Invoices", "Qty", "Gross", "Discount", "Net"]
ITEM_COLS = ["Date", "Brand", "Region", "Source", "Session", "Label", "Item", "Category", "Invoices", "Qty", "Gross", "Discount", "Net"]
BASE_KEYS = ["day", "brand", "region", "source", "session", "label"]
RENAME_BASE = {"day": "Date", "brand": "Brand", "region": "Region", "source": "Source", "session": "Session", "label": "Label"}
AGG = dict(Invoices=("invoiceNumber", "nunique"), Qty=("qty", "sum"), Gross=("gross", "sum"),
           Discount=("disc", "sum"), Net=("net", "sum"))
SHEET_ID_PATTERN = re.compile(r"[A-Za-z0-9_-]{20,}")


def aggregate(rows, keep_days=0):
    """rows: the rows of data/discount_rows.csv. Returns (label rows, item rows)."""
    rows = rows.copy()
    for c in ["qty", "gross", "disc", "net"]:
        rows[c] = pd.to_numeric(rows[c], errors="coerce").fillna(0.0)
    rows["disc"] = rows["disc"].abs()
    rows["day"] = rows["day"].astype(str).str[:10]
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
    return lab[LABEL_COLS], itm[ITEM_COLS]


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


def write_sheet(lab, itm):
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
    write_tab(sh, "Disc_Label", lab)
    write_tab(sh, "Disc_Item", itm)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rows", default="data/discount_rows.csv")
    ap.add_argument("--keep-days", type=int, default=400)
    args = ap.parse_args()
    rows = pd.read_csv(args.rows, dtype=str, keep_default_na=False)
    lab, itm = aggregate(rows, args.keep_days)
    print(f"{len(lab)} label rows, {len(itm)} item rows, days {lab['Date'].min()} to {lab['Date'].max()}")
    write_sheet(lab, itm)


if __name__ == "__main__":
    main()
