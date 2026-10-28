#!/usr/bin/env python3
"""
Discount Dashboard data: writes the Discount Code rows to a Google Sheet for the Apps Script dashboard.
  --rows  data/discount_rows.csv   completed days (from sync_discounts.py, nightly)
  --live  data/discount_live.csv   today so far (from sync_discounts.py --live), optional
Writes:
  Disc_Label        one row per day, brand, region, source, session and discount label (Overview)
  Disc_Item         the same, with item and category (Overview)
  Disc_Hour         one row per day, business hour, brand, region, source, session and label (Live)
  Disc_Bucket       orders and sales by AOV band and by discount band, per day, brand, region, source, session
  Disc_Prod         item sales split by whether the order had a discount (Product performance)
  Disc_Prod_Bucket  item sales inside each AOV and discount band (Discount analysis drill-down)
The bucket bands are the same as the Sales dashboard (build_site.py), applied to each order.
Bands are upper-inclusive, as on the Sales dashboard: an order of exactly 200 is in 100-200.
  Disc_Recon        product totals against the order totals, by day and source (--recon, from sync_discounts.py)
Disc_Label, Disc_Item and Disc_Hour use Swiggy and Zomato only. Disc_Bucket, Disc_Prod and Disc_Prod_Bucket use
all channels (--products, from sync_discounts.py --products-out) and keep the last PRODUCT_DAYS completed days.
Lines count item lines only: the "Charges (not a product)" and "Other differences" rows (line_flag 0) are not counted.
Usage: python write_discount_sheet.py [--rows ...] [--live ...] [--keep-days 400] [--hour-days 9]
       [--product-days 62] [--only-hourly]
Needs DISCOUNT_SHEET_ID and GOOGLE_CREDENTIALS.
"""
import argparse
import json
import os
import re
import sys
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd

LABEL_COLS = ["Date", "Brand", "Region", "Source", "Session", "Label", "Invoices", "Qty", "Gross", "Discount", "Net", "Lines"]
ITEM_COLS = ["Date", "Brand", "Region", "Source", "Session", "Label", "Item", "Category", "Invoices", "Qty", "Gross", "Discount", "Net", "Lines"]
HOUR_COLS = ["Date", "Hour", "Brand", "Region", "Source", "Session", "Label", "Invoices", "Qty", "Gross", "Discount", "Net", "Lines"]
BUCKET_COLS = ["Date", "Brand", "Region", "Source", "Session", "Kind", "Bucket", "Orders", "Qty", "Gross", "Discount", "Net"]
PROD_COLS = ["Date", "Brand", "Region", "Source", "Discounted", "Category", "Item", "Orders", "Qty", "Gross", "Discount", "Net"]
PBUCKET_COLS = ["Date", "Brand", "Region", "Source", "Kind", "Bucket", "Category", "Item", "Orders", "Qty", "Gross", "Discount", "Net"]
RECON_NAMES = {"day": "Date", "source": "Source", "orders": "Orders", "sales_gross": "Sales gross",
               "sales_discount": "Sales discount", "sales_net": "Sales net", "product_gross": "Product gross",
               "product_discount": "Product discount", "product_net": "Product net", "charges_net": "Charges",
               "other_net": "Other net", "other_gross": "Other gross", "gross_gap": "Gross gap", "net_gap": "Net gap"}
BASE_KEYS = ["day", "brand", "region", "source", "session", "label"]
HOUR_KEYS = ["day", "bh", "brand", "region", "source", "session", "label"]
RENAME_BASE = {"day": "Date", "brand": "Brand", "region": "Region", "source": "Source", "session": "Session", "label": "Label"}
AGG = dict(Invoices=("invoiceNumber", "nunique"), Qty=("qty", "sum"), Gross=("gross", "sum"),
           Discount=("disc", "sum"), Net=("net", "sum"), Lines=("line_flag", "sum"))
SHEET_ID_PATTERN = re.compile(r"[A-Za-z0-9_-]{20,}")

# Bands, the same as the Sales dashboard (build_site.py). Applied per order.
AOV_LABELS = ["0-100", "100-200", "200-300", "300-400", "400-500", "500-600", "600-900", ">900"]
AOV_EDGES = [100, 200, 300, 400, 500, 600, 900]
DISC_LABELS = ["0%", "1%-10%", "10%-20%", "20%-30%", "30%-40%", "40%-50%", "50%-60%",
               "60%-70%", "70%-80%", "80%-90%", "90%-100%"]
DISC_EDGES = [10, 20, 30, 40, 50, 60, 70, 80, 90, 100]
PRODUCT_DAYS = 62
DEFAULT_STORE_TYPE = "COCO"


def keep_store(rows, store_type, label):
    """Keeps the rows of one store type (COCO by default), as the Sales dashboard's COCO view does."""
    if rows is None or store_type == "all" or "ownership" not in rows.columns:
        return rows
    kept = rows[rows["ownership"] == store_type]
    print(f"{label}: kept {store_type} rows {len(kept):,} of {len(rows):,}")
    return kept


def read_csv_if_present(path):
    if not path or not os.path.exists(path) or os.path.getsize(path) == 0:
        return None
    return pd.read_csv(path, dtype=str, keep_default_na=False)


def clean_numbers(rows):
    rows = rows.copy()
    for c in ["qty", "gross", "disc", "net"]:
        rows[c] = pd.to_numeric(rows[c], errors="coerce").fillna(0.0)
    rows["disc"] = rows["disc"].abs()
    if "line_flag" not in rows.columns:
        rows["line_flag"] = 1
    rows["line_flag"] = pd.to_numeric(rows["line_flag"], errors="coerce").fillna(1)
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


def aov_band(net):
    """Band index for an order's net amount (upper-inclusive, as on the Sales dashboard)."""
    return np.searchsorted(AOV_EDGES, np.asarray(net, dtype=float), side="left")


def disc_band(disc, gross):
    """0 = no discount; otherwise the band of discount % of gross (upper-inclusive)."""
    disc = np.asarray(disc, dtype=float)
    gross = np.asarray(gross, dtype=float)
    pct = np.where(gross > 0, disc / np.where(gross > 0, gross, 1.0) * 100, 0.0)
    return np.where(disc <= 0, 0, np.searchsorted(DISC_EDGES, pct, side="left") + 1)


def build_product_tabs(rows, days=PRODUCT_DAYS):
    """Bucket, product and product-by-bucket tables from the completed item rows (last `days` days)."""
    empty = (pd.DataFrame(columns=BUCKET_COLS), pd.DataFrame(columns=PROD_COLS), pd.DataFrame(columns=PBUCKET_COLS))
    if rows is None or rows.empty:
        return empty
    r = clean_numbers(rows)
    day_list = sorted(r["day"].unique())
    if len(day_list) > days:
        r = r[r["day"] >= day_list[-days]]
    if r.empty:
        return empty
    r["invoiceNumber"] = r["invoiceNumber"].astype(str)
    orders = r.groupby("invoiceNumber").agg(o_gross=("gross", "sum"), o_disc=("disc", "sum"), o_net=("net", "sum")).reset_index()
    orders["aov_i"] = aov_band(orders["o_net"])
    orders["disc_i"] = disc_band(orders["o_disc"], orders["o_gross"])
    orders["Discounted"] = np.where(orders["o_disc"] > 0, "Yes", "No")
    r = r.merge(orders[["invoiceNumber", "aov_i", "disc_i", "Discounted"]], on="invoiceNumber", how="left")
    r["aov"] = [AOV_LABELS[i] for i in r["aov_i"]]
    r["dsc"] = [DISC_LABELS[i] for i in r["disc_i"]]

    def agg(keys, frame):
        g = frame.groupby(keys).agg(Orders=("invoiceNumber", "nunique"), Qty=("qty", "sum"), Gross=("gross", "sum"),
                                    Discount=("disc", "sum"), Net=("net", "sum")).reset_index()
        for c in ["Qty", "Gross", "Discount", "Net"]:
            g[c] = g[c].astype(float).round(2)
        g["Orders"] = g["Orders"].astype(int)
        return g

    names = {"day": "Date", "brand": "Brand", "region": "Region", "source": "Source", "session": "Session",
             "category": "Category", "item": "Item", "Discounted": "Discounted", "aov": "Bucket", "dsc": "Bucket"}
    b_aov = agg(["day", "brand", "region", "source", "session", "aov"], r).rename(columns=names)
    b_aov["Kind"] = "AOV"
    b_dsc = agg(["day", "brand", "region", "source", "session", "dsc"], r).rename(columns=names)
    b_dsc["Kind"] = "Discount"
    bucket = pd.concat([b_aov, b_dsc], ignore_index=True)[BUCKET_COLS]

    prod = agg(["day", "brand", "region", "source", "Discounted", "category", "item"], r).rename(columns=names)[PROD_COLS]

    p_aov = agg(["day", "brand", "region", "source", "aov", "category", "item"], r).rename(columns=names)
    p_aov["Kind"] = "AOV"
    p_dsc = agg(["day", "brand", "region", "source", "dsc", "category", "item"], r).rename(columns=names)
    p_dsc["Kind"] = "Discount"
    pbucket = pd.concat([p_aov, p_dsc], ignore_index=True)[PBUCKET_COLS]
    return bucket, prod, pbucket


def build_recon_tab(recon):
    """Product totals against the order totals, by day and source, with the gaps (net and gross should be 0)."""
    cols = list(RECON_NAMES.values())
    if recon is None or recon.empty:
        return pd.DataFrame(columns=cols)
    r = recon.rename(columns=RECON_NAMES)[cols].copy()
    for c in cols[2:]:
        r[c] = pd.to_numeric(r[c], errors="coerce").fillna(0.0).round(2)
    r["Orders"] = r["Orders"].astype(int)
    return r.sort_values(["Date", "Source"]).reset_index(drop=True)


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
    # RAW keeps labels such as "0%" as text, so they match the dashboard's band names.
    for i in range(0, len(values), 20000):
        ws.update(range_name=f"A{i + 1}", values=values[i:i + 20000], value_input_option="RAW")
    print(f"{name}: {len(df)} rows written")


def open_sheet():
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
        return gspread.authorize(creds).open_by_key(sid)
    except gspread.exceptions.APIError:
        raise SystemExit(f"Could not open the Discount Dashboard sheet. Share it with "
                         f"{creds.service_account_email} as Editor, and check DISCOUNT_SHEET_ID.")


def write_sheet(tabs):
    sh = open_sheet()
    for name, df in tabs:
        write_tab(sh, name, df)


def read_tab(sh, name):
    """Current contents of a tab, as text. An empty frame if the tab does not exist yet."""
    import gspread
    try:
        ws = sh.worksheet(name)
    except gspread.WorksheetNotFound:
        return pd.DataFrame()
    values = ws.get_all_values()
    if not values:
        return pd.DataFrame()
    return pd.DataFrame(values[1:], columns=values[0])


DATE_FORMATS = ["%Y-%m-%d", "%d/%m/%Y", "%d-%m-%Y", "%m/%d/%Y"]


def _one_date(text):
    first = str(text).strip().split(" ")[0]          # drop a time part, if the sheet added one
    for fmt in DATE_FORMATS:
        try:
            return datetime.strptime(first, fmt).strftime("%Y-%m-%d")
        except ValueError:
            pass
    return ""


def normalize_dates(values):
    """Dates from the sheet as yyyy-mm-dd. Reads ISO, day-first and month-first text; unreadable values become ''."""
    return pd.Series([_one_date(v) for v in values], dtype=object)


def merge_live_hours(existing, live_hours, today, hour_days):
    """Disc_Hour for the window: the days already in the sheet (written by the nightly run, and including last week),
    with today's hours replaced by the live rows. Returns (frame, whether last week's day is in the sheet, rows per day)."""
    today_s = today.strftime("%Y-%m-%d")
    last_week_s = (today - timedelta(days=7)).strftime("%Y-%m-%d")
    first_s = (today - timedelta(days=hour_days - 1)).strftime("%Y-%m-%d")
    if existing is None or existing.empty or "Date" not in existing.columns:
        old = pd.DataFrame(columns=HOUR_COLS)
    else:
        old = existing.copy()
        old["Date"] = normalize_dates(old["Date"])
        unreadable = int((old["Date"] == "").sum())
        if unreadable:
            print(f"Warning: {unreadable} row(s) in Disc_Hour have a Date that could not be read.")
        old = old[(old["Date"] != today_s) & (old["Date"] >= first_s)].copy()
        for c in ["Hour", "Invoices", "Qty", "Gross", "Discount", "Net", "Lines"]:
            if c in old.columns:
                old[c] = pd.to_numeric(old[c], errors="coerce").fillna(0)
        old = old.reindex(columns=HOUR_COLS)
    combined = pd.concat([old, live_hours.reindex(columns=HOUR_COLS)], ignore_index=True)
    days = old["Date"].value_counts().sort_index().to_dict() if len(old) else {}
    return combined, last_week_s in days, days


def describe_days(days):
    return ", ".join(f"{d} ({n} rows)" for d, n in days.items()) if days else "none"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rows", default="data/discount_rows.csv")
    ap.add_argument("--live", default="")
    ap.add_argument("--keep-days", type=int, default=400)
    ap.add_argument("--hour-days", type=int, default=9)
    ap.add_argument("--product-days", type=int, default=PRODUCT_DAYS)
    ap.add_argument("--products", default="data/product_rows.csv", help="all channels (sync_discounts.py --products-out)")
    ap.add_argument("--recon", default="data/product_recon.csv", help="reconciliation (sync_discounts.py --recon-out)")
    ap.add_argument("--only-hourly", action="store_true", help="write only the hourly tab (live job)")
    ap.add_argument("--store-type", default=DEFAULT_STORE_TYPE, help="COCO (default), FOFO, or all")
    args = ap.parse_args()
    today = (datetime.now(ZoneInfo("Asia/Kolkata")) - timedelta(hours=5)).date()
    rows = keep_store(read_csv_if_present(args.rows), args.store_type, "discount rows")
    live = keep_store(read_csv_if_present(args.live) if args.live else None, args.store_type, "live rows")
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
    if not args.only_hourly and len(hour):
        print(f"Disc_Hour days from the saved rows: {describe_days(hour['Date'].value_counts().sort_index().to_dict())}")
    if args.only_hourly:
        # Live job: no saved rows are needed. Last week's hours come from the Disc_Hour tab that the nightly run wrote.
        sh = open_sheet()
        existing = read_tab(sh, "Disc_Hour")
        combined, last_week_found, days = merge_live_hours(existing, hour, today, args.hour_days)
        write_tab(sh, "Disc_Hour", combined)
        print(f"Disc_Hour days in the window (before today): {describe_days(days)}")
        if not last_week_found:
            last_week = (today - timedelta(days=7)).strftime("%Y-%m-%d")
            print(f"ERROR: last week's day ({last_week}) is not in the Disc_Hour tab, so the live growth has no base. "
                  "Run the Discount Code rows workflow for that day (from and to = its date), then this job again.")
            sys.exit(1)
        return
    if rows is None:
        raise SystemExit("No completed-day rows. Run the Discount Code rows workflow first.")
    lab, itm = aggregate(rows, args.keep_days)
    product_rows = keep_store(read_csv_if_present(args.products), args.store_type, "product rows")
    if product_rows is None:
        print("Warning: no product rows file. Product tabs use Swiggy and Zomato only.")
        product_rows = rows
    else:
        print(f"product rows: {len(product_rows)} item rows, all channels")
    bucket, prod, pbucket = build_product_tabs(product_rows, args.product_days)
    recon_rows = keep_store(read_csv_if_present(args.recon), args.store_type, "reconciliation rows")
    recon_tab = build_recon_tab(recon_rows)
    if recon_rows is None:
        print("Warning: no reconciliation file. Disc_Recon is not written.")
    else:
        net_gap = recon_tab["Net gap"].sum()
        print(f"reconciliation: sales net {recon_tab['Sales net'].sum():,.2f}, product net {recon_tab['Product net'].sum():,.2f}, "
              f"net gap {net_gap:,.2f}")
        if abs(net_gap) > 1 or abs(recon_tab["Gross gap"].sum()) > 1:
            print("WARNING: product totals do not match the order totals. Check the Disc_Recon tab.")
    print(f"{len(lab)} label rows, {len(itm)} item rows, days {lab['Date'].min()} to {lab['Date'].max()}")
    print(f"product tabs (last {args.product_days} days): bucket {len(bucket)}, product {len(prod)}, "
          f"product by bucket {len(pbucket)} rows")
    total_cells = (len(lab) * len(LABEL_COLS) + len(itm) * len(ITEM_COLS) + len(hour) * len(HOUR_COLS)
                   + len(bucket) * len(BUCKET_COLS) + len(prod) * len(PROD_COLS) + len(pbucket) * len(PBUCKET_COLS)
                   + len(recon_tab) * len(RECON_NAMES))
    print(f"cells written: about {total_cells:,} of the Google Sheets limit of 10,000,000")
    if total_cells > 8_000_000:
        print("WARNING: close to the Google Sheets cell limit. Lower --product-days or --keep-days.")
    tabs = [("Disc_Label", lab), ("Disc_Item", itm), ("Disc_Hour", hour),
            ("Disc_Bucket", bucket), ("Disc_Prod", prod), ("Disc_Prod_Bucket", pbucket)]
    if recon_rows is not None:
        tabs.append(("Disc_Recon", recon_tab))
    write_sheet(tabs)


if __name__ == "__main__":
    main()
