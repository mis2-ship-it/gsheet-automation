#!/usr/bin/env python3
"""
Pull order timings for the KPT and O2D dashboard from Rista's sales/page endpoint.

Same source and rules as kpt_o2d_mtd.py:
  - branches come from the Help Sheet. Default: COCO and FOFO (--ownership COCO for COCO only, ALL for every branch)
  - online orders only: the channel contains "swiggy" or "zomato"
  - the order's invoiceDay must equal the day pulled
  - order time = invoiceDate, ready time = orderReadyTimestamp, delivery time = modifiedDate
  - KPT = ready - order and O2D = delivery - order, in minutes; negatives become 0; rows with no KPT are dropped

Saved file: days already in --out are kept. Only the last --refresh-days days, and any day missing from the file,
are pulled again, so a nightly run takes minutes. A file without an ownership column, or --full, pulls the whole range.
Branches are fetched --workers at a time. Rows outside the range are dropped from the file.

Needs API_KEY, SECRET_KEY and GOOGLE_CREDENTIALS.
Usage: python sync_kpt.py --out data/kpt_orders.csv [--from 2026-10-01] [--to 2026-10-18] [--ownership COCO,FOFO]
"""
import argparse
import json
import os
import re
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import jwt
import pandas as pd
import requests

HELP_SHEET_ID = "19z6KkVBFoLC33_wcNqVhDLyQEC2dDQ8YQE0gE38BhVg"
HELP_TAB = "Help Sheet"
SALES_URL = "https://api.ristaapps.com/v1/sales/page"
KPT_LIMIT, O2D_LIMIT = 12, 30   # minutes: the SLA lines used by the dashboard
OUT_COLUMNS = ["day", "branchCode", "store", "region", "ownership", "channel", "brand", "session",
               "order_time", "ready_time", "delivery_time", "kpt_min", "o2d_min"]


def headers():
    token = jwt.encode({"iss": os.environ["API_KEY"], "iat": int(time.time())},
                       os.environ["SECRET_KEY"], algorithm="HS256")
    return {"x-api-key": os.environ["API_KEY"], "x-api-token": token, "content-type": "application/json"}


def parse_ownerships(text):
    text = text.strip().upper()
    if text == "ALL":
        return None
    return [t.strip() for t in text.split(",") if t.strip()]


def filter_branches(df, ownerships):
    """df: Help Sheet rows with lower-case column names (branchcode, storename, region, ownership)."""
    df = df.copy()
    df["branchcode"] = df["branchcode"].astype(str).str.strip()
    df["ownership"] = df["ownership"].astype(str).str.strip().str.upper()
    df = df[df["branchcode"] != ""]
    if ownerships is not None:
        df = df[df["ownership"].isin(ownerships)]
    df = df.copy()
    df["storename"] = df["storename"].astype(str).map(lambda s: " ".join(s.split()))
    out = df.rename(columns={"branchcode": "branchCode", "storename": "store"})[["branchCode", "store", "region", "ownership"]]
    return out.drop_duplicates("branchCode")


def load_branches(ownerships):
    import gspread
    from google.oauth2.service_account import Credentials
    creds = Credentials.from_service_account_info(
        json.loads(os.environ["GOOGLE_CREDENTIALS"]),
        scopes=["https://www.googleapis.com/auth/spreadsheets.readonly"])
    values = gspread.authorize(creds).open_by_key(HELP_SHEET_ID).worksheet(HELP_TAB).get_all_values()
    head = [re.sub(r"\s+", "", h).lower() for h in values[0]]
    df = pd.DataFrame([(list(r) + [""] * len(head))[:len(head)] for r in values[1:]], columns=head)
    missing = [c for c in ["branchcode", "storename", "region", "ownership"] if c not in df.columns]
    if missing:
        sys.exit(f"The Help Sheet is missing columns {missing}. Columns found: {head}")
    return filter_branches(df, ownerships)


def fetch_branch_day(branch, day):
    r = None
    for attempt in range(3):
        try:
            r = requests.get(SALES_URL, headers=headers(), params={"branch": branch, "day": day}, timeout=120)
        except requests.RequestException as e:
            print(f"WARNING branch {branch} day {day}: {e}")
            time.sleep(5)
            continue
        if r.status_code == 200:
            data = r.json().get("data", [])
            return pd.json_normalize(data) if data else pd.DataFrame()
        time.sleep(5 if r.status_code == 429 else 3)
    code = r.status_code if r is not None else "no response"
    print(f"WARNING branch {branch} day {day}: the sales endpoint returned {code}")
    return pd.DataFrame()


def pull_day(codes, day, workers):
    with ThreadPoolExecutor(max_workers=max(1, workers)) as pool:
        parts = list(pool.map(lambda code: fetch_branch_day(code, day), codes))
    parts = [p for p in parts if not p.empty]
    return pd.concat(parts, ignore_index=True) if parts else pd.DataFrame()


def find_col(df, exact, contains=None, avoid=()):
    lower = {c.lower(): c for c in df.columns}
    for name in exact:
        if name.lower() in lower:
            return lower[name.lower()]
    if contains:
        for c in df.columns:
            cl = c.lower()
            if contains in cl and not any(a in cl for a in avoid):
                return c
    return None


def as_time(df, col):
    return pd.to_datetime(df[col], errors="coerce") if col else pd.Series(pd.NaT, index=df.index)


def transform(raw, day, branches, report=None):
    """raw: sales/page rows for one day, all branches. Returns one row per online order with its timings."""
    if raw.empty:
        return pd.DataFrame(columns=OUT_COLUMNS)
    df = raw.copy()
    if "branchCode" not in df.columns:
        print("WARNING: branchCode is missing from the response; nothing kept for", day)
        return pd.DataFrame(columns=OUT_COLUMNS)
    df["branchCode"] = df["branchCode"].astype(str).str.strip()

    chan_col = find_col(df, ["Channel", "channel"])
    channel = df[chan_col].astype(str).str.strip() if chan_col else pd.Series("", index=df.index)
    df = df[channel.str.lower().str.contains("swiggy|zomato", na=False)].copy()
    df["channel"] = channel[df.index].values

    inv_day = find_col(df, ["invoiceDay"])
    if inv_day:
        df = df[df[inv_day].astype(str) == day].copy()

    df = df.merge(branches, on="branchCode", how="inner")
    dropped = len(raw) - len(df)

    brand_col = find_col(df, ["brandName", "brand"], contains="brand", avoid=("branch",))
    session_col = find_col(df, ["sessionName", "session"], contains="session")
    order = as_time(df, find_col(df, ["invoiceDate"]))
    ready = as_time(df, find_col(df, ["orderReadyTimestamp"]))
    delivery = as_time(df, find_col(df, ["modifiedDate"]))

    out = pd.DataFrame({
        "day": day,
        "branchCode": df["branchCode"].values,
        "store": df["store"].values,
        "region": df["region"].values,
        "ownership": df["ownership"].values,
        "channel": df["channel"].values,
        "brand": df[brand_col].astype(str).values if brand_col else "",
        "session": df[session_col].astype(str).values if session_col else "",
        "order_time": order.values,
        "ready_time": ready.values,
        "delivery_time": delivery.values,
    })
    out["kpt_min"] = ((ready - order).dt.total_seconds() / 60).clip(lower=0).round(1).values
    out["o2d_min"] = ((delivery - order).dt.total_seconds() / 60).clip(lower=0).round(1).values
    out = out[out["kpt_min"].notna()].copy()

    if report is not None:
        report.update({"brand_col": brand_col, "session_col": session_col, "dropped_rows": dropped})
    return out[OUT_COLUMNS]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default="data/kpt_orders.csv")
    ap.add_argument("--from", dest="start", default=None)
    ap.add_argument("--to", dest="end", default=None)
    ap.add_argument("--ownership", default="COCO,FOFO", help="COCO, FOFO, COCO,FOFO (default) or ALL")
    ap.add_argument("--refresh-days", type=int, default=2, help="the last days are always pulled again")
    ap.add_argument("--workers", type=int, default=4, help="branches fetched at the same time")
    ap.add_argument("--full", action="store_true", help="ignore the saved file and pull the whole range")
    args = ap.parse_args()
    began = time.time()

    today = datetime.now(ZoneInfo("Asia/Kolkata")).date()
    start = datetime.strptime(args.start, "%Y-%m-%d").date() if args.start else today.replace(day=1)
    end = datetime.strptime(args.end, "%Y-%m-%d").date() if args.end else today - timedelta(days=1)
    if end < start:
        sys.exit("The end day is before the start day. Nothing to pull.")
    start_s, end_s = start.strftime("%Y-%m-%d"), end.strftime("%Y-%m-%d")

    ownerships = parse_ownerships(args.ownership)
    branches = load_branches(ownerships)
    label = "ALL" if ownerships is None else ", ".join(ownerships)
    print(f"{len(branches)} branches ({label}) from the Help Sheet; range {start_s} to {end_s}")

    days = [start + timedelta(days=i) for i in range((end - start).days + 1)]
    saved = None
    if not args.full and os.path.exists(args.out) and os.path.getsize(args.out) > 0:
        saved = pd.read_csv(args.out, dtype=str)
        if "ownership" not in saved.columns or "day" not in saved.columns:
            print("The saved file has no ownership column, so the whole range is pulled again.")
            saved = None

    refresh_from = end - timedelta(days=max(args.refresh_days, 1) - 1)
    if saved is None:
        keep = pd.DataFrame(columns=OUT_COLUMNS)
        pull_dates = days
    else:
        have = set(saved["day"])
        pull_dates = [d for d in days if d >= refresh_from or d.strftime("%Y-%m-%d") not in have]
        pulled = {d.strftime("%Y-%m-%d") for d in pull_dates}
        keep = saved[(~saved["day"].isin(pulled)) & (saved["day"] >= start_s) & (saved["day"] <= end_s)]
        print(f"Saved file: {len(keep)} orders kept; {len(pull_dates)} day(s) pulled again")

    codes = branches["branchCode"].tolist()
    frames = []
    for d in pull_dates:
        day_str = d.strftime("%Y-%m-%d")
        raw = pull_day(codes, day_str, args.workers)
        report = {}
        got = transform(raw, day_str, branches, report)
        print(f"{day_str}: {len(raw)} rows from Rista, {len(got)} online orders with KPT "
              f"(rows dropped: other channels, other days, or branches not in the Help Sheet: {report.get('dropped_rows', 0)})")
        if report.get("brand_col") is None or report.get("session_col") is None:
            print(f"  note: brand column = {report.get('brand_col')}, session column = {report.get('session_col')}")
        frames.append(got)

    new = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame(columns=OUT_COLUMNS)
    parts = [p for p in [keep, new] if not p.empty]
    out = pd.concat(parts, ignore_index=True) if parts else pd.DataFrame(columns=OUT_COLUMNS)
    out = out[OUT_COLUMNS].sort_values(["day", "branchCode"]).reset_index(drop=True)
    os.makedirs(os.path.dirname(args.out) or ".", exist_ok=True)
    out.to_csv(args.out, index=False)

    if not out.empty:
        kpt = pd.to_numeric(out["kpt_min"], errors="coerce")
        o2d = pd.to_numeric(out["o2d_min"], errors="coerce")
        print("ownership:", out["ownership"].value_counts().to_dict())
        print("brand values:", sorted(out["brand"].dropna().unique())[:10])
        print("session values:", sorted(out["session"].dropna().unique())[:12])
        print(f"KPT over {KPT_LIMIT} min: {(kpt > KPT_LIMIT).sum()} orders; "
              f"O2D over {O2D_LIMIT} min: {(o2d > O2D_LIMIT).sum()} orders")
    print(f"wrote {args.out} with {len(out)} orders in {(time.time() - began) / 60:.1f} minutes")


if __name__ == "__main__":
    main()
