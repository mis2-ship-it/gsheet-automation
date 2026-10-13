#!/usr/bin/env python3
"""
Pull sales from Rista and write rows in the historical file's layout.

  --mode nightly  completed business days from --since up to the last completed day
                  -> data/new_sales.csv.gz   (commit this file; the live run reads it)
  --mode live     today's business day so far (09:00 to now)
                  -> data/live_sales.csv

Uses the same API calls and mapping as rista_live.py:
  - Rista: branch/list and sales/summary (JWT auth with API_KEY and SECRET_KEY)
  - Sales Dashboard sheet, tab 'Help Sheet': store type and region (G:M), channel -> source and brand (D:F)
  - Net sales = netAmount + chargeAmount, closed invoices only (as in rista_live.py)
  - Business day cut-off at 05:00 for each invoice (as in prepare_dates)

Failed branch-days are listed in data/sync_errors.csv and make the job exit with an error,
so a missing store is never silently dropped.
"""
import argparse
import json
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta

import jwt
import pandas as pd
import requests

RISTA = "https://api.ristaapps.com/v1"
# Session names in the live script are Breakfast, Lunch, Snacks, Dinner and Post Dinner.
# The historical file uses Late Night and Closing. Post Dinner is mapped to Late Night here
# until the business confirms the historical rule.
SESSION_MAP = {"Post Dinner": "Late Night"}
OUT_COLS = ["Brand Name", "Date", "Branch", "Source", "Session", "Store Type", "Region",
            "Net Sales", "Discount", "Gross Sales", "Orders"]


def session_for_hour(h):
    if 8 <= h <= 11: name = "Breakfast"
    elif 12 <= h <= 15: name = "Lunch"
    elif 16 <= h <= 19: name = "Snacks"
    elif 20 <= h <= 23: name = "Dinner"
    else: name = "Post Dinner"
    return SESSION_MAP.get(name, name)


def business_day_now(now):
    """The business day in progress: before 06:00 it is still yesterday's."""
    return (now - timedelta(hours=6)).date()


def headers():
    token = jwt.encode({"iss": os.environ["API_KEY"], "iat": int(time.time())},
                       os.environ["SECRET_KEY"], algorithm="HS256")
    return {"x-api-key": os.environ["API_KEY"], "x-api-token": token, "content-type": "application/json"}


def get_json(url, params=None, retries=3):
    last = None
    for attempt in range(retries):
        try:
            r = requests.get(url, headers=headers(), params=params, timeout=30)
            if r.status_code == 200:
                return r.json()
            last = f"HTTP {r.status_code}"
        except requests.RequestException as e:
            last = str(e)
        time.sleep(2 * (attempt + 1))
    raise RuntimeError(f"{url} {params}: {last}")


def active_branches():
    data = get_json(f"{RISTA}/branch/list")
    data = data.get("data", []) if isinstance(data, dict) else data
    return [b["branchCode"] for b in data if b.get("status") == "Active"]


def fetch_branch_day(branch, day):
    """All invoices for one branch and one day, following lastKey pages."""
    rows, last_key = [], None
    while True:
        params = {"branch": branch, "day": day}
        if last_key:
            params["lastKey"] = last_key
        js = get_json(f"{RISTA}/sales/summary", params=params)
        data = js.get("data", [])
        if not data:
            break
        rows.extend(data)
        last_key = js.get("lastKey")
        if not last_key:
            break
    return rows


def load_maps():
    import gspread
    from google.oauth2.service_account import Credentials
    creds = Credentials.from_service_account_info(
        json.loads(os.environ["GOOGLE_CREDENTIALS"]),
        scopes=["https://www.googleapis.com/auth/spreadsheets.readonly"])
    gc = gspread.authorize(creds)
    help_ws = gc.open("Sales Dashboard").worksheet("Help Sheet")

    gh = help_ws.get("G:M")
    branch_master = pd.DataFrame(gh[1:], columns=gh[0])
    for c in ["Store Name", "Ownership", "Region"]:
        branch_master[c] = branch_master[c].astype(str).str.strip()
    store_type = dict(zip(branch_master["Store Name"], branch_master["Ownership"]))
    region = dict(zip(branch_master["Store Name"], branch_master["Region"]))

    sm = help_ws.get("D:F")
    source_master = pd.DataFrame(sm[1:], columns=sm[0])
    for c in ["Channel", "Source Group", "Brand"]:
        source_master[c] = source_master[c].astype(str).str.strip()
    source = dict(zip(source_master["Channel"], source_master["Source Group"]))
    brand = dict(zip(source_master["Channel"], source_master["Brand"]))
    return store_type, region, source, brand


def transform_invoices(inv, store_type, region, source, brand):
    """Turn Rista invoices into rows in the historical file's layout (one row per group)."""
    if inv.empty:
        return pd.DataFrame(columns=OUT_COLS)
    for c in ["netAmount", "chargeAmount", "grossAmount", "discountAmount"]:
        if c not in inv.columns:
            inv[c] = 0
        inv[c] = pd.to_numeric(inv[c], errors="coerce").fillna(0)
    for c in ["status", "branchName", "channel"]:
        if c not in inv.columns:
            inv[c] = ""
    inv = inv[inv["status"] == "Closed"].copy()
    if inv.empty:
        return pd.DataFrame(columns=OUT_COLS)

    dt = pd.to_datetime(inv["invoiceDate"], errors="coerce").dt.tz_localize(None)
    hour = dt.dt.hour
    biz = dt.apply(lambda x: (x - pd.Timedelta(days=1)).date() if x.hour < 5 else x.date())
    branch = inv["branchName"].astype(str).str.strip()
    channel = inv["channel"].astype(str).str.strip()

    out = pd.DataFrame({
        "Brand Name": channel.map(brand),
        "Date": pd.to_datetime(biz).dt.strftime("%Y-%m-%d"),
        "Branch": branch,
        "Source": channel.map(source).fillna("Others"),
        "Session": hour.apply(session_for_hour),
        "Store Type": branch.map(store_type).fillna("Unmapped"),
        "Region": branch.map(region).fillna("Unmapped"),
        "Net Sales": inv["netAmount"] + inv["chargeAmount"],
        "Discount": inv["discountAmount"],
        "Gross Sales": inv["grossAmount"],
        "Orders": 1,
    })
    out = out.dropna(subset=["Brand Name"])        # unknown channels, as rows without a brand
    out = out.groupby(["Brand Name", "Date", "Branch", "Source", "Session", "Store Type", "Region"],
                      as_index=False).sum(numeric_only=True)
    return out[OUT_COLS]


def run_days(days, branches, maps, workers=8):
    rows, errors = [], []
    jobs = {}
    with ThreadPoolExecutor(max_workers=workers) as ex:
        for day in days:
            for b in branches:
                jobs[ex.submit(fetch_branch_day, b, day)] = (b, day)
        for fut in as_completed(jobs):
            b, day = jobs[fut]
            try:
                rows.extend(fut.result())
            except Exception as e:
                errors.append({"branch": b, "day": day, "error": str(e)[:200]})
    inv = pd.json_normalize(rows) if rows else pd.DataFrame()
    return transform_invoices(inv, *maps), errors


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--mode", choices=["nightly", "live"], required=True)
    ap.add_argument("--since", default="2026-09-01", help="first day to pull in nightly mode")
    ap.add_argument("--out", required=True)
    args = ap.parse_args()

    now = datetime.now()
    today_bd = business_day_now(now)
    if args.mode == "live":
        days = [today_bd.strftime("%Y-%m-%d")]
    else:
        start = datetime.strptime(args.since, "%Y-%m-%d").date()
        end = today_bd - timedelta(days=1)
        days = [(start + timedelta(days=i)).strftime("%Y-%m-%d") for i in range((end - start).days + 1)]
    if not days:
        print("Nothing to pull.")
        return 0

    branches = active_branches()
    maps = load_maps()
    out, errors = run_days(days, branches, maps)
    os.makedirs(os.path.dirname(args.out) or ".", exist_ok=True)
    compression = "gzip" if args.out.endswith(".gz") else None
    out.to_csv(args.out, index=False, compression=compression)
    print(f"{args.mode}: {len(days)} day(s), {len(branches)} branches, {len(out)} rows -> {args.out}")

    err_path = os.path.join(os.path.dirname(args.out) or ".", "sync_errors.csv")
    if errors:
        pd.DataFrame(errors).to_csv(err_path, index=False)
        print(f"{len(errors)} branch-day pulls failed, see {err_path}")
        return 1
    if os.path.exists(err_path):
        os.remove(err_path)
    return 0


if __name__ == "__main__":
    sys.exit(main())
