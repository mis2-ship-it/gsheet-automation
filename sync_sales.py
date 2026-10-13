#!/usr/bin/env python3
"""
Pull sales from Rista for the Frozen Bottle dashboard.

Business day: 9 AM to 5 AM the next morning. An invoice before 5 AM belongs to the day before.
Business hour: 0 = 9 AM, 1 = 10 AM ... 14 = 11 PM, 15 = 12 AM ... 19 = 4 AM.

  --mode nightly  completed business days from --since up to the last completed day
                  -> --out         daily rows (data/new_sales.csv.gz)
                  -> --hourly-out  hourly rows for the last 14 business days (data/new_hourly.csv.gz)
  --mode live     today's business day so far (9 AM to now)
                  -> --out         hourly rows for today (data/live_sales.csv)
                  -> live_meta.json beside it: the business day and the time of the pull

Uses the same API calls as rista_live.py:
  - Rista: branch/list and sales/summary (JWT auth with API_KEY and SECRET_KEY)
  - POS_Master_Dashboard sheet, tab 'Region_Help_Sheet': branch -> region and store type (A:C),
    channel -> source group and brand (D:F)
  - Net sales = netAmount + chargeAmount, closed invoices only (as in rista_live.py)

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
from zoneinfo import ZoneInfo

import jwt
import pandas as pd
import requests

RISTA = "https://api.ristaapps.com/v1"
MASTER_SHEET_ID = "1g4vuRZPy7qsUvDzF5yYM60VKWTL2r0VSDvtvNl06hiY"   # POS_Master_Dashboard
MASTER_TAB = "Region_Help_Sheet"
# Source Group names in the sheet -> the sources used on the dashboard.
# Website, Magicpin and HOGR Dine-In count as Others (online). Ownly stays as its own source.
SOURCE_FIX = {"Website": "Others", "Magicpin": "Others", "HOGR": "Others"}
OUT_COLS = ["Brand Name", "Date", "Branch", "Source", "Session", "Store Type", "Region",
            "Net Sales", "Discount", "Gross Sales", "Orders"]
HOURLY_COLS = OUT_COLS + ["Hour"]
KEY_COLS = ["Brand Name", "Date", "Branch", "Source", "Session", "Store Type", "Region"]
HISTORY_DAYS = 14   # hourly rows kept in the repo, for same-hour comparisons


def session_for_hour(h):
    if 8 <= h <= 11: name = "Breakfast"
    elif 12 <= h <= 15: name = "Lunch"
    elif 16 <= h <= 19: name = "Snacks"
    elif 20 <= h <= 23: name = "Dinner"
    else: name = "Post Dinner"
    return name   # Late Night and Closing in the history are merged into Post Dinner in build_site.py


def business_day_now(now):
    """The business day in progress. Until 05:00 it is still the previous day's."""
    return (now - timedelta(hours=5)).date()


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
    ws = gc.open_by_key(MASTER_SHEET_ID).worksheet(MASTER_TAB)

    # Store master, columns A:C = Branch, Region, Store Type (COCO or FOFO)
    store_type, region = {}, {}
    for row in ws.get("A:C")[1:]:
        row = row + ["", "", ""]
        name = " ".join(row[0].split())
        if name:
            region[name] = row[1].strip() or "Unmapped"
            store_type[name] = row[2].strip() or "Unmapped"

    # Channel master, columns D:F = Channel, Source Group, Brand
    source, brand = {}, {}
    for row in ws.get("D:F")[1:]:
        row = row + ["", "", ""]
        channel = " ".join(row[0].split())
        if channel:
            group = row[1].strip() or "Others"
            source[channel] = SOURCE_FIX.get(group, group)
            brand[channel] = row[2].strip()

    print(f"Master sheet: {len(store_type)} branches, {len(brand)} channels.")
    return store_type, region, source, brand


def transform_invoices(inv, store_type, region, source, brand):
    """Turn Rista invoices into hourly rows: one row per group and business hour."""
    if inv.empty:
        return pd.DataFrame(columns=HOURLY_COLS)
    for c in ["netAmount", "chargeAmount", "grossAmount", "discountAmount"]:
        if c not in inv.columns:
            inv[c] = 0
        inv[c] = pd.to_numeric(inv[c], errors="coerce").fillna(0)
    for c in ["status", "branchName", "channel"]:
        if c not in inv.columns:
            inv[c] = ""
    inv = inv[inv["status"] == "Closed"].copy()
    if inv.empty:
        return pd.DataFrame(columns=HOURLY_COLS)

    dt = pd.to_datetime(inv["invoiceDate"], errors="coerce").dt.tz_localize(None)
    hour = dt.dt.hour
    biz = dt.apply(lambda x: (x - pd.Timedelta(days=1)).date() if x.hour < 5 else x.date())
    branch = inv["branchName"].astype(str).str.split().str.join(" ")
    channel = inv["channel"].astype(str).str.split().str.join(" ")

    unknown_branches = sorted(set(branch) - set(store_type))
    if unknown_branches:
        print(f"Warning: {len(unknown_branches)} branch(es) not in the master sheet, saved as Unmapped: "
              + ", ".join(unknown_branches))
    no_brand = channel.map(brand).isna()
    if no_brand.any():
        print(f"Warning: {int(no_brand.sum())} invoice(s) from channels not in the master sheet were left out: "
              + ", ".join(sorted(set(channel[no_brand]))))

    out = pd.DataFrame({
        "Brand Name": channel.map(brand),
        "Date": pd.to_datetime(biz).dt.strftime("%Y-%m-%d"),
        "Branch": branch,
        "Source": channel.map(source).fillna("Others"),
        "Session": hour.apply(session_for_hour),
        "Store Type": branch.map(store_type).fillna("Unmapped"),
        "Region": branch.map(region).fillna("Unmapped"),
        "Hour": (hour - 9) % 24,
        "Net Sales": inv["netAmount"] + inv["chargeAmount"],
        "Discount": inv["discountAmount"],
        "Gross Sales": inv["grossAmount"],
        "Orders": 1,
    })
    out = out.dropna(subset=["Brand Name"])
    out = out.groupby(KEY_COLS + ["Hour"], as_index=False).sum(numeric_only=True)
    return out[HOURLY_COLS]


def daily_from_hourly(h):
    """Daily rows (the layout of the historical file) from the hourly rows."""
    if h.empty:
        return pd.DataFrame(columns=OUT_COLS)
    d = h.drop(columns=["Hour"]).groupby(KEY_COLS, as_index=False).sum(numeric_only=True)
    return d[OUT_COLS]


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


def read_existing(path):
    """Rows already saved by earlier runs, or None if there are none yet."""
    if not os.path.exists(path):
        return None
    old = pd.read_csv(path, compression="infer", low_memory=False)
    return old if not old.empty else None


def nightly_start(since, path):
    """First day to pull: the --since date, or the last saved day if that is later.
    The last saved day is pulled again in case it was incomplete."""
    start = datetime.strptime(since, "%Y-%m-%d").date()
    old = read_existing(path)
    if old is not None:
        last = datetime.strptime(str(old["Date"].max())[:10], "%Y-%m-%d").date()
        start = max(start, last)
    return start


def merge_with_existing(old, new, first_day):
    """Keep saved days before first_day and replace everything from first_day onwards."""
    keep = old[old["Date"].astype(str).str[:10] < first_day]
    return pd.concat([keep, new], ignore_index=True)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--mode", choices=["nightly", "live"], required=True)
    ap.add_argument("--since", default="2026-09-01", help="first day to pull in nightly mode")
    ap.add_argument("--out", required=True)
    ap.add_argument("--hourly-out", default="data/new_hourly.csv.gz")
    args = ap.parse_args()

    now = datetime.now(ZoneInfo("Asia/Kolkata"))   # GitHub runners use UTC; the business day is in India time
    today_bd = business_day_now(now)
    out_dir = os.path.dirname(args.out) or "."
    if args.mode == "live":
        days = [today_bd.strftime("%Y-%m-%d")]
    else:
        start = nightly_start(args.since, args.out)
        end = today_bd - timedelta(days=1)
        days = [(start + timedelta(days=i)).strftime("%Y-%m-%d") for i in range((end - start).days + 1)]
    if not days:
        print("Nothing to pull.")
        return 0

    branches = active_branches()
    maps = load_maps()
    hourly, errors = run_days(days, branches, maps)
    os.makedirs(out_dir, exist_ok=True)

    if args.mode == "live":
        hourly.to_csv(args.out, index=False)
        meta = {"business_day": today_bd.strftime("%Y-%m-%d"), "as_of": now.isoformat(timespec="minutes")}
        with open(os.path.join(out_dir, "live_meta.json"), "w", encoding="utf-8") as f:
            json.dump(meta, f)
        print(f"live: business day {today_bd}, {len(branches)} branches, {len(hourly)} hourly rows -> {args.out}")
    else:
        daily = daily_from_hourly(hourly)
        existing = read_existing(args.out)
        if existing is not None:
            daily = merge_with_existing(existing, daily, days[0])
        daily.to_csv(args.out, index=False, compression="gzip" if args.out.endswith(".gz") else None)

        old_h = read_existing(args.hourly_out)
        all_h = merge_with_existing(old_h, hourly, days[0]) if old_h is not None else hourly
        keep_from = (today_bd - timedelta(days=HISTORY_DAYS)).strftime("%Y-%m-%d")
        all_h = all_h[all_h["Date"].astype(str).str[:10] >= keep_from]
        all_h.to_csv(args.hourly_out, index=False,
                     compression="gzip" if args.hourly_out.endswith(".gz") else None)
        print(f"nightly: {len(days)} day(s), {len(branches)} branches, {len(daily)} daily rows -> {args.out}; "
              f"{len(all_h)} hourly rows kept -> {args.hourly_out}")

    err_path = os.path.join(out_dir, "sync_errors.csv")
    if errors:
        pd.DataFrame(errors).to_csv(err_path, index=False)
        print(f"{len(errors)} branch-day pulls failed, see {err_path}")
        return 1
    if os.path.exists(err_path):
        os.remove(err_path)
    return 0


if __name__ == "__main__":
    sys.exit(main())
