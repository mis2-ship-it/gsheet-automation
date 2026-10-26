#!/usr/bin/env python3
"""
Discount Code data: Swiggy and Zomato orders from Rista's sales/page endpoint.

One row per item line of a Closed order. Each row carries its order's discount label, so the
dashboard counts each order once, under that label.

Rules:
- channel contains Swiggy or Zomato. status must be "Closed" (Cancelled, Voided and Open are left out).
- a discount name shows the text inside its brackets: "Merchant Voucher Code (TRYNEW)" -> "TRYNEW",
  "Restaurant Discount (15% off)" -> "15% Off". A name without brackets is used as it is ("SALT").
- a "?" before a number is read as the rupee sign: "Flat ?125 off" -> "Flat ₹125 Off".
- the voucher in `discounts` and the item discounts in `item_discounts` are combined into one label
  per order, for example "15% Off + TRYNEW". No discount at all gives "Zero discount".
- a Swiggy order whose tags contain "Toing" gets "Toing Discount" for its item discounts.
- discount per item = abs(item_baseNetDiscountAmount). That figure already includes the Zomato voucher.
- the API sends each order with its items inside a list. Each item becomes one row, and the item's own
  fields are named item_<field>. Order fields keep their names.

Usage: python sync_discounts.py --out data/discount_rows.csv [--from 2026-09-01] [--to YYYY-MM-DD | yesterday]
"""
import argparse
import ast
import os
import re
import sys
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import pandas as pd

OUT_COLUMNS = ["day", "branchCode", "store", "region", "ownership", "brand", "source", "session",
               "invoiceNumber", "category", "item", "qty", "gross", "disc", "net", "label", "bh"]
SESSION_ALIAS = {"Late Night": "Post Dinner", "Closing": "Post Dinner"}
BRACKET = re.compile(r"\((.*)\)")
FIRST_DAY = "2026-09-01"
ITEM_HINTS = ("quantity", "shortName", "baseGrossAmount", "baseNetAmount")


def parse_list(v):
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return []
    s = str(v).strip()
    if s in ("", "[]", "nan", "None"):
        return []
    try:
        out = ast.literal_eval(s)
        return out if isinstance(out, list) else []
    except (ValueError, SyntaxError):
        return []


def title_label(text):
    text = re.sub(r"\?(?=\d)", "₹", str(text).strip())
    return " ".join(w if w.isupper() else w[:1].upper() + w[1:] for w in text.split())


def bracket_label(name):
    m = BRACKET.search(str(name))
    return title_label(m.group(1) if m else name)


def session_of(v):
    s = "" if v is None or (isinstance(v, float) and pd.isna(v)) else str(v).strip()
    if not s or s.lower() == "nan":
        return "Not tagged"
    return SESSION_ALIAS.get(s, s)


def business_hour_series(values):
    """Business hour of each order: 0 = 9 AM, 19 = 4 AM (India time). -1 when the time is missing."""
    ts = pd.to_datetime(values, errors="coerce", utc=True)
    hour = ts.dt.tz_convert("Asia/Kolkata").dt.hour
    bh = (hour - 9) % 24
    return bh.where(ts.notna(), -1).astype(int)


def is_list_of_dicts(v):
    return isinstance(v, list) and len(v) > 0 and isinstance(v[0], dict)


def expand_items(raw):
    """sales/page sends one record per order, with the items in a list. This makes one row per item:
    the item's fields get the item_ prefix, and order fields keep their names."""
    if raw is None or raw.empty:
        return raw
    if "item_shortName" in raw.columns or "item_quantity" in raw.columns:
        return raw
    def has_item_keys(col):
        for v in raw[col]:
            if is_list_of_dicts(v):
                return any(h in v[0] for h in ITEM_HINTS)
        return False
    item_cols = [c for c in raw.columns if has_item_keys(c)]
    if not item_cols:
        first = raw.to_dict("records")[0]
        nested = {k: list(v[0].keys()) for k, v in first.items() if is_list_of_dicts(v)}
        raise SystemExit("sales/page: no item list found in the records. Keys of the first record: "
                         f"{sorted(first.keys())[:40]}. Nested lists: {nested}")
    col = item_cols[0]
    rows = []
    for rec in raw.to_dict("records"):
        items = rec.pop(col, None)
        if not is_list_of_dicts(items):
            continue
        for it in items:
            row = dict(rec)
            for k, v in it.items():
                row["item_" + k] = v
            rows.append(row)
    return pd.DataFrame(rows)


def transform(raw, day, branches):
    """raw: sales/page rows (one or more days), one row per order or per item.
    day: keep only this day (None = keep all). branches: Help Sheet rows with branchCode, store, region, ownership."""
    cols = ["channel", "status", "invoiceNumber", "branchCode", "brandName", "item_shortName",
            "item_categoryName", "item_quantity", "item_baseGrossAmount", "item_baseNetDiscountAmount",
            "item_baseNetAmount"]
    if raw is None or raw.empty:
        return pd.DataFrame(columns=OUT_COLUMNS)
    raw = expand_items(raw)
    missing = [c for c in cols if c not in raw.columns]
    if missing:
        raise SystemExit(f"sales/page is missing columns: {missing}. Columns received: {list(raw.columns)}")
    df = raw.copy()
    for c in ["sessionLabel", "tags", "discounts", "item_discounts", "invoiceDay", "invoiceDate"]:
        if c not in df.columns:
            df[c] = ""
    df["status"] = df["status"].astype(str).str.strip()
    is_online = df["channel"].astype(str).str.lower().str.contains("swiggy|zomato", na=False)
    df = df[is_online & (df["status"] == "Closed")].copy()
    df["day"] = df["invoiceDay"].astype(str).str[:10]
    if day:
        df = df[df["day"] == day]
    if df.empty:
        return pd.DataFrame(columns=OUT_COLUMNS)

    df["source"] = df["channel"].astype(str).str.lower().map(lambda c: "Swiggy" if "swiggy" in c else "Zomato")
    df["branchCode"] = df["branchCode"].astype(str).str.strip().str.replace(r"\.0$", "", regex=True)
    df["toing"] = df["tags"].astype(str).str.contains("toing", case=False, na=False) & (df["source"] == "Swiggy")
    df["voucher_names"] = df["discounts"].map(lambda v: [bracket_label(x.get("name", "")) for x in parse_list(v)])
    df["item_names"] = [
        ["Toing Discount" if toing else bracket_label(x.get("name", "")) for x in parse_list(v)]
        for v, toing in zip(df["item_discounts"], df["toing"])
    ]
    label_of = {}
    for inv, g in df.groupby("invoiceNumber"):
        parts = set()
        for lst in list(g["voucher_names"]) + list(g["item_names"]):
            parts.update(lst)
        label_of[inv] = " + ".join(sorted(parts)) if parts else "Zero discount"
    df["label"] = df["invoiceNumber"].map(label_of)

    df["bh"] = business_hour_series(df["invoiceDate"]).values
    df["session"] = df["sessionLabel"].map(session_of)
    df["brand"] = df["brandName"].astype(str).str.strip()
    df["category"] = df["item_categoryName"].astype(str).str.strip()
    df["item"] = df["item_shortName"].astype(str).str.strip()
    df["invoiceNumber"] = df["invoiceNumber"].astype(str).str.strip()
    df["qty"] = pd.to_numeric(df["item_quantity"], errors="coerce").fillna(0)
    df["gross"] = pd.to_numeric(df["item_baseGrossAmount"], errors="coerce").fillna(0).round(2)
    df["disc"] = pd.to_numeric(df["item_baseNetDiscountAmount"], errors="coerce").fillna(0).abs().round(2)
    df["net"] = pd.to_numeric(df["item_baseNetAmount"], errors="coerce").fillna(0).round(2)

    if branches is None or branches.empty:
        raise SystemExit("The branch list (Help Sheet) is needed for store and region.")
    b = branches.copy()
    b["branchCode"] = b["branchCode"].astype(str).str.strip()
    out = df.merge(b[["branchCode", "store", "region", "ownership"]], on="branchCode", how="inner")
    return out[OUT_COLUMNS]


def parse_day(text, default):
    """'' or 'default' = the default; 'today' and 'yesterday' = business days (a business day ends at 05:00 next morning)."""
    t = (text or "").strip().lower()
    if t in ("", "default"):
        return default
    business_today = (datetime.now(ZoneInfo("Asia/Kolkata")) - timedelta(hours=5)).date()
    if t == "today":
        return business_today
    if t == "yesterday":
        return business_today - timedelta(days=1)
    return datetime.strptime(t, "%Y-%m-%d").date()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default="data/discount_rows.csv")
    ap.add_argument("--from", dest="start", default="")
    ap.add_argument("--to", dest="end", default="")
    ap.add_argument("--refresh-days", type=int, default=2, help="the last days are always pulled again")
    ap.add_argument("--workers", type=int, default=4)
    ap.add_argument("--live", action="store_true", help="pull today's business day only, to a separate file")
    args = ap.parse_args()
    from sync_kpt import load_branches, pull_day     # same Rista and Help Sheet settings as the KPT pull

    today = datetime.now(ZoneInfo("Asia/Kolkata")).date()
    start = parse_day(args.start, datetime.strptime(FIRST_DAY, "%Y-%m-%d").date())
    end = parse_day(args.end, today - timedelta(days=1))
    if args.live:
        start = end = (datetime.now(ZoneInfo("Asia/Kolkata")) - timedelta(hours=5)).date()
    if end < start:
        sys.exit("The end day is before the start day. Nothing to pull.")
    start_s, end_s = start.strftime("%Y-%m-%d"), end.strftime("%Y-%m-%d")

    branches = load_branches(None)
    codes = branches["branchCode"].tolist()
    days = [start + timedelta(days=i) for i in range((end - start).days + 1)]
    saved = None
    if os.path.exists(args.out) and os.path.getsize(args.out) > 0 and not args.live:
        saved = pd.read_csv(args.out, dtype=str, keep_default_na=False)
        if "day" not in saved.columns:
            saved = None
    refresh_from = end - timedelta(days=max(args.refresh_days, 1) - 1)
    if saved is None:
        keep = pd.DataFrame(columns=OUT_COLUMNS)
        pull_dates = days
    else:
        have = set(saved["day"])
        pull_dates = [d for d in days if d >= refresh_from or d.strftime("%Y-%m-%d") not in have]
        redo = {d.strftime("%Y-%m-%d") for d in pull_dates}
        keep = saved[(~saved["day"].isin(redo)) & (saved["day"] >= start_s) & (saved["day"] <= end_s)]
        print(f"Saved file: {len(keep)} rows kept; {len(pull_dates)} day(s) pulled")

    frames = []
    for d in pull_dates:
        ds = d.strftime("%Y-%m-%d")
        raw = pull_day(codes, ds, args.workers)
        got = transform(raw, ds, branches)
        print(f"{ds}: {len(raw)} rows from Rista, {len(got)} closed Swiggy/Zomato item rows")
        frames.append(got)
    new = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame(columns=OUT_COLUMNS)
    parts = [p for p in [keep, new] if not p.empty]
    out = pd.concat(parts, ignore_index=True) if parts else pd.DataFrame(columns=OUT_COLUMNS)
    out = out[OUT_COLUMNS].sort_values(["day", "store"]).reset_index(drop=True)
    os.makedirs(os.path.dirname(args.out) or ".", exist_ok=True)
    out.to_csv(args.out, index=False)
    print(f"wrote {args.out}: {len(out)} rows, {out['day'].nunique() if len(out) else 0} days")


if __name__ == "__main__":
    main()
