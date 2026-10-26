#!/usr/bin/env python3
"""
Discount Code and product data, from Rista's sales/page endpoint. One pull per branch and day gives every output.

Outputs:
  --out           discount rows: Swiggy and Zomato orders, Closed. One row per item line, carrying the order's
                  discount label, so the Overview counts each order once under that label.
  --products-out  product rows: all channels (Swiggy, Zomato, In Store, Ownly, others), Closed, one row per item
                  line, for the last --product-days days. Used by Product performance and Discount analysis.
  --recon-out     reconciliation by day and source: the order totals (counted the way the Sales dashboard counts
                  them) against the product rows. Net and gross must agree; discount is shown for comparison.

How the data is pulled:
- branches: every Active branch in Rista (the same list as the Sales sync), not the KPT Help Sheet.
- every page of each branch and day is read, following lastKey, so no orders are left out.
- a branch-day that still fails after retries stops the run before anything is saved, so the cache is not updated.
- store, region and store type come from the POS master, as on the Sales dashboard. Unknown branches are kept as
  "Unmapped" instead of being dropped.

Rules:
- status must be "Closed" (Cancelled, Voided and Open are left out).
- Net = netAmount + chargeAmount of the order, as on the Sales dashboard. The item rows carry each item's net. The
  difference between the order and its items (charges, tax, rounding) is one row per order named
  "Charges, tax and rounding (not a product)", so the product rows add up to the order totals.
- an order with no item list is kept as one row named "(no item detail)".
- a discount name shows the text inside its brackets: "Merchant Voucher Code (TRYNEW)" -> "TRYNEW",
  "Restaurant Discount (15% off)" -> "15% Off". A name without brackets is used as it is ("SALT").
- a "?" before a number is read as the rupee sign: "Flat ?125 off" -> "Flat ₹125 Off".
- the voucher in `discounts` and the item discounts in `item_discounts` are combined into one label per order,
  for example "15% Off + TRYNEW". No discount at all gives "Zero discount".
- a Swiggy order whose tags contain "Toing" gets "Toing Discount" for its item discounts.
- discount per item = abs(item_baseNetDiscountAmount). On Swiggy and Zomato the platform discount is already in
  the order net, so the order-level discountAmount does not include it. The reconciliation shows that difference.

Usage: python sync_discounts.py --out data/discount_rows.csv --products-out data/product_rows.csv
       --recon-out data/product_recon.csv [--from 2026-09-01] [--to YYYY-MM-DD | yesterday] [--product-days 62]
"""
import argparse
import ast
import os
import re
import sys
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd

OUT_COLUMNS = ["day", "branchCode", "store", "region", "ownership", "brand", "source", "session",
               "invoiceNumber", "category", "item", "qty", "gross", "disc", "net", "label", "bh", "line_flag"]
RECON_COLUMNS = ["day", "source", "orders", "sales_gross", "sales_discount", "sales_net",
                 "product_gross", "product_discount", "product_net", "gross_gap", "net_gap"]
ADJ_ITEM = "Charges, tax and rounding (not a product)"
PLACEHOLDER_ITEM = "(no item detail)"
SESSION_ALIAS = {"Late Night": "Post Dinner", "Closing": "Post Dinner"}
BRACKET = re.compile(r"\((.*)\)")
FIRST_DAY = "2026-09-01"
PRODUCT_DAYS = 62
ITEM_HINTS = ("quantity", "shortName", "baseGrossAmount", "baseNetAmount")


def parse_list(v):
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return []
    if isinstance(v, list):
        return v
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


def product_source(channel, source_map):
    """Source for product rows: the sales dashboard's channel master when available, else from the name."""
    name = " ".join(str(channel).split())
    if source_map and source_map.get(name):
        return source_map[name]
    low = name.lower()
    if "swiggy" in low:
        return "Swiggy"
    if "zomato" in low:
        return "Zomato"
    if "in-store" in low or "in store" in low or "instore" in low:
        return "In Store"
    if "ownly" in low:
        return "Ownly"
    return "Others"


def num(s):
    return pd.to_numeric(s, errors="coerce").fillna(0.0)


def expand_items(raw):
    """sales/page sends one record per order, with the items in a list. This makes one row per item: the item's
    fields get the item_ prefix, and order fields keep their names. An order with no items keeps one row."""
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
            row = dict(rec)
            row.update({"item_shortName": PLACEHOLDER_ITEM, "item_categoryName": "", "item_quantity": 0,
                        "item_baseGrossAmount": 0, "item_baseNetDiscountAmount": 0, "item_baseNetAmount": 0,
                        "is_placeholder": True})
            rows.append(row)
            continue
        for it in items:
            row = dict(rec)
            for k, v in it.items():
                row["item_" + k] = v
            row["is_placeholder"] = False
            rows.append(row)
    return pd.DataFrame(rows)


def transform(raw, day, master):
    """raw: sales/page records (one or more days). day: keep only this day (None = all).
    master: {'store_type', 'region', 'source'} from the POS master (sync_sales.load_maps).
    Returns (rows for all channels with an `online` flag, reconciliation rows for the day and source)."""
    cols = ["channel", "status", "invoiceNumber", "branchCode", "brandName", "item_shortName",
            "item_categoryName", "item_quantity", "item_baseGrossAmount", "item_baseNetDiscountAmount",
            "item_baseNetAmount"]
    empty_rows = pd.DataFrame(columns=OUT_COLUMNS + ["online"])
    empty_recon = pd.DataFrame(columns=RECON_COLUMNS)
    if raw is None or raw.empty:
        return empty_rows, empty_recon
    raw = expand_items(raw)
    missing = [c for c in cols if c not in raw.columns]
    if missing:
        raise SystemExit(f"sales/page is missing columns: {missing}. Columns received: {list(raw.columns)}")
    df = raw.copy()
    for c in ["sessionLabel", "tags", "discounts", "item_discounts", "invoiceDay", "invoiceDate", "branchName",
              "branchCode", "netAmount", "chargeAmount", "grossAmount", "discountAmount"]:
        if c not in df.columns:
            df[c] = ""
    if "is_placeholder" not in df.columns:
        df["is_placeholder"] = False
    df["is_placeholder"] = df["is_placeholder"].fillna(False).astype(bool)
    df["status"] = df["status"].astype(str).str.strip()
    df = df[df["status"] == "Closed"].copy()
    df["day"] = df["invoiceDay"].astype(str).str[:10]
    if day:
        df = df[df["day"] == day]
    if df.empty:
        return empty_rows, empty_recon
    df["invoiceNumber"] = df["invoiceNumber"].astype(str).str.strip()

    channel_text = df["channel"].astype(str)
    df["online"] = channel_text.str.contains("swiggy|zomato", case=False, na=False)
    df["source"] = [("Swiggy" if "swiggy" in c.lower() else "Zomato") if o else product_source(c, master["source"])
                    for c, o in zip(channel_text, df["online"])]
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
    df["qty"] = num(df["item_quantity"])
    df["gross"] = num(df["item_baseGrossAmount"]).round(2)
    df["disc"] = num(df["item_baseNetDiscountAmount"]).abs().round(2)
    df["net"] = num(df["item_baseNetAmount"]).round(2)
    df["store"] = df["branchName"].astype(str).map(lambda s: " ".join(s.split()))
    df["branchCode"] = df["branchCode"].astype(str).str.strip().str.replace(r"\.0$", "", regex=True)
    df["o_net"] = num(df["netAmount"])
    df["o_charge"] = num(df["chargeAmount"])
    df["o_gross"] = num(df["grossAmount"])
    df["o_disc"] = num(df["discountAmount"])
    df["line_flag"] = (~df["is_placeholder"]).astype(int)

    # One line per order: the order totals against its items. The difference is kept as its own row.
    orders = df.groupby("invoiceNumber").agg(
        invoiceNumber_=("invoiceNumber", "first"), day=("day", "first"), source=("source", "first"),
        o_net=("o_net", "first"), o_charge=("o_charge", "first"), o_gross=("o_gross", "first"),
        o_disc=("o_disc", "first"), item_net=("net", "sum"), item_gross=("gross", "sum")).reset_index(drop=True)
    orders["adj_net"] = (orders["o_net"] + orders["o_charge"] - orders["item_net"]).round(2)
    orders["adj_gross"] = (orders["o_gross"] - orders["item_gross"]).round(2)
    orders["sales_net"] = orders["o_net"] + orders["o_charge"]

    head = df.drop_duplicates("invoiceNumber").merge(
        orders[["invoiceNumber_", "adj_net", "adj_gross"]].rename(columns={"invoiceNumber_": "invoiceNumber"}),
        on="invoiceNumber", how="left")
    adj = head[(head["adj_net"].abs() > 0.005) | (head["adj_gross"].abs() > 0.005)].copy()
    adj["item"] = ADJ_ITEM
    adj["category"] = "Adjustments"
    adj["qty"] = 0.0
    adj["gross"] = adj["adj_gross"]
    adj["disc"] = 0.0
    adj["net"] = adj["adj_net"]
    adj["line_flag"] = 0
    items_all = pd.concat([df, adj], ignore_index=True)

    sales = orders.groupby(["day", "source"]).agg(
        orders=("invoiceNumber_", "count"), sales_gross=("o_gross", "sum"),
        sales_discount=("o_disc", "sum"), sales_net=("sales_net", "sum")).reset_index()
    prod = items_all.groupby(["day", "source"]).agg(
        product_gross=("gross", "sum"), product_discount=("disc", "sum"), product_net=("net", "sum")).reset_index()
    recon = sales.merge(prod, on=["day", "source"], how="outer").fillna(0)
    recon["gross_gap"] = (recon["product_gross"] - recon["sales_gross"]).round(2)
    recon["net_gap"] = (recon["product_net"] - recon["sales_net"]).round(2)
    for c in ["sales_gross", "sales_discount", "sales_net", "product_gross", "product_discount", "product_net"]:
        recon[c] = recon[c].round(2)
    recon["orders"] = recon["orders"].astype(int)
    recon = recon[RECON_COLUMNS]

    items_all["region"] = items_all["store"].map(master["region"]).fillna("Unmapped")
    items_all["ownership"] = items_all["store"].map(master["store_type"]).fillna("Unmapped")
    items_all["invoiceNumber"] = items_all["invoiceNumber"].astype(str)
    out = items_all[OUT_COLUMNS + ["online"]].copy()
    return out, recon


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


def read_saved(path):
    if not path or not os.path.exists(path) or os.path.getsize(path) == 0:
        return None
    saved = pd.read_csv(path, dtype=str, keep_default_na=False)
    return saved if "day" in saved.columns else None


def needed(saved, first_day, days, refresh_from):
    """Days from first_day that must be pulled: all of them without a saved file, else the missing and the refreshed ones."""
    if first_day is None:
        return []
    cand = [d for d in days if d >= first_day]
    if saved is None:
        return cand
    have = set(saved["day"])
    return [d for d in cand if d >= refresh_from or d.strftime("%Y-%m-%d") not in have]


def fetch_day_pages(branch, day):
    """Every sales/page record for one branch and day, following lastKey pages. Raises if a page cannot be read."""
    from sync_sales import RISTA, get_json
    rows, last_key = [], None
    while True:
        params = {"branch": branch, "day": day}
        if last_key:
            params["lastKey"] = last_key
        js = get_json(f"{RISTA}/sales/page", params=params)
        data = js.get("data", []) if isinstance(js, dict) else (js or [])
        rows.extend(data or [])
        last_key = js.get("lastKey") if isinstance(js, dict) else None
        if not data or not last_key:
            break
    return rows


def pull_day(codes, day, workers):
    """Every branch for one day. Returns (records, failures)."""
    records, failures = [], []

    def one(code):
        try:
            return code, fetch_day_pages(code, day), None
        except Exception as e:
            return code, [], str(e)

    with ThreadPoolExecutor(max_workers=max(1, workers)) as pool:
        for code, rows, err in pool.map(one, codes):
            if err:
                failures.append(f"{code} {day}: {err}")
            records.extend(rows)
    return records, failures


def load_master():
    """Store type, region and channel source, from the POS master (the same as the Sales sync)."""
    from sync_sales import load_maps
    store_type, region, source, _brand = load_maps()
    return {"store_type": store_type, "region": region, "source": source}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default="data/discount_rows.csv")
    ap.add_argument("--products-out", default="data/product_rows.csv", help="all channels; empty = do not write")
    ap.add_argument("--recon-out", default="data/product_recon.csv", help="reconciliation; empty = do not write")
    ap.add_argument("--product-days", type=int, default=PRODUCT_DAYS)
    ap.add_argument("--from", dest="start", default="")
    ap.add_argument("--to", dest="end", default="")
    ap.add_argument("--refresh-days", type=int, default=2, help="the last days are always pulled again")
    ap.add_argument("--workers", type=int, default=4)
    ap.add_argument("--live", action="store_true", help="pull today's business day only, to a separate file")
    args = ap.parse_args()
    from sync_sales import active_branches

    today = datetime.now(ZoneInfo("Asia/Kolkata")).date()
    first_day = datetime.strptime(FIRST_DAY, "%Y-%m-%d").date()
    start = parse_day(args.start, first_day)
    end = parse_day(args.end, today - timedelta(days=1))
    if args.live:
        start = end = (datetime.now(ZoneInfo("Asia/Kolkata")) - timedelta(hours=5)).date()
    if end < start:
        sys.exit("The end day is before the start day. Nothing to pull.")
    end_s = end.strftime("%Y-%m-%d")
    products = bool(args.products_out) and not args.live
    recon_on = bool(args.recon_out) and not args.live
    # Windows are measured from the END day, so a run that starts later keeps the saved history.
    saved_disc = read_saved(args.out) if not args.live else None
    saved_min = min(saved_disc["day"]) if saved_disc is not None and len(saved_disc) else None
    earliest = [first_day, start] + ([datetime.strptime(saved_min, "%Y-%m-%d").date()] if saved_min else [])
    disc_keep_from = min(earliest)     # never drops saved history, even when a run starts later
    product_start = end - timedelta(days=max(args.product_days, 1) - 1) if (products or recon_on) else None
    span_start = min(start, product_start) if product_start else start
    all_days = [span_start + timedelta(days=i) for i in range((end - span_start).days + 1)]
    refresh_from = end - timedelta(days=max(args.refresh_days, 1) - 1)

    codes = active_branches()
    if not codes:
        sys.exit("Rista returned no Active branches.")
    print(f"Active branches in Rista: {len(codes)}")
    master = load_master()

    if args.live:
        saved_disc = saved_prod = saved_recon = None
        pull_dates = all_days
    else:
        saved_disc = read_saved(args.out)
        saved_prod = read_saved(args.products_out) if products else None
        saved_recon = read_saved(args.recon_out) if recon_on else None
        wanted = set(needed(saved_disc, start, all_days, refresh_from))
        if products:
            wanted |= set(needed(saved_prod, product_start, all_days, refresh_from))
        if recon_on:
            wanted |= set(needed(saved_recon, product_start, all_days, refresh_from))
        pull_dates = sorted(wanted)
    print(f"Days to pull: {len(pull_dates)}")

    new_disc, new_prod, new_recon, failures = [], [], [], []
    pulled_disc, pulled_prod = set(), set()
    for d in pull_dates:
        ds = d.strftime("%Y-%m-%d")
        records, day_failures = pull_day(codes, ds, args.workers)
        failures += day_failures
        raw = pd.json_normalize(records) if records else pd.DataFrame()
        got, recon_day = transform(raw, ds, master)
        online_rows = got[got["online"]] if not got.empty else got
        print(f"{ds}: {len(records)} sales/page records, {len(online_rows)} Swiggy/Zomato item rows, "
              f"{len(got)} item rows in all channels")
        if d >= disc_keep_from:
            pulled_disc.add(ds)
            if not online_rows.empty:
                new_disc.append(online_rows[OUT_COLUMNS])
        if products and product_start and d >= product_start:
            pulled_prod.add(ds)
            if not got.empty:
                new_prod.append(got[OUT_COLUMNS])
            if not recon_day.empty:
                new_recon.append(recon_day)
    if failures:
        print("ERROR: these branch-days could not be read after retries. Nothing was saved, so the next run pulls them again:")
        for f in failures[:50]:
            print("  " + f)
        sys.exit(1)

    def combine(saved, frames, keep_from, pulled_set, columns, sort_by):
        """Saved rows inside the window that were not pulled again, plus the new rows."""
        keep = pd.DataFrame(columns=columns)
        if saved is not None:
            lo = keep_from.strftime("%Y-%m-%d")
            keep = saved[(~saved["day"].isin(pulled_set)) & (saved["day"] >= lo) & (saved["day"] <= end_s)]
        new = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame(columns=columns)
        parts = [p for p in [keep, new] if not p.empty]
        out = pd.concat(parts, ignore_index=True) if parts else pd.DataFrame(columns=columns)
        out = out.reindex(columns=columns)
        return out.sort_values(sort_by).reset_index(drop=True)

    disc_out = combine(saved_disc, new_disc, disc_keep_from, pulled_disc, OUT_COLUMNS, ["day", "store"])
    os.makedirs(os.path.dirname(args.out) or ".", exist_ok=True)
    disc_out.to_csv(args.out, index=False)
    print(f"wrote {args.out}: {len(disc_out)} rows, {disc_out['day'].nunique() if len(disc_out) else 0} days")

    if products:
        prod_out = combine(saved_prod, new_prod, product_start, pulled_prod, OUT_COLUMNS, ["day", "store"])
        os.makedirs(os.path.dirname(args.products_out) or ".", exist_ok=True)
        prod_out.to_csv(args.products_out, index=False)
        sources = sorted(prod_out["source"].unique()) if len(prod_out) else []
        print(f"wrote {args.products_out}: {len(prod_out)} item rows, "
              f"{prod_out['day'].nunique() if len(prod_out) else 0} days, sources {sources}")
    if recon_on:
        recon_out = combine(saved_recon, new_recon, product_start, pulled_prod, RECON_COLUMNS, ["day", "source"])
        os.makedirs(os.path.dirname(args.recon_out) or ".", exist_ok=True)
        recon_out.to_csv(args.recon_out, index=False)
        r = recon_out.copy()
        for c in ["sales_net", "product_net", "sales_gross", "product_gross", "sales_discount", "product_discount"]:
            r[c] = pd.to_numeric(r[c], errors="coerce").fillna(0)
        print(f"CROSS-CHECK, {product_start} to {end_s} (sales = Sales dashboard definitions):")
        print(f"  net:      sales {r['sales_net'].sum():,.2f} | product {r['product_net'].sum():,.2f} | "
              f"gap {r['product_net'].sum() - r['sales_net'].sum():,.2f}")
        print(f"  gross:    sales {r['sales_gross'].sum():,.2f} | product {r['product_gross'].sum():,.2f} | "
              f"gap {r['product_gross'].sum() - r['sales_gross'].sum():,.2f}")
        print(f"  discount: sales {r['sales_discount'].sum():,.2f} | product {r['product_discount'].sum():,.2f} "
              "(expected to differ: platform discounts sit in the order net, not in the order discount)")
        if abs(r["product_net"].sum() - r["sales_net"].sum()) > 1 or abs(r["product_gross"].sum() - r["sales_gross"].sum()) > 1:
            print("WARNING: net or gross does not agree with the order totals. See the Disc_Recon tab.")


if __name__ == "__main__":
    main()
