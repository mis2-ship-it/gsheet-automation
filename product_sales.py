#!/usr/bin/env python3
"""Product sales for the dashboard's Product Level tab: the closed item lines of each day, aggregated by store, brand,
region, store type, source, category and item. One file per day: <out>/<day>.csv.gz. Files older than --keep-days
(62 by default) are removed. A day with a failed branch-day is not written, so the last good file stays.

Only closed orders count (status Closed, as the discount job). Charges are not products, so they are not in these lines.

Usage: python product_sales.py --from 2026-11-24 --to 2026-11-25 --out data/product
"""
import argparse
import os
from datetime import datetime, timedelta

import pandas as pd

from cancel_sales import DEFAULT_ENDPOINT, OTHERS, business_day, days_between, pull
from sync_sales import active_branches, load_maps

PRODUCT_COLS = ["Date", "Branch", "Brand", "Region", "Store Type", "Source", "Category", "Item",
                "Orders", "Qty", "Gross", "Discount", "Net"]


def product_lines(rec):
    """(item, category, qty, gross, net, discount) for each item line of an order. The discount is the item's
    base net discount, as a positive amount. An order with no items gives one '(no item)' line."""
    items = None
    for v in rec.values():
        if isinstance(v, list) and v and isinstance(v[0], dict) and ("shortName" in v[0] or "quantity" in v[0]):
            items = v
            break
    if not items:
        return [("(no item)", "", 0.0, 0.0, 0.0, 0.0)]
    return [(str(it.get("shortName") or "(no item)").strip(), str(it.get("categoryName") or "").strip(),
             float(it.get("quantity") or 0), float(it.get("baseGrossAmount") or 0), float(it.get("baseNetAmount") or 0),
             abs(float(it.get("baseNetDiscountAmount") or 0))) for it in items]


def is_closed(rec):
    return str(rec.get("status") or "").strip().lower() == "closed"


def aggregate(records, maps):
    """The closed item lines of the records, aggregated. Orders is the number of distinct orders that have the item in
    that group, so an order with two lines of one item counts once."""
    store_type, region, source, brand = maps
    acc = {}
    for i, rec in enumerate(records):
        if not is_closed(rec):
            continue
        biz, _ = business_day(rec.get("invoiceDate"))
        if not biz:
            continue
        branch = str(rec.get("branchName") or rec.get("_branch_pull") or "").strip()
        channel = str(rec.get("channel") or "").strip()
        b = str(rec.get("brandName") or brand.get(channel) or "Unmapped").strip()
        reg, typ = region.get(branch, "Unmapped"), store_type.get(branch, "Unmapped")
        src = source.get(channel, OTHERS)
        order = str(rec.get("invoiceNumber") or rec.get("invoiceNo") or f"{branch}-{biz}-{i}")
        for name, cat, qty, gross, net, disc in product_lines(rec):
            key = (biz, branch, b, reg, typ, src, cat, name)
            g = acc.setdefault(key, {"orders": set(), "Qty": 0.0, "Gross": 0.0, "Discount": 0.0, "Net": 0.0})
            g["orders"].add(order)
            g["Qty"] += qty
            g["Gross"] += gross
            g["Discount"] += disc
            g["Net"] += net
    rows = [{"Date": k[0], "Branch": k[1], "Brand": k[2], "Region": k[3], "Store Type": k[4], "Source": k[5],
             "Category": k[6], "Item": k[7], "Orders": len(g["orders"]), "Qty": round(g["Qty"], 2),
             "Gross": round(g["Gross"], 2), "Discount": round(g["Discount"], 2), "Net": round(g["Net"], 2)}
            for k, g in acc.items()]
    if not rows:
        return pd.DataFrame(columns=PRODUCT_COLS)
    return pd.DataFrame(rows, columns=PRODUCT_COLS).sort_values(["Date", "Branch", "Category", "Item"]).reset_index(drop=True)


def _is_day(name):
    try:
        datetime.strptime(name, "%Y-%m-%d")
        return True
    except ValueError:
        return False


def write_days(df, out, keep_days=62, skip_days=()):
    """One file per day, from the aggregated rows. Days in skip_days are not written. Then files older than keep_days
    (counted back from the newest day in the folder) are removed. Returns [(day, rows, bytes)] for the days written."""
    os.makedirs(out, exist_ok=True)
    report = []
    if len(df):
        for day, part in df.groupby("Date"):
            if day in skip_days:
                continue
            path = os.path.join(out, f"{day}.csv.gz")
            part.to_csv(path, index=False, compression="gzip")
            report.append((day, len(part), os.path.getsize(path)))
    days = [f[:-len(".csv.gz")] for f in os.listdir(out) if f.endswith(".csv.gz") and _is_day(f[:-len(".csv.gz")])]
    if days:
        newest = datetime.strptime(max(days), "%Y-%m-%d").date()
        cutoff = (newest - timedelta(days=keep_days - 1)).strftime("%Y-%m-%d")
        for d in days:
            if d < cutoff:
                os.remove(os.path.join(out, f"{d}.csv.gz"))
    return report


def main(argv=None):
    ap = argparse.ArgumentParser(description="Closed item lines per day, for the Product Level tab.")
    ap.add_argument("--from", dest="start", required=True, help="first business day, YYYY-MM-DD")
    ap.add_argument("--to", dest="end", default="", help="last business day (default: the same as --from)")
    ap.add_argument("--out", default="data/product", help="folder of the per-day files")
    ap.add_argument("--pull-dir", default="data/product_pull", help="the saved branch-days (for a run that stops part way)")
    ap.add_argument("--keep-days", type=int, default=62, help="days kept in the folder")
    ap.add_argument("--branches", default="", help="optional comma list of branch codes (default: all active)")
    ap.add_argument("--endpoint", default=DEFAULT_ENDPOINT)
    ap.add_argument("--workers", type=int, default=12)
    ap.add_argument("--fresh", action="store_true", help="ignore saved branch-days and pull again")
    ap.add_argument("--progress-every", type=int, default=25)
    a = ap.parse_args(argv)
    a.end = a.end or a.start
    branches = [b.strip() for b in a.branches.split(",") if b.strip()] or active_branches()
    days = days_between(a.start, a.end)
    maps = load_maps()
    raw_dir = os.path.join(a.pull_dir, "raw", a.endpoint.strip("/").replace("/", "_") or "root")
    raw, errors, stats = pull(days, branches, a.endpoint, a.workers, raw_dir, a.fresh, a.progress_every)
    df = aggregate(raw, maps)
    failed = sorted({e.get("day") for e in errors if isinstance(e, dict) and e.get("day")})
    report = write_days(df, a.out, a.keep_days, skip_days=failed)
    total = 0
    for day, rows, size in report:
        total += size
        print(f"{day}: {rows} rows, {size / 1024:.0f} KB")
    kept = [f for f in os.listdir(a.out) if f.endswith(".csv.gz")]
    folder = sum(os.path.getsize(os.path.join(a.out, f)) for f in kept)
    print(f"written {len(report)} day file(s) this run; the folder holds {len(kept)} day file(s), {folder / 1024 / 1024:.2f} MB")
    if failed:
        print(f"days NOT written because some branch-days failed: {failed}")
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
