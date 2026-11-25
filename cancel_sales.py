#!/usr/bin/env python3
"""Pulls cancelled orders (Voided or Cancelled) from Rista's /sale endpoint, with the cancel reason.

Kept apart from the Closed history. Uses the same login, branch list, store maps and business-day cut-off
as sync_sales.py (a sale before 05:00 belongs to the previous business day).

  python cancel_sales.py --from 2025-04-01 --to 2025-04-30 --out data/cancel

Reason matching: a reason counts for a bucket when the keyword in data/cancel_reasons.csv is contained in the
reason text (case-insensitive; the first match in file order wins). Any other reason, or a blank one, is "Others".
Others is kept as its own column; it is not counted in the listed buckets.

Output in --out:
  cancel_invoices_<from>_<to>.csv       one row per cancelled order (Date = business day, Orders = 1)
  cancel_summary_<from>_<to>.csv        orders by date, branch, brand, source, session, store type, region, bucket, status
  cancel_reasons_seen_<from>_<to>.csv   every reason text with its count and bucket (to extend the reason list)
  status_seen_<from>_<to>.csv           every status value with its count, Closed included (to check against the Closed data)
  cancel_raw_sample.json                the first raw orders, to check the field names
  cancel_errors.csv                     branch-day pulls that failed (the job then exits with 1)
"""
import argparse
import csv
import json
import os
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, timedelta

import pandas as pd

from sync_sales import RISTA, active_branches, get_json, load_maps, session_for_hour

CANCEL_STATUSES = ("voided", "cancelled")
CLOSED = "closed"
OTHERS = "Others"


def load_reasons(path):
    """The reason list: keyword (contains, any case), bucket, type and notes, in file order."""
    with open(path, newline="", encoding="utf-8-sig") as fh:
        rows = list(csv.DictReader(fh))
    reasons = []
    for r in rows:
        # the reason column is found by its name (its header contains a comma, so it must be quoted in the file)
        col = next((k for k in r if k and k.strip().lower().startswith("reason")), None)
        kw = ((r.get(col) if col else "") or "").strip()
        if kw:
            reasons.append({"keyword": kw, "type": (r.get("Type") or "").strip(),
                            "bucket": (r.get("Bucket") or "").strip(), "notes": (r.get("Notes") or "").strip()})
    if not reasons:
        raise SystemExit(f"no reasons in {path}")
    return reasons


def classify(reason_text, reasons):
    """(bucket, listed). A reason that contains a listed keyword is that bucket; anything else is Others."""
    text = (reason_text or "").strip()
    low = text.lower()
    if not low:
        return OTHERS, False
    for r in reasons:
        if r["keyword"].lower() in low:
            return r["bucket"], True
    return OTHERS, False


def business_day(invoice_date):
    """The business day of an invoice: before 05:00 it belongs to the day before (as in sync_sales.py)."""
    dt = pd.to_datetime(invoice_date, errors="coerce")
    if pd.isna(dt):
        return None, None
    dt = dt.tz_localize(None) if getattr(dt, "tzinfo", None) else dt
    biz = (dt - pd.Timedelta(days=1)).date() if dt.hour < 5 else dt.date()
    return biz.strftime("%Y-%m-%d"), dt.hour


def days_between(start, end):
    d0, d1 = datetime.strptime(start, "%Y-%m-%d").date(), datetime.strptime(end, "%Y-%m-%d").date()
    return [(d0 + timedelta(days=i)).strftime("%Y-%m-%d") for i in range((d1 - d0).days + 1)]


def fetch_branch_day(branch, day, endpoint):
    """Every order for one branch and one day, following lastKey pages (the same paging as the sales sync)."""
    rows, last_key = [], None
    while True:
        params = {"branch": branch, "day": day}
        if last_key:
            params["lastKey"] = last_key
        js = get_json(f"{RISTA}{endpoint}", params=params)
        data = js.get("data", []) if isinstance(js, dict) else (js or [])
        if not data:
            break
        rows.extend(data)
        last_key = js.get("lastKey") if isinstance(js, dict) else None
        if not last_key:
            break
    return rows


def pull(days, branches, endpoint, workers):
    """All orders for the days and branches, as raw records. Failed branch-days are returned as errors."""
    raw, errors = [], []
    jobs = {}
    with ThreadPoolExecutor(max_workers=workers) as ex:
        for day in days:
            for b in branches:
                jobs[ex.submit(fetch_branch_day, b, day, endpoint)] = (b, day)
        for fut in as_completed(jobs):
            b, day = jobs[fut]
            try:
                for r in fut.result():
                    r["_branch_pull"] = b
                    r["_day_pull"] = day
                    raw.append(r)
            except Exception as e:
                errors.append({"branch": b, "day": day, "error": str(e)[:200]})
    return raw, errors


def build_rows(raw, reasons, maps, status_field, reason_field, cancel_statuses):
    """One record per cancelled order, plus the status and reason counts over every order."""
    store_type, region, source, brand = maps
    status_count, reason_count, cancelled = {}, {}, []
    for i, r in enumerate(raw):
        status = str(r.get(status_field, "") or "").strip()
        status_count[status] = status_count.get(status, 0) + 1
        if status.lower() not in cancel_statuses:
            continue
        reason = str(r.get(reason_field, "") or "").strip()
        bucket, listed = classify(reason, reasons)
        biz, hour = business_day(r.get("invoiceDate"))
        branch = str(r.get("branchName") or r.get("_branch_pull") or "").strip()
        channel = str(r.get("channel") or "").strip()
        reason_count[reason or "(blank)"] = reason_count.get(reason or "(blank)", 0) + 1
        cancelled.append({
            "Invoice No": str(r.get("invoiceNumber") or r.get("invoiceNo") or f"{branch}-{r.get('_day_pull')}-{i}"),
            "Date": biz or r.get("_day_pull"),
            "Branch": branch,
            "Brand Name": brand.get(channel),
            "Source": source.get(channel, OTHERS),
            "Session": session_for_hour(hour) if hour is not None else "Unmapped",
            "Store Type": store_type.get(branch, "Unmapped"),
            "Region": region.get(branch, "Unmapped"),
            "Channel": channel,
            "Status": status,
            "Reason": reason or "(blank)",
            "Bucket": bucket,
            "Listed": "Yes" if listed else "No",
            "Orders": 1,
            "Net Sales": float(r.get("netAmount") or 0) + float(r.get("chargeAmount") or 0),
            "Gross Sales": float(r.get("grossAmount") or 0),
            "Discount": float(r.get("discountAmount") or 0),
        })
    return cancelled, status_count, reason_count


def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("--from", dest="start", required=True, help="first business day, YYYY-MM-DD")
    ap.add_argument("--to", dest="end", required=True, help="last business day, YYYY-MM-DD")
    ap.add_argument("--out", default="data/cancel")
    ap.add_argument("--endpoint", default="/sale", help="Rista endpoint for orders (to confirm from the first sample)")
    ap.add_argument("--status-field", default="status")
    ap.add_argument("--reason-field", default="reason")
    ap.add_argument("--cancel-statuses", default="Voided,Cancelled")
    ap.add_argument("--reasons", default="data/cancel_reasons.csv")
    ap.add_argument("--branches", default="", help="comma list of branch codes, to test on a few branches")
    ap.add_argument("--workers", type=int, default=8)
    a = ap.parse_args(argv)

    reasons = load_reasons(a.reasons)
    cancel_set = tuple(s.strip().lower() for s in a.cancel_statuses.split(",") if s.strip())
    days = days_between(a.start, a.end)
    branches = [b.strip() for b in a.branches.split(",") if b.strip()] or active_branches()
    maps = load_maps()
    os.makedirs(a.out, exist_ok=True)
    tag = f"{a.start}_{a.end}"

    raw, errors = pull(days, branches, a.endpoint, a.workers)
    print(f"pulled {len(raw)} orders for {len(days)} day(s) and {len(branches)} branch(es)")
    if errors:
        pd.DataFrame(errors).to_csv(os.path.join(a.out, "cancel_errors.csv"), index=False)
        print(f"{len(errors)} branch-day pulls failed, see cancel_errors.csv")

    keys = sorted({k for r in raw[:200] for k in r.keys() if not k.startswith("_")})
    with open(os.path.join(a.out, "cancel_raw_sample.json"), "w", encoding="utf-8") as fh:
        json.dump({"keys": keys, "sample": [{k: v for k, v in r.items() if not k.startswith("_")} for r in raw[:3]]},
                  fh, indent=2, default=str)
    if raw and a.status_field not in keys:
        print(f"status field '{a.status_field}' not in the /sale response. Keys seen: {keys}")
        return 2
    if raw and a.reason_field not in keys:
        print(f"reason field '{a.reason_field}' not in the /sale response. Keys seen: {keys}")
        return 2

    cancelled, status_count, reason_count = build_rows(raw, reasons, maps, a.status_field, a.reason_field, cancel_set)
    pd.DataFrame(sorted(status_count.items(), key=lambda x: -x[1]), columns=["Status", "Orders"]) \
        .to_csv(os.path.join(a.out, f"status_seen_{tag}.csv"), index=False)
    inv = pd.DataFrame(cancelled)
    if inv.empty:
        inv = pd.DataFrame(columns=["Invoice No", "Date", "Branch", "Brand Name", "Source", "Session", "Store Type",
                                    "Region", "Channel", "Status", "Reason", "Bucket", "Listed", "Orders",
                                    "Net Sales", "Gross Sales", "Discount"])
    inv.to_csv(os.path.join(a.out, f"cancel_invoices_{tag}.csv"), index=False)
    if not inv.empty:
        summary = (inv.groupby(["Date", "Branch", "Brand Name", "Source", "Session", "Store Type", "Region",
                                "Bucket", "Status"], as_index=False)
                      .agg(Orders=("Orders", "sum"), **{"Net Sales": ("Net Sales", "sum"),
                                                        "Gross Sales": ("Gross Sales", "sum"),
                                                        "Discount": ("Discount", "sum")}))
        summary.to_csv(os.path.join(a.out, f"cancel_summary_{tag}.csv"), index=False)
    reasons_seen = pd.DataFrame(sorted(reason_count.items(), key=lambda x: -x[1]), columns=["Reason", "Orders"])
    reasons_seen["Bucket"] = reasons_seen["Reason"].map(lambda t: classify(t if t != "(blank)" else "", reasons)[0])
    reasons_seen.to_csv(os.path.join(a.out, f"cancel_reasons_seen_{tag}.csv"), index=False)

    print(f"cancelled orders: {len(inv)} (listed reasons {int((inv['Listed'] == 'Yes').sum()) if len(inv) else 0}, "
          f"Others {int((inv['Bucket'] == OTHERS).sum()) if len(inv) else 0})")
    print(f"statuses seen: {status_count}")
    return 1 if errors else 0


if __name__ == "__main__":
    sys.exit(main())
