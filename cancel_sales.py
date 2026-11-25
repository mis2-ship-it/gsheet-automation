#!/usr/bin/env python3
"""Pulls cancelled orders (Voided or Cancelled) from Rista, with the cancel reason.

Uses the same endpoint as the discount sync: /sales/page with branch, day and lastKey. That endpoint returns every
order, including Voided and Cancelled ones (the discount sync leaves them out). Kept apart from the Closed history.
Uses the same login, branch list, store maps and business-day cut-off as sync_sales.py.

  python cancel_sales.py --probe --from 2025-04-01                   # first check: which request works, and what it returns
  python cancel_sales.py --from 2025-04-01 --to 2025-04-30 --out data/cancel

Reason matching: a reason counts for a bucket when the keyword in data/cancel_reasons.csv is contained in the reason
text (case-insensitive; the first match in file order wins). Any other reason, or a blank one, is "Others".

Output in --out:
  probe_report.json                     (--probe) each candidate request with its status, fields and statuses
  cancel_invoices_<from>_<to>.csv       one row per cancelled order (Date = business day, Orders = 1)
  cancel_summary_<from>_<to>.csv        orders by date, branch, brand, source, session, store type, region, bucket, status
  cancel_reasons_seen_<from>_<to>.csv   every reason text with its count and bucket
  status_seen_<from>_<to>.csv           every status value with its count, Closed included
  cancel_raw_sample.json                the first raw orders, with their field names
  cancel_errors.csv                     branch-days that failed, with the API's error text
  raw/<endpoint>/<day>/<branch>.json.gz each branch-day as it was pulled (a run that stops carries on from these)
"""
import argparse
import csv
import gzip
import json
import os
import sys
import threading
import time
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta

import pandas as pd
import requests

from sync_sales import RISTA, active_branches, headers, load_maps, session_for_hour

DEFAULT_ENDPOINT = "/sales/page"
CANCEL_STATUSES = ("voided", "cancelled")
OTHERS = "Others"
ABORT_AFTER = 20   # stop when this many branch-days have failed and none has worked yet


def load_reasons(path):
    """The reason list: keyword (contains, any case), bucket, type and notes, in file order."""
    with open(path, newline="", encoding="utf-8-sig") as fh:
        rows = list(csv.DictReader(fh))
    reasons = []
    for r in rows:
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
    """The business day of an order: before 05:00 it belongs to the day before (as in sync_sales.py)."""
    dt = pd.to_datetime(invoice_date, errors="coerce")
    if pd.isna(dt):
        return None, None
    dt = dt.tz_localize(None) if getattr(dt, "tzinfo", None) else dt
    biz = (dt - pd.Timedelta(days=1)).date() if dt.hour < 5 else dt.date()
    return biz.strftime("%Y-%m-%d"), dt.hour


def days_between(start, end):
    d0, d1 = datetime.strptime(start, "%Y-%m-%d").date(), datetime.strptime(end, "%Y-%m-%d").date()
    return [(d0 + timedelta(days=i)).strftime("%Y-%m-%d") for i in range((d1 - d0).days + 1)]


def get_json(url, params=None, retries=3):
    """One request. Retries only server errors (429 and 5xx), with a growing pause.
    404 and 204 mean no orders for that branch and day: an empty day, with no retry.
    Other client errors (400, 401, 403) fail at once, with the API's own message in the error."""
    last = None
    for attempt in range(retries):
        try:
            r = requests.get(url, headers=headers(), params=params, timeout=30)
        except requests.RequestException as e:
            last = str(e)
        else:
            if r.status_code == 200:
                return r.json()
            if r.status_code in (404, 204):
                return {"data": [], "lastKey": None}
            body = (r.text or "")[:300]
            if r.status_code == 429 or r.status_code >= 500:
                last = f"HTTP {r.status_code} {body}"
            else:
                raise RuntimeError(f"{url} {params}: HTTP {r.status_code} {body}")
        time.sleep(2 * (attempt + 1))
    raise RuntimeError(f"{url} {params}: {last}")


def fetch_branch_day(branch, day, endpoint):
    """Every order for one branch and one day, following lastKey pages. Returns (orders, requests made)."""
    rows, last_key, pages = [], None, 0
    while True:
        params = {"branch": branch, "day": day}
        if last_key:
            params["lastKey"] = last_key
        js = get_json(f"{RISTA}{endpoint}", params=params)
        pages += 1
        data = js.get("data", []) if isinstance(js, dict) else (js or [])
        if not data:
            break
        rows.extend(data)
        last_key = js.get("lastKey") if isinstance(js, dict) else None
        if not last_key:
            break
    return rows, pages


def checkpoint_path(raw_dir, branch, day):
    return os.path.join(raw_dir, day, f"{branch}.json.gz")


class Skipped(Exception):
    """A branch-day that was not started, because the run had already been stopped."""


def pull(days, branches, endpoint, workers, raw_dir, fresh=False, progress_every=25):
    """All orders for the days and branches, as raw records. Each branch-day is saved as it finishes, so a
    stopped run carries on from the saved ones (unless fresh=True). Stops early when nothing is working:
    the check is made inside each worker, before it sends a request.
    Returns (orders, errors, stats)."""
    raw, errors = [], []
    stats = {"branch_days": 0, "from_saved": 0, "requests": 0, "empty": 0, "aborted": False}
    total = len(days) * len(branches)
    started = time.time()
    jobs = {}
    lock = threading.Lock()
    state = {"ok": 0, "fail": 0, "stop": False}

    def job(branch, day):
        with lock:
            if state["stop"]:
                raise Skipped()
        path = checkpoint_path(raw_dir, branch, day)
        if not fresh and os.path.exists(path):
            with gzip.open(path, "rt", encoding="utf-8") as fh:
                saved = json.load(fh)
            with lock:
                state["ok"] += 1
            return saved["rows"], saved["pages"], True
        try:
            rows, pages = fetch_branch_day(branch, day, endpoint)
        except Exception:
            with lock:
                state["fail"] += 1
                if state["ok"] == 0 and state["fail"] >= ABORT_AFTER:
                    state["stop"] = True
            raise
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with gzip.open(path, "wt", encoding="utf-8") as fh:
            json.dump({"rows": rows, "pages": pages}, fh, default=str)
        with lock:
            state["ok"] += 1
        return rows, pages, False

    with ThreadPoolExecutor(max_workers=workers) as ex:
        for day in days:
            for b in branches:
                jobs[ex.submit(job, b, day)] = (b, day)
        for fut in as_completed(jobs):
            b, day = jobs[fut]
            try:
                rows, pages, from_saved = fut.result()
                stats["branch_days"] += 1
                for r in rows:
                    r["_branch_pull"] = b
                    r["_day_pull"] = day
                    raw.append(r)
                stats["requests"] += 0 if from_saved else pages
                stats["from_saved"] += 1 if from_saved else 0
                stats["empty"] += 1 if not rows else 0
            except Skipped:
                pass
            except Exception as e:
                stats["branch_days"] += 1
                errors.append({"branch": b, "day": day, "error": str(e)[:400]})
            if state["stop"] and not stats["aborted"]:
                stats["aborted"] = True
            done = stats["branch_days"]
            if done and (done % progress_every == 0 or done == total):
                elapsed = time.time() - started
                left = (total - done) * (elapsed / done)
                print(f"{done}/{total} branch-days | {elapsed / 60:.1f} min | requests {stats['requests']} | "
                      f"saved {stats['from_saved']} | empty {stats['empty']} | errors {len(errors)} | "
                      f"about {left / 60:.0f} min left", flush=True)
    return raw, errors, stats


PROBE_CANDIDATES = [
    ("/sales/page", lambda b, d: {"branch": b, "day": d}),
    ("/sale", lambda b, d: {"branch": b, "day": d}),
    ("/sale", lambda b, d: {"branchCode": b, "date": d}),
    ("/sale", lambda b, d: {"branch": b, "fromDate": d, "toDate": d}),
    ("/sale", lambda b, d: {"branchCode": b, "fromDate": d, "toDate": d}),
    ("/sales/summary", lambda b, d: {"branch": b, "day": d}),
]


def probe_candidates(branch, day):
    """Tries each candidate request once, for one branch and day. Reports the status, the fields of the
    first order, any reason or cancel fields, the statuses seen, and the API's message when it fails."""
    report = []
    for endpoint, make in PROBE_CANDIDATES:
        params = make(branch, day)
        row = {"endpoint": endpoint, "params": ",".join(params.keys()), "http": None, "orders": 0,
               "reason_fields": "", "statuses": "", "fields": "", "message": ""}
        try:
            r = requests.get(f"{RISTA}{endpoint}", headers=headers(), params=params, timeout=30)
            row["http"] = r.status_code
            if r.status_code == 200:
                js = r.json()
                data = (js.get("data", []) if isinstance(js, dict) else (js or [])) or []
                row["orders"] = len(data)
                if data:
                    keys = sorted(data[0].keys())
                    row["fields"] = ",".join(keys)
                    row["reason_fields"] = ",".join(k for k in keys if "reason" in k.lower() or "cancel" in k.lower())
                    row["statuses"] = ";".join(f"{s}={n}" for s, n in Counter(
                        str(d.get("status", "")) for d in data).most_common())
                row["message"] = "" if data else "200 with no orders for this branch and day"
            else:
                row["message"] = (r.text or "")[:300]
        except Exception as e:
            row["message"] = str(e)[:300]
        report.append(row)
    return report


def detect_reason_key(raw, reason_field):
    """The field that holds the cancel reason: the one given, or 'auto' finds it by name."""
    if reason_field != "auto":
        return reason_field
    keys = sorted({k for r in raw[:500] for k in r if not k.startswith("_")})
    if "reason" in keys:
        return "reason"
    for k in keys:
        if "reason" in k.lower():
            return k
    return None


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


def write_errors(out, errors):
    pd.DataFrame(errors).to_csv(os.path.join(out, "cancel_errors.csv"), index=False)


def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("--from", dest="start", required=True, help="first business day, YYYY-MM-DD")
    ap.add_argument("--to", dest="end", default="", help="last business day, YYYY-MM-DD (default: the same as --from)")
    ap.add_argument("--out", default="data/cancel")
    ap.add_argument("--probe", action="store_true", help="try each candidate request once and report what works")
    ap.add_argument("--endpoint", default=DEFAULT_ENDPOINT, help="Rista endpoint for orders")
    ap.add_argument("--status-field", default="status")
    ap.add_argument("--reason-field", default="auto", help="the reason field name, or auto to find it")
    ap.add_argument("--cancel-statuses", default="Voided,Cancelled")
    ap.add_argument("--reasons", default="data/cancel_reasons.csv")
    ap.add_argument("--branches", default="", help="comma list of branch codes (for a test run)")
    ap.add_argument("--workers", type=int, default=8, help="parallel requests (raise only if the API allows it)")
    ap.add_argument("--fresh", action="store_true", help="ignore saved branch-days and pull again")
    ap.add_argument("--progress-every", type=int, default=25, help="print progress every N branch-days")
    a = ap.parse_args(argv)
    a.end = a.end or a.start

    branches = [b.strip() for b in a.branches.split(",") if b.strip()] or active_branches()
    os.makedirs(a.out, exist_ok=True)

    if a.probe:
        report = probe_candidates(branches[0], a.start)
        with open(os.path.join(a.out, "probe_report.json"), "w", encoding="utf-8") as fh:
            json.dump(report, fh, indent=2)
        for row in report:
            print(f"{row['endpoint']:15s} {row['params']:20s} HTTP {row['http']}  orders {row['orders']:4d}  "
                  f"reason fields: {row['reason_fields'] or '-'}  statuses: {row['statuses'] or '-'}")
            if row["message"]:
                print(f"    message: {row['message']}")
        print(f"written to {os.path.join(a.out, 'probe_report.json')}")
        return 0

    reasons = load_reasons(a.reasons)
    cancel_set = tuple(s.strip().lower() for s in a.cancel_statuses.split(",") if s.strip())
    days = days_between(a.start, a.end)
    maps = load_maps()
    tag = f"{a.start}_{a.end}"
    raw_dir = os.path.join(a.out, "raw", a.endpoint.strip("/").replace("/", "_") or "root")

    raw, errors, stats = pull(days, branches, a.endpoint, a.workers, raw_dir, a.fresh, a.progress_every)
    print(f"pulled {len(raw)} orders for {len(days)} day(s) and {len(branches)} branch(es); {stats['requests']} "
          f"requests, {stats['from_saved']} branch-days from saved files, {stats['empty']} empty, {len(errors)} failed")
    if errors:
        write_errors(a.out, errors)
    if stats["aborted"]:
        print(f"STOPPED: the first {ABORT_AFTER} branch-days all failed. The API said: {errors[0]['error']}")
        print("Run again with --probe to see which request the API accepts.")
        return 2

    keys = sorted({k for r in raw[:200] for k in r.keys() if not k.startswith("_")})
    with open(os.path.join(a.out, "cancel_raw_sample.json"), "w", encoding="utf-8") as fh:
        json.dump({"keys": keys, "sample": [{k: v for k, v in r.items() if not k.startswith("_")} for r in raw[:3]]},
                  fh, indent=2, default=str)
    if raw and a.status_field not in keys:
        print(f"status field '{a.status_field}' not in the response. Keys seen: {keys}")
        return 2
    reason_key = detect_reason_key(raw, a.reason_field) if raw else (a.reason_field if a.reason_field != "auto" else "reason")
    if raw and reason_key not in keys:
        print(f"reason field '{reason_key}' not in the response. Keys seen: {keys}")
        return 2

    cancelled, status_count, reason_count = build_rows(raw, reasons, maps, a.status_field, reason_key, cancel_set)
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

    listed = int((inv["Listed"] == "Yes").sum()) if len(inv) else 0
    others = int((inv["Bucket"] == OTHERS).sum()) if len(inv) else 0
    print(f"cancelled orders: {len(inv)} (listed reasons {listed}, Others {others})")
    print(f"status values seen: {len(status_count)} (see status_seen_{tag}.csv); reason field used: {reason_key}")
    return 1 if errors else 0


if __name__ == "__main__":
    sys.exit(main())
