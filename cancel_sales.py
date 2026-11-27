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
  cancel_items_<from>_<to>.csv          one row per item line of each cancelled order (Item Name, Category, Qty, Item Net)
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
                    flat = [flatten(d) for d in data]
                    keys = sorted({k for f in flat for k in f})           # every field seen in the orders
                    row["fields"] = ",".join(keys)
                    row["reason_fields"] = ",".join(k for k in keys if any(w in k.lower() for w in
                                                    ("reason", "remark", "status", "cancel")))
                    row["statuses"] = ";".join(f"{s}={n}" for s, n in Counter(
                        status_label(f) for f in flat).most_common())
                    cancelled_here = [f for f in flat if is_cancelled(f, "auto", ("cancel", "void"))]
                    if cancelled_here:
                        # the fields of one cancelled order, so the cancel reason can be seen
                        row["cancelled_sample"] = {k: v for k, v in cancelled_here[0].items()
                                                   if any(w in k.lower() for w in
                                                          ("status", "reason", "remark", "cancel", "invoice"))}
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


def flatten(rec, prefix=""):
    """The record with nested fields as dotted names: {"statusInfo": {"reason": x}} -> {"statusInfo.reason": x}."""
    out = {}
    for k, v in rec.items():
        key = f"{prefix}{k}"
        if isinstance(v, dict):
            out.update(flatten(v, key + "."))
        else:
            out[key] = v
    return out


# The reason and its notes. The Sales Page report shows them as statusInfo.reason, statusInfo.sourceReason and
# statusInfo.remarks; a flat record has them as reason, sourceReason and remarks.
REASON_KEYS = ("statusInfo.reason", "reason")
SOURCE_REASON_KEYS = ("statusInfo.sourceReason", "sourceReason")
REMARK_KEYS = ("statusInfo.remarks", "remarks")


def status_fields(flat, status_field="auto"):
    """The fields that hold an order status: any field with 'status' in its name (status, statusInfo.status,
    fulfillmentStatus). With a name given, only that field."""
    if status_field != "auto":
        return [status_field] if status_field in flat else []
    # the reason, its source reason and the remarks are text, not the status, even though their names contain "status"
    return [k for k in flat if "status" in k.lower() and not any(w in k.lower() for w in ("reason", "remark"))]


def is_cancelled(flat, status_field, cancel_words):
    """Cancelled when any status field contains one of the words (cancel, void), in any case."""
    return any(w in str(flat[k] or "").lower() for k in status_fields(flat, status_field) for w in cancel_words)


def status_label(flat, status_field="auto"):
    """The status fields and their values, for example 'status=Closed | statusInfo.status=Cancelled'."""
    parts = [f"{k}={flat[k]}" for k in status_fields(flat, status_field)]
    return " | ".join(parts) or "(no status field)"


def first_value(flat, keys):
    for k in keys:
        v = flat.get(k)
        if v not in (None, ""):
            return str(v).strip()
    return ""


def reason_parts(flat, reason_field="auto"):
    """(reason, source reason, remarks). A reason field given by name is tried first. With auto, a field with
    'reason' in its name (for example cancelReason) is used when statusInfo.reason is not there."""
    keys = (reason_field,) + REASON_KEYS if reason_field != "auto" else REASON_KEYS
    reason = first_value(flat, keys)
    if not reason and reason_field == "auto":
        for k, v in flat.items():
            if "reason" in k.lower() and "source" not in k.lower() and v not in (None, ""):
                reason = str(v).strip()
                break
    return reason, first_value(flat, SOURCE_REASON_KEYS), first_value(flat, REMARK_KEYS)


def build_rows(raw, reasons, maps, status_field, reason_field, cancel_statuses):
    """One record per cancelled order, plus the status and reason counts over every order.
    An order is cancelled when a status field says cancel or void. Its reason text is the reason, source reason and
    remarks together, so a reason such as 'as the ordered item(s) are now out of stock' is found."""
    store_type, region, source, brand = maps
    status_count, reason_count, cancelled = {}, {}, []
    for i, rec in enumerate(raw):
        r = flatten(rec)
        label = status_label(r, status_field)
        status_count[label] = status_count.get(label, 0) + 1
        if not is_cancelled(r, status_field, cancel_statuses):
            continue
        reason, source_reason, remarks = reason_parts(r, reason_field)
        text = " | ".join(p for p in (reason, source_reason, remarks) if p)
        bucket, listed = classify(text, reasons)
        biz, hour = business_day(r.get("invoiceDate"))
        branch = str(r.get("branchName") or r.get("_branch_pull") or "").strip()
        channel = str(r.get("channel") or "").strip()
        reason_count[text or "(blank)"] = reason_count.get(text or "(blank)", 0) + 1
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
            "Status": label,
            "Reason": reason or "(blank)",
            "Source Reason": source_reason,
            "Remarks": remarks,
            "Bucket": bucket,
            "Listed": "Yes" if listed else "No",
            "Orders": 1,
            "Net Sales": float(r.get("netAmount") or 0) + float(r.get("chargeAmount") or 0),
            "Gross Sales": float(r.get("grossAmount") or 0),
            "Discount": float(r.get("discountAmount") or 0),
        })
    return cancelled, status_count, reason_count


# The item lines of an order: sales/page sends the items as a list of dicts (shortName, categoryName, quantity,
# baseGrossAmount, baseNetAmount), the same fields the discount job reads.
ITEM_NAME, ITEM_CATEGORY = "shortName", "categoryName"


def item_lines(rec):
    """The items of one order, as (name, category, qty, gross, net) tuples. An order with no items gives one line
    named '(no item)', so the order is still counted."""
    items = None
    for v in rec.values():
        if isinstance(v, list) and v and isinstance(v[0], dict) and (ITEM_NAME in v[0] or "quantity" in v[0]):
            items = v
            break
    if not items:
        return [("(no item)", "", 0, 0.0, 0.0)]
    out = []
    for it in items:
        out.append((str(it.get(ITEM_NAME) or "(no item)").strip(), str(it.get(ITEM_CATEGORY) or "").strip(),
                    float(it.get("quantity") or 0), float(it.get("baseGrossAmount") or 0), float(it.get("baseNetAmount") or 0)))
    return out


ITEM_COLS = ["Invoice No", "Date", "Branch", "Brand Name", "Source", "Session", "Store Type", "Region", "Channel",
             "Status", "Reason", "Source Reason", "Remarks", "Bucket", "Listed", "Line", "Item Name", "Category",
             "Qty", "Item Gross", "Item Net"]


def build_item_rows(raw, reasons, maps, status_field, reason_field, cancel_statuses):
    """One row per item line of each cancelled order. The order's own fields (reason, bucket, branch...) come from
    build_rows, so an item row and its order row always agree."""
    rows = []
    for rec in raw:
        orders, _, _ = build_rows([rec], reasons, maps, status_field, reason_field, cancel_statuses)
        if not orders:
            continue
        o = orders[0]
        for n, (name, category, qty, gross, net) in enumerate(item_lines(rec), 1):
            rows.append({**{k: o.get(k, "") for k in ITEM_COLS if k not in ("Line", "Item Name", "Category", "Qty",
                                                                             "Item Gross", "Item Net")},
                         "Line": n, "Item Name": name, "Category": category, "Qty": qty,
                         "Item Gross": gross, "Item Net": net})
    return rows


def write_errors(out, errors):
    pd.DataFrame(errors).to_csv(os.path.join(out, "cancel_errors.csv"), index=False)


def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("--from", dest="start", required=True, help="first business day, YYYY-MM-DD")
    ap.add_argument("--to", dest="end", default="", help="last business day, YYYY-MM-DD (default: the same as --from)")
    ap.add_argument("--out", default="data/cancel")
    ap.add_argument("--probe", action="store_true", help="try each candidate request once and report what works")
    ap.add_argument("--endpoint", default=DEFAULT_ENDPOINT, help="Rista endpoint for orders")
    ap.add_argument("--status-field", default="auto", help="the status field name, or auto to find every status field")
    ap.add_argument("--reason-field", default="auto", help="the reason field name, or auto to find it")
    ap.add_argument("--cancel-statuses", default="cancel,void", help="a status containing one of these is cancelled")
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

    keys = sorted({k for r in raw[:200] for k in flatten(r).keys() if not k.startswith("_")})
    with open(os.path.join(a.out, "cancel_raw_sample.json"), "w", encoding="utf-8") as fh:
        json.dump({"keys": keys, "sample": [{k: v for k, v in r.items() if not k.startswith("_")} for r in raw[:3]]},
                  fh, indent=2, default=str)
    flat0 = flatten(raw[0]) if raw else {}
    if raw and not status_fields(flat0, a.status_field):
        print(f"no status field in the response. Keys seen: {keys}")
        return 2
    if raw and a.reason_field != "auto" and a.reason_field not in flat0:
        print(f"reason field '{a.reason_field}' not in the response. Keys seen: {keys}")
        return 2
    print(f"status fields used: {status_fields(flat0, a.status_field)}; reason fields: {', '.join(REASON_KEYS)}")

    cancelled, status_count, reason_count = build_rows(raw, reasons, maps, a.status_field, a.reason_field, cancel_set)
    pd.DataFrame(sorted(status_count.items(), key=lambda x: -x[1]), columns=["Status", "Orders"]) \
        .to_csv(os.path.join(a.out, f"status_seen_{tag}.csv"), index=False)
    inv = pd.DataFrame(cancelled)
    if inv.empty:
        inv = pd.DataFrame(columns=["Invoice No", "Date", "Branch", "Brand Name", "Source", "Session", "Store Type",
                                    "Region", "Channel", "Status", "Reason", "Bucket", "Listed", "Orders",
                                    "Net Sales", "Gross Sales", "Discount"])
    inv.to_csv(os.path.join(a.out, f"cancel_invoices_{tag}.csv"), index=False)
    items = pd.DataFrame(build_item_rows(raw, reasons, maps, a.status_field, a.reason_field, cancel_set), columns=ITEM_COLS)
    items.to_csv(os.path.join(a.out, f"cancel_items_{tag}.csv"), index=False)
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
    print(f"status values seen: {len(status_count)} (see status_seen_{tag}.csv); reason fields: {", ".join(REASON_KEYS)}")
    return 1 if errors else 0


if __name__ == "__main__":
    sys.exit(main())
