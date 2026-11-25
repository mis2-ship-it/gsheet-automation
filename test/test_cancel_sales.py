"""Checks cancel_sales.py against a made-up /sale response (no company data). Run from the repo root:
    python tests/test_cancel_sales.py
"""
import csv, json, os, sys, tempfile

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(HERE))
import cancel_sales as cs

PASS = FAIL = 0


def check(name, cond, extra=""):
    global PASS, FAIL
    if cond:
        PASS += 1
        print("PASS", name)
    else:
        FAIL += 1
        print("FAIL", name, extra)


def inv(no, status, channel, when, reason=None, net=100.0):
    r = {"invoiceNumber": no, "status": status, "channel": channel, "branchName": None,
         "invoiceDate": when, "netAmount": net, "chargeAmount": 0, "grossAmount": net + 10, "discountAmount": 10}
    if reason is not None:
        r["reason"] = reason
    return r


# made-up responses, keyed by (branch, day, lastKey)
FAKE = {
    ("B1", "2025-04-01", None): {"data": [
        dict(inv("1", "Closed", "Swiggy", "2025-04-01T13:00:00"), branchName="B1"),
        dict(inv("2", "Voided", "Swiggy", "2025-04-01T19:00:00", "Item out of stock today"), branchName="B1"),
        dict(inv("3", "Cancelled", "Zomato", "2025-04-01T21:00:00", "Customer changed mind"), branchName="B1")],
        "lastKey": "k1"},
    ("B1", "2025-04-01", "k1"): {"data": [
        dict(inv("4", "Voided", "Swiggy", "2025-04-02T02:00:00", "The Restaurant is now CLOSED"), branchName="B1")]},
    ("B2", "2025-04-01", None): {"data": [
        dict(inv("5", "Voided", "Swiggy", "2025-04-01T12:00:00", ""), branchName="B2"),
        dict(inv("6", "Rejected", "Zomato", "2025-04-01T20:00:00", "whatever"), branchName="B2")]},
    ("B1", "2025-04-02", None): {"data": [dict(inv("7", "Closed", "Swiggy", "2025-04-02T13:00:00"), branchName="B1")]},
    ("B2", "2025-04-02", None): {"data": []},
}
MAPS = ({"B1": "COCO", "B2": "FOFO"}, {"B1": "KA", "B2": "TN"},
        {"Swiggy": "Swiggy", "Zomato": "Zomato"}, {"Swiggy": "Frozen Bottle", "Zomato": "Madno"})


def fake_get(url, params=None):
    if url.endswith("/sale"):
        if params["branch"] == "B3":
            raise RuntimeError("HTTP 500")
        key = (params["branch"], params["day"], params.get("lastKey"))
        return FAKE.get(key, {"data": []})
    raise AssertionError("unexpected url " + url)


cs.get_json = fake_get
cs.load_maps = lambda: MAPS
cs.active_branches = lambda: ["B1", "B2"]

# the classification rules
reasons = cs.load_reasons(os.path.join(os.path.dirname(HERE), "data", "cancel_reasons.csv"))
check("out of stock -> Item OOS (listed)", cs.classify("Item out of stock today", reasons) == ("Item OOS", True))
check("case-insensitive: CLOSED -> Closed in hours", cs.classify("The Restaurant is now CLOSED", reasons) == ("Closed in hours", True))
check("electricity issue -> Device/Power", cs.classify("electricity issue at store", reasons) == ("Device/Power", True))
check("a reason not on the list -> Others", cs.classify("Customer changed mind", reasons) == ("Others", False))
check("a blank reason -> Others", cs.classify("", reasons) == ("Others", False))

with tempfile.TemporaryDirectory() as out:
    code = cs.main(["--from", "2025-04-01", "--to", "2025-04-02", "--out", out,
                    "--reasons", os.path.join(os.path.dirname(HERE), "data", "cancel_reasons.csv")])
    check("the run finishes with no errors (exit 0)", code == 0, str(code))
    rows = list(csv.DictReader(open(os.path.join(out, "cancel_invoices_2025-04-01_2025-04-02.csv"))))
    nos = sorted(r["Invoice No"] for r in rows)
    check("cancelled orders: 2, 3, 4 and 5 (Closed and Rejected left out)", nos == ["2", "3", "4", "5"], str(nos))
    by = {r["Invoice No"]: r for r in rows}
    check("order 4 (after midnight) counts to the previous business day, 1 Apr", by["4"]["Date"] == "2025-04-01", by["4"]["Date"])
    check("order 4 sits in the Post Dinner session", by["4"]["Session"] == "Post Dinner", by["4"]["Session"])
    check("order 2 is Item OOS and listed", by["2"]["Bucket"] == "Item OOS" and by["2"]["Listed"] == "Yes")
    check("order 3 is Others and not listed", by["3"]["Bucket"] == "Others" and by["3"]["Listed"] == "No")
    check("order 5 (blank reason) is Others", by["5"]["Bucket"] == "Others" and by["5"]["Reason"] == "(blank)")
    check("each order counts as 1 (order level)", all(r["Orders"] == "1" for r in rows))
    check("store type, region, brand and source are mapped", by["2"]["Store Type"] == "COCO" and by["2"]["Region"] == "KA"
          and by["2"]["Brand Name"] == "Frozen Bottle" and by["2"]["Source"] == "Swiggy")
    summ = list(csv.DictReader(open(os.path.join(out, "cancel_summary_2025-04-01_2025-04-02.csv"))))
    check("summary totals match the orders (4)", sum(int(r["Orders"]) for r in summ) == 4)
    buckets = {}
    for r in summ:
        buckets[r["Bucket"]] = buckets.get(r["Bucket"], 0) + int(r["Orders"])
    check("bucket breakdown: Item OOS 1, Closed in hours 1, Others 2", buckets == {"Item OOS": 1, "Closed in hours": 1, "Others": 2}, str(buckets))
    st = {r["Status"]: int(r["Orders"]) for r in csv.DictReader(open(os.path.join(out, "status_seen_2025-04-01_2025-04-02.csv")))}
    check("status counts cover every order: Closed 2, Voided 3, Cancelled 1, Rejected 1", st == {"Closed": 2, "Voided": 3, "Cancelled": 1, "Rejected": 1}, str(st))
    seen = {r["Reason"]: int(r["Orders"]) for r in csv.DictReader(open(os.path.join(out, "cancel_reasons_seen_2025-04-01_2025-04-02.csv")))}
    check("reasons seen lists every reason text, including the one not on the list", seen.get("Customer changed mind") == 1 and seen.get("(blank)") == 1, str(seen))
    sample = json.load(open(os.path.join(out, "cancel_raw_sample.json")))
    check("the raw sample keeps the status and reason fields for checking", "status" in sample["keys"] and "reason" in sample["keys"])
    check("no error file when every pull works", not os.path.exists(os.path.join(out, "cancel_errors.csv")))

with tempfile.TemporaryDirectory() as out:
    code = cs.main(["--from", "2025-04-01", "--to", "2025-04-01", "--out", out, "--branches", "B1,B3",
                    "--reasons", os.path.join(os.path.dirname(HERE), "data", "cancel_reasons.csv")])
    check("a failed branch-day is listed and the job exits with 1", code == 1 and os.path.exists(os.path.join(out, "cancel_errors.csv")), str(code))

with tempfile.TemporaryDirectory() as out:
    code = cs.main(["--from", "2025-04-01", "--to", "2025-04-02", "--out", out, "--reason-field", "cancelReason",
                    "--reasons", os.path.join(os.path.dirname(HERE), "data", "cancel_reasons.csv")])
    check("a wrong reason field name stops the job with a message (exit 2)", code == 2, str(code))

print(f"{PASS} passed, {FAIL} failed")
sys.exit(1 if FAIL else 0)
