"""Checks the probe, the reason-field detection, the early stop and the API's own error text. Run from the repo root:
    python tests/test_cancel_probe.py
"""
import contextlib, io, json, os, sys, tempfile

os.environ.setdefault("API_KEY", "test-key")      # dummy values: every request is faked
os.environ.setdefault("SECRET_KEY", "test-secret")
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


class Resp:
    def __init__(self, code, body=None, text=""):
        self.status_code, self._body = code, body or {}
        self.text = text or json.dumps(self._body)

    def json(self):
        return self._body


cs.time.sleep = lambda s: None
REASONS = os.path.join(os.path.dirname(HERE), "data", "cancel_reasons.csv")
cs.load_maps = lambda: ({"B1": "COCO", "B2": "FOFO"}, {"B1": "KA", "B2": "TN"}, {"Swiggy": "Swiggy"}, {"Swiggy": "Frozen Bottle"})
cs.active_branches = lambda: ["B1", "B2"]

# 1. the probe: /sales/page answers, /sale answers 400 with the API's message
def probe_get(url, headers=None, params=None, timeout=None):
    if url.endswith("/sales/page"):
        return Resp(200, {"data": [{"invoiceNumber": "1", "status": "Closed", "reason": "", "invoiceDate": "2025-04-01T13:00:00"},
                                   {"invoiceNumber": "2", "status": "Voided", "reason": "outlet closed", "invoiceDate": "2025-04-01T19:00:00"}],
                           "lastKey": None})
    if url.endswith("/sale"):
        return Resp(400, text='{"message":"fromDate is required"}')
    return Resp(404, {})


real_get = cs.requests.get
cs.requests.get = probe_get
report = cs.probe_candidates("B1", "2025-04-01")
page = next(r for r in report if r["endpoint"] == "/sales/page" and r["params"] == "branch,day")
check("the probe finds /sales/page working, with 2 orders", page["http"] == 200 and page["orders"] == 2, str(page))
check("the probe names the reason field and the statuses it sees", page["reason_fields"] == "reason" and "Voided=1" in page["statuses"], str(page))
bad = next(r for r in report if r["endpoint"] == "/sale" and r["params"] == "branch,day")
check("the probe reports the API's own message for a failing request", bad["http"] == 400 and "fromDate is required" in bad["message"], str(bad))

with tempfile.TemporaryDirectory() as out:
    buf = io.StringIO()
    with contextlib.redirect_stdout(buf):
        code = cs.main(["--probe", "--from", "2025-04-01", "--out", out])
    check("--probe writes probe_report.json and exits 0", code == 0 and os.path.exists(os.path.join(out, "probe_report.json")))
cs.requests.get = real_get

# 2. the reason field is found by name when it is not called "reason"
def orders_get(url, params=None):
    return {"data": [{"invoiceNumber": "9", "status": "Cancelled", "cancelReason": "Out of stock",
                      "invoiceDate": "2025-04-01T13:00:00", "branchName": params["branch"], "channel": "Swiggy",
                      "netAmount": 10, "chargeAmount": 0, "grossAmount": 10, "discountAmount": 0}], "lastKey": None}


cs.get_json = orders_get
with tempfile.TemporaryDirectory() as out:
    with contextlib.redirect_stdout(io.StringIO()):
        code = cs.main(["--from", "2025-04-01", "--out", out, "--reasons", REASONS, "--branches", "B1"])
    import csv
    rows = list(csv.DictReader(open(os.path.join(out, "cancel_invoices_2025-04-01_2025-04-01.csv"))))
    check("the reason field is found by name (cancelReason), and the order is classified", code == 0 and rows and rows[0]["Bucket"] == "Item OOS", str(rows[:1]))

# 3. nothing works: the run stops after 20 failed branch-days, with the API's message
calls = {"n": 0}


def always_400(url, params=None):
    calls["n"] += 1
    raise RuntimeError(f"{url} {params}: HTTP 400 fromDate is required")


cs.get_json = always_400
with tempfile.TemporaryDirectory() as out:
    buf = io.StringIO()
    with contextlib.redirect_stdout(buf):
        code = cs.main(["--from", "2025-04-01", "--to", "2025-04-30", "--out", out, "--reasons", REASONS,
                        "--branches", ",".join(f"B{i}" for i in range(1, 13))])
    text = buf.getvalue()
    check("when nothing works the run stops early (exit 2), not after every branch-day", code == 2 and calls["n"] < 60,
          f"exit {code}, calls {calls['n']} of 360")
    check("the stop message includes the API's own message", "fromDate is required" in text, text[-300:])

print(f"{PASS} passed, {FAIL} failed")
sys.exit(1 if FAIL else 0)
