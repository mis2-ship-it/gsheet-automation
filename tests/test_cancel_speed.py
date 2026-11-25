"""Checks the request rules, resume and progress of cancel_sales.py. Run from the repo root:
    python tests/test_cancel_speed.py
"""
import contextlib, io, json, os, sys, tempfile

os.environ.setdefault("API_KEY", "test-key")      # dummy values: the requests are faked, nothing is sent
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
    def __init__(self, code, body=None):
        self.status_code, self._body = code, body or {}
        self.text = json.dumps(self._body)

    def json(self):
        return self._body


def scripted(codes):
    """requests.get stand-in: returns the given status codes in turn (a 200 carries one order)."""
    calls = {"n": 0}

    def get(url, headers=None, params=None, timeout=None):
        i = calls["n"]
        calls["n"] += 1
        code = codes[min(i, len(codes) - 1)]
        return Resp(code, {"data": [{"invoiceNumber": "1"}], "lastKey": None} if code == 200 else {})
    return get, calls


real_get = cs.requests.get
real_sleep = cs.time.sleep
cs.time.sleep = lambda s: None   # no pauses in the tests

g, c = scripted([404])
cs.requests.get = g
out = cs.get_json("u", {"branch": "B1"})
check("404 means no orders: an empty day, with one request and no retry", out == {"data": [], "lastKey": None} and c["n"] == 1, str(c["n"]))

g, c = scripted([500, 500, 200])
cs.requests.get = g
out = cs.get_json("u", {"branch": "B1"})
check("server errors are retried until one works", len(out["data"]) == 1 and c["n"] == 3, str(c["n"]))

g, c = scripted([400])
cs.requests.get = g
try:
    cs.get_json("u", {"branch": "B1"})
    raised = False
except RuntimeError as e:
    raised = "HTTP 400" in str(e)
check("a 400 (bad request) fails at once, with no retry", raised and c["n"] == 1, str(c["n"]))

g, c = scripted([429])
cs.requests.get = g
try:
    cs.get_json("u", {"branch": "B1"})
    raised = False
except RuntimeError as e:
    raised = "HTTP 429" in str(e)
check("rate limits (429) are retried three times, then reported", raised and c["n"] == 3, str(c["n"]))
cs.requests.get = real_get
cs.time.sleep = real_sleep

# resume and progress, on a few branches and days
reasons = os.path.join(os.path.dirname(HERE), "data", "cancel_reasons.csv")
cs.load_maps = lambda: ({"B1": "COCO", "B2": "FOFO"}, {"B1": "KA", "B2": "TN"},
                        {"Swiggy": "Swiggy"}, {"Swiggy": "Frozen Bottle"})
cs.active_branches = lambda: ["B1", "B2", "B3"]
calls = {"n": 0}


def counting(url, params=None):
    calls["n"] += 1
    return {"data": [{"invoiceNumber": f"{params['branch']}-{params['day']}", "status": "Voided", "reason": "outlet closed",
                      "channel": "Swiggy", "branchName": params["branch"], "invoiceDate": params["day"] + "T13:00:00",
                      "netAmount": 10, "chargeAmount": 0, "grossAmount": 12, "discountAmount": 2}], "lastKey": None}


cs.get_json = counting
with tempfile.TemporaryDirectory() as out:
    buf = io.StringIO()
    with contextlib.redirect_stdout(buf):
        code1 = cs.main(["--from", "2025-04-01", "--to", "2025-04-03", "--out", out, "--reasons", reasons,
                         "--progress-every", "3"])
    first_calls = calls["n"]
    text = buf.getvalue()
    check("the first run pulls every branch-day (3 branches x 3 days = 9 requests)", first_calls == 9 and code1 == 0, str(first_calls))
    check("progress is printed as the run goes, with the time left", "9/9 branch-days" in text and "min left" in text, text[-300:])
    saved = os.path.join(out, "raw", "sales_page", "2025-04-02", "B1.json.gz")
    check("each branch-day is saved to its own file (raw/sales_page/<day>/<branch>.json.gz)", os.path.exists(saved), saved)
    calls["n"] = 0
    buf2 = io.StringIO()
    with contextlib.redirect_stdout(buf2):
        code2 = cs.main(["--from", "2025-04-01", "--to", "2025-04-03", "--out", out, "--reasons", reasons])
    check("a second run reuses the saved branch-days (no new requests)", calls["n"] == 0 and code2 == 0, str(calls["n"]))
    check("the second run reports the saved branch-days", "9 branch-days from saved files" in buf2.getvalue() or "saved 9" in buf2.getvalue(), buf2.getvalue()[-200:])
    calls["n"] = 0
    with contextlib.redirect_stdout(io.StringIO()):
        cs.main(["--from", "2025-04-01", "--to", "2025-04-03", "--out", out, "--reasons", reasons, "--fresh"])
    check("--fresh pulls every branch-day again", calls["n"] == 9, str(calls["n"]))

print(f"{PASS} passed, {FAIL} failed")
sys.exit(1 if FAIL else 0)
