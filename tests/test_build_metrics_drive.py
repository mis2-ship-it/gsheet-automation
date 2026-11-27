"""Reading the metrics file through Google Drive. Run from the repo root:
    METRICS_XLSX=Metrics_Data.xlsx python tests/test_build_metrics_drive.py"""
import io, os, sys
HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE) if os.path.basename(HERE) == "tests" else HERE
sys.path.insert(0, ROOT)
import pandas as pd
import requests
import build_metrics as bm

XLSX = os.environ.get("METRICS_XLSX", "/mnt/user-data/uploads/Metrics_Data.xlsx")
FILE_ID = "1s9tNe9QnlOzbOgsrus-pvlZzulRDxGft"
PASS = FAIL = 0


def check(name, cond, extra=""):
    global PASS, FAIL
    PASS, FAIL = (PASS + 1, FAIL) if cond else (PASS, FAIL + 1)
    print(("PASS " if cond else "FAIL ") + name + (f"  [{extra}]" if extra else ""))


ref_frames = bm.read_xlsx(XLSX)
ref = bm.build(ref_frames)
XLSX_BYTES = open(XLSX, "rb").read()


class Resp:
    def __init__(self, status, content=b"", js=None):
        self.status_code, self.content, self._js = status, content, js
        self.text = content.decode("utf-8", "replace") if isinstance(content, bytes) else ""
    def json(self):
        return self._js


CALLS, TOKENS = [], []
STATE = {"mime": bm.XLSX_MIME, "meta": 200, "body": XLSX_BYTES, "body_status": 200}


def fake_get(url, params=None, headers=None, timeout=None):
    CALLS.append({"url": url, "params": dict(params or {}), "headers": dict(headers or {})})
    if url == f"{bm.DRIVE_FILES}/{FILE_ID}" and "alt" not in (params or {}):
        if STATE["meta"] != 200:
            return Resp(STATE["meta"], b"permission denied")
        return Resp(200, js={"id": FILE_ID, "name": "Metrics Data", "mimeType": STATE["mime"]})
    return Resp(STATE["body_status"], STATE["body"])


requests.get = fake_get
bm.drive_token = lambda creds: (TOKENS.append(creds) or "TOKEN")

# 1. an .xlsx file kept in Drive: downloaded as it is, and builds the same data as the workbook
got = bm.build(bm.read_gsheet(FILE_ID, '{"type": "service_account"}'))
check("an .xlsx file in Drive is downloaded (alt=media)", CALLS[-1]["params"].get("alt") == "media")
check("the request carries the Drive token", CALLS[-1]["headers"].get("Authorization") == "Bearer TOKEN")
check("the same rows as the workbook", got["rows"] == ref["rows"], f"{len(got['rows'])} vs {len(ref['rows'])}")
check("the same dates, sources, brands, regions and stores",
      all(got[k] == ref[k] for k in ["dateMin", "dateMax", "sources", "brands", "regions", "storeTypes", "stores"]),
      f"{got['dateMin']} to {got['dateMax']}")
# 2. a Google Sheet: exported as .xlsx, and builds the same data
STATE["mime"] = bm.SHEET_MIME
got2 = bm.build(bm.read_gsheet(FILE_ID, "/keys/service.json"))
check("a Google Sheet is exported as .xlsx", CALLS[-1]["url"].endswith("/export") and CALLS[-1]["params"].get("mimeType") == bm.XLSX_MIME)
check("a Google Sheet builds the same rows", got2["rows"] == ref["rows"])
check("a key file path is passed to the token", TOKENS[-1] == "/keys/service.json")
STATE["mime"] = bm.XLSX_MIME
# 3. the file cannot be opened: the reason, and what to share or enable
STATE["meta"] = 403
try:
    bm.read_gsheet(FILE_ID, "/k.json"); msg = ""
except SystemExit as e:
    msg = str(e)
check("a file that cannot be opened says to share it and enable Drive", "Share it" in msg and "Google Drive API" in msg, msg[:80])
STATE["meta"] = 200
# 4. the download fails
STATE["body_status"] = 500
try:
    bm.read_gsheet(FILE_ID, "/k.json"); msg = ""
except SystemExit as e:
    msg = str(e)
check("a failed download says so", "could not download" in msg, msg[:60])
STATE["body_status"] = 200
# 5. a missing tab stops with the tabs it found
buf = io.BytesIO()
with pd.ExcelWriter(buf, engine="openpyxl") as w:
    for n in ["Z_Funnel", "Z_Ads", "S_Funnel"]:
        ref_frames[n].to_excel(w, sheet_name=n, index=False)
STATE["body"] = buf.getvalue()
try:
    bm.read_gsheet(FILE_ID, "/k.json"); msg = ""
except SystemExit as e:
    msg = str(e)
check("a missing tab is named, with the tabs found", "S_Ads" in msg and "Tabs found" in msg, msg[:90])
print(f"{PASS} passed, {FAIL} failed")
sys.exit(1 if FAIL else 0)
