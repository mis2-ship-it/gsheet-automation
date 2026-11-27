"""Runs build_metrics.py from start to finish, as the workflow does: the workbook, the run page summary, and the
Google Sheet (the Drive download is replaced by the workbook's bytes). Run from the repo root:
    METRICS_XLSX=Metrics_Data.xlsx python tests/test_build_metrics_main.py"""
import gzip, json, os, sys, tempfile
HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE) if os.path.basename(HERE) == "tests" else HERE
sys.path.insert(0, ROOT)
import build_metrics as bm

XLSX = os.environ.get("METRICS_XLSX", "/mnt/user-data/uploads/Metrics_Data.xlsx")
PASS = FAIL = 0


def check(name, cond, extra=""):
    global PASS, FAIL
    PASS, FAIL = (PASS + 1, FAIL) if cond else (PASS, FAIL + 1)
    print(("PASS " if cond else "FAIL ") + name + (f"  [{extra}]" if extra else ""))


def load(p):
    with gzip.open(p, "rt", encoding="utf-8") as f:
        return json.load(f)


work = tempfile.mkdtemp()
os.environ.pop("GITHUB_STEP_SUMMARY", None)
out1 = os.path.join(work, "workbook.json.gz")
sys.argv = ["build_metrics.py", "--xlsx", XLSX, "--out", out1]
bm.main()
check("the workbook run writes the file", os.path.exists(out1) and os.path.getsize(out1) > 0)

summ = os.path.join(work, "summary.md")
os.environ["GITHUB_STEP_SUMMARY"] = summ
out2 = os.path.join(work, "summary_run.json.gz")
sys.argv = ["build_metrics.py", "--xlsx", XLSX, "--out", out2]
bm.main()
text = open(summ, encoding="utf-8").read() if os.path.exists(summ) else ""
check("the run page gets the days, the rows and the stores", "2026-11-01 to 2026-11-22" in text and "rows:" in text and "stores:" in text, text[:60])
os.environ.pop("GITHUB_STEP_SUMMARY", None)

xlsx_bytes = open(XLSX, "rb").read()
bm.download_sheet = lambda sheet_id, creds: (xlsx_bytes, "Metrics Data")
os.environ["GOOGLE_CREDENTIALS"] = '{"type": "service_account"}'
out3 = os.path.join(work, "sheet.json.gz")
sys.argv = ["build_metrics.py", "--gsheet", "1s9tNe9QnlOzbOgsrus-pvlZzulRDxGft", "--out", out3]
bm.main()
check("the Google Sheet run writes the file", os.path.exists(out3))
check("the Google Sheet gives the same data as the workbook", load(out3)["rows"] == load(out1)["rows"])
print(f"{PASS} passed, {FAIL} failed")
sys.exit(1 if FAIL else 0)
