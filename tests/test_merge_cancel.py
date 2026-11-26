"""Checks merge_cancel.py. Run from the repo root: python tests/test_merge_cancel.py"""
import os, sys, tempfile
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
import pandas as pd
import merge_cancel as mc

PASS = FAIL = 0


def check(name, cond, extra=""):
    global PASS, FAIL
    PASS, FAIL = (PASS + 1, FAIL) if cond else (PASS, FAIL + 1)
    print(("PASS " if cond else "FAIL ") + name + (f"  [{extra}]" if extra else ""))


work = tempfile.mkdtemp()
master = os.path.join(work, "cancel_orders.csv.gz")
run1 = os.path.join(work, "run1.csv")
run2 = os.path.join(work, "run2.csv")
empty = os.path.join(work, "empty.csv")
cols = ["Invoice No", "Date", "Branch", "Status", "Reason", "Bucket", "Net Sales"]
pd.DataFrame([["A1", "2025-04-02", "Madno Koramangala", "status=Cancelled", "out of stock", "Item OOS", "100"],
              ["A2", "2025-04-01", "Madno Koramangala", "status=Voided", "", "Others", "50"]], columns=cols).to_csv(run1, index=False)
# run 2: A1 is corrected (a different reason), A3 is new, and A2 is repeated unchanged
pd.DataFrame([["A1", "2025-04-02", "Madno Koramangala", "status=Cancelled", "outlet closed", "Closed in hours", "100"],
              ["A3", "2025-04-03", "Frozen Bottle Indiranagar", "status=Voided", "electricity issue", "Device/Power", "70"],
              ["A2", "2025-04-01", "Madno Koramangala", "status=Voided", "", "Others", "50"]], columns=cols).to_csv(run2, index=False)
pd.DataFrame(columns=cols).to_csv(empty, index=False)

mc.main(["--new", run1, "--master", master])
m = pd.read_csv(master, dtype=str, keep_default_na=False)
check("the first run creates the stored file with its rows", len(m) == 2, str(len(m)))
mc.main(["--new", run2, "--master", master])
m = pd.read_csv(master, dtype=str, keep_default_na=False)
check("a second run adds the new order and keeps the others", len(m) == 3 and "A3" in set(m["Invoice No"]), str(len(m)))
a1 = m[m["Invoice No"] == "A1"].iloc[0]
check("a changed order is replaced, not duplicated", (m["Invoice No"] == "A1").sum() == 1 and a1["Bucket"] == "Closed in hours")
check("the stored rows are sorted by date", list(m["Date"]) == sorted(m["Date"]), str(list(m["Date"])))
mc.main(["--new", run2, "--master", master])
check("repeating the same run changes nothing", len(pd.read_csv(master, dtype=str, keep_default_na=False)) == 3)
before = os.path.getmtime(master)
mc.main(["--new", empty, "--master", master])
check("a run with no cancelled orders leaves the stored file alone", len(pd.read_csv(master, dtype=str, keep_default_na=False)) == 3)
print(f"{PASS} passed, {FAIL} failed")
sys.exit(1 if FAIL else 0)
