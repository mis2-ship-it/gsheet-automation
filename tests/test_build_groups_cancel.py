"""Checks the cancel cut per login. Run from the repo root: python tests/test_build_groups_cancel.py"""
import gzip, os, sys, tempfile
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
import pandas as pd
import build_groups as bg

PASS = FAIL = 0


def check(name, cond, extra=""):
    global PASS, FAIL
    PASS, FAIL = (PASS + 1, FAIL) if cond else (PASS, FAIL + 1)
    print(("PASS " if cond else "FAIL ") + name + (f"  [{extra}]" if extra else ""))


work = tempfile.mkdtemp()
src = os.path.join(work, "cancel_orders.csv.gz")
pd.DataFrame([["A1", "2025-04-02", "Madno Koramangala", "Item OOS"],
              ["A2", "2025-04-01", "Frozen Bottle Indiranagar", "Others"],
              ["A3", "2025-04-03", "Other Store", "Others"]],
             columns=["Invoice No", "Date", "Branch", "Bucket"]).to_csv(src, index=False, compression="gzip")
allowed = [bg.clean("Madno Koramangala")]
out_lim = os.path.join(work, "lim", "cancel_orders.csv.gz")
bg.cancel_for_group(src, out_lim, allowed)
lim = pd.read_csv(out_lim, dtype=str, keep_default_na=False)
check("a store-limited login gets only its stores' cancelled orders", list(lim["Invoice No"]) == ["A1"], str(list(lim["Invoice No"])))
out_all = os.path.join(work, "all", "cancel_orders.csv.gz")
bg.cancel_for_group(src, out_all, None)
check("the all-stores login gets every cancelled order", len(pd.read_csv(out_all, dtype=str)) == 3)
check("no other store's name is in the limited file", not any(n in set(lim["Branch"]) for n in ["Other Store", "Frozen Bottle Indiranagar"]))
print(f"{PASS} passed, {FAIL} failed")
sys.exit(1 if FAIL else 0)
