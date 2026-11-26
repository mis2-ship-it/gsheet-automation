"""Runs build_groups.py with the arguments the sales workflow passes (build_site.py is stubbed out).
Run from the repo root: python tests/test_build_groups_run.py"""
import gzip, json, os, sys, tempfile
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
src = json.load(gzip.open(os.path.join(ROOT, "tests", "fixtures", "metrics.json.gz"), "rt"))
names = src["stores"]
two = sorted({names[r[10]] for r in src["rows"] if len(r) > 10})[:2]
# the inputs: an access list (one login with two stores, one login with a store that has no sales), and sales history
access = os.path.join(work, "access.csv")
pd.DataFrame({"Email": ["asha@frozenbottle.in", "asha@frozenbottle.in", "ravi@example.com"],
              "Store Name": [two[0], two[1], "Nowhere Store"]}).to_csv(access, index=False)
history = os.path.join(work, "historical_sales.csv.gz")
pd.DataFrame({"Branch": two + ["Other Store"]}).to_csv(history, index=False, compression="gzip")
discounts = os.path.join(work, "discount_rows.csv")
open(discounts, "w").write("x\n")
metrics = os.path.join(ROOT, "tests", "fixtures", "metrics.json.gz")
mix = os.path.join(work, "product_mix.json.gz")
with gzip.open(mix, "wt") as f:
    json.dump({"ref": "2026-11-22", "categories": ["Shakes"], "rows": [[0, 0, 0, 0, 1]]}, f)
site, funcs = os.path.join(work, "site"), os.path.join(work, "functions")

calls = []
class Done(Exception):
    pass
bg.subprocess.run = lambda cmd, check=True: calls.append(cmd)
sys.argv = ["build_groups.py", "--access", access, "--history", history, "--discounts", discounts,
            "--metrics", metrics, "--product-mix", mix, "--site", site, "--functions", funcs]
try:
    bg.main()
    ran = True
except SystemExit as e:
    ran = False
    print("exit:", e)
check("the build accepts the workflow's arguments and runs", ran)
check("one build for all stores and one for the login with stores (the other login is refused)", len(calls) == 2, str(len(calls)))
check("every build gets --discounts", all("--discounts" in c for c in calls))
tokens = [c[c.index("--out") + 1] for c in calls]
for t in tokens:
    check("metrics file in " + os.path.basename(os.path.dirname(t)) + "/", os.path.exists(os.path.join(t, "metrics.json.gz")))
all_dir = [t for t in tokens if "--only-stores" not in calls[tokens.index(t)]][0]
check("product mix only in the all-stores login", os.path.exists(os.path.join(all_dir, "product_mix.json.gz")) and
      sum(os.path.exists(os.path.join(t, "product_mix.json.gz")) for t in tokens) == 1)
ravi = [t for t in tokens if t != all_dir]
lim = [json.load(gzip.open(os.path.join(t, "metrics.json.gz"), "rt")) for t in tokens]
check("store-limited logins get only their stores", all(set(d["stores"]) <= set(two) for d in lim[1:]), str([d["stores"] for d in lim[1:]]))
check("the all-stores login gets every store", len(lim[0]["stores"]) == len(names))
print(f"{PASS} passed, {FAIL} failed")
sys.exit(1 if FAIL else 0)
