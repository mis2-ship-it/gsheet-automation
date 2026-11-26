"""Checks the per-login metrics cut in build_groups.py. Run from the repo root: python tests/test_build_groups_metrics.py"""
import gzip, json, os, sys, tempfile
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
import build_groups as bg

SRC = os.path.join(ROOT, "tests", "fixtures", "metrics.json.gz")
tmp = tempfile.mkdtemp()
PASS = FAIL = 0


def check(name, cond, extra=""):
    global PASS, FAIL
    PASS, FAIL = (PASS + 1, FAIL) if cond else (PASS, FAIL + 1)
    print(("PASS " if cond else "FAIL ") + name + (f"  [{extra}]" if extra else ""))


def load(p):
    with gzip.open(p, "rt", encoding="utf-8") as f:
        return json.load(f)


src = load(SRC)
names = src["stores"]
# a store-limited login: three stores that have rows in the file, written the way the sales data writes them
with_rows = sorted({names[r[10]] for r in src["rows"] if len(r) > 10})
allowed_raw = with_rows[:3]
allowed = [bg.clean(n) for n in allowed_raw]

# 1. all stores: the file is copied as it is
out_all = os.path.join(tmp, "all", "metrics.json.gz")
bg.metrics_for_group(SRC, out_all, None)
a = load(out_all)
check("all-stores login: every row and store is kept", len(a["rows"]) == len(src["rows"]) and a["stores"] == names)

# 2. a store-limited login: only its stores' rows
out_lim = os.path.join(tmp, "lim", "metrics.json.gz")
bg.metrics_for_group(SRC, out_lim, allowed)
L = load(out_lim)
expected_rows = [r for r in src["rows"] if len(r) > 10 and bg.clean(names[r[10]]) in set(allowed)]
check("store-limited login: only its stores' rows", len(L["rows"]) == len(expected_rows), f"{len(L['rows'])} vs {len(expected_rows)}")
check("store-limited login: the store list is cut to its stores", set(L["stores"]) <= set(allowed_raw) and len(L["stores"]) > 0, str(L["stores"]))
check("store indices point at the cut list", all(0 <= r[10] < len(L["stores"]) for r in L["rows"]))
for s in allowed_raw:
    got = sum(r[7] for r in L["rows"] if L["stores"][r[10]] == s)
    want = sum(r[7] for r in src["rows"] if len(r) > 10 and names[r[10]] == s)
    check(f"orders for {s} match the source", abs(got - want) < 1e-6, f"{got} vs {want}")
leaked = set(names) - set(allowed_raw)
check("no other store's name is in the limited file", not any(n in L["stores"] for n in leaked))
check("the rest of the file is unchanged (dates, sources, brands)", L["sources"] == src["sources"] and L["brands"] == src["brands"] and L["fields"] == src["fields"])

# 3. an old file with no store names: a store-limited login gets no rows; the all-stores login gets it
old = {"epoch": src["epoch"], "sources": src["sources"], "brands": src["brands"], "regions": src["regions"],
       "storeTypes": src["storeTypes"], "fields": src["fields"][:10], "rows": [r[:10] for r in src["rows"]]}
old_path = os.path.join(tmp, "old.json.gz")
with gzip.open(old_path, "wt", encoding="utf-8") as f:
    json.dump(old, f)
bg.metrics_for_group(old_path, os.path.join(tmp, "old_lim", "metrics.json.gz"), allowed)
check("old file, store-limited login: no rows (no store names to match)", load(os.path.join(tmp, "old_lim", "metrics.json.gz"))["rows"] == [])

print(f"{PASS} passed, {FAIL} failed")
sys.exit(1 if FAIL else 0)
