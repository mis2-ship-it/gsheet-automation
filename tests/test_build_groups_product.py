"""The product day files, cut per login. Run from the repo root: python tests/test_build_groups_product.py"""
import os, sys, tempfile
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
import pandas as pd
import build_groups as bg
import product_sales as ps

PASS = FAIL = 0


def check(name, cond, extra=""):
    global PASS, FAIL
    PASS, FAIL = (PASS + 1, FAIL) if cond else (PASS, FAIL + 1)
    print(("PASS " if cond else "FAIL ") + name + (f"  [{extra}]" if extra else ""))


folder = tempfile.mkdtemp()
def row(day, branch, item, net):
    return {"Date": day, "Branch": branch, "Brand": "Madno", "Region": "KA", "Store Type": "COCO", "Source": "Swiggy",
            "Category": "Shakes", "Item": item, "Orders": 1, "Qty": 1, "Gross": net, "Discount": 0, "Net": net}
pd.DataFrame([row("2026-11-24", "Madno Koramangala", "Belgian Shake", 190), row("2026-11-24", "Other Store", "Cookie", 50)],
             columns=ps.PRODUCT_COLS).to_csv(os.path.join(folder, "2026-11-24.csv.gz"), index=False, compression="gzip")
pd.DataFrame([row("2026-11-25", "Madno Koramangala", "Mango Shake", 200)], columns=ps.PRODUCT_COLS).to_csv(
    os.path.join(folder, "2026-11-25.csv.gz"), index=False, compression="gzip")
out_all = os.path.join(folder, "out_all", "product_items.csv.gz")
bg.product_for_group(folder, out_all, None)
a = pd.read_csv(out_all, dtype=str, keep_default_na=False)
check("the all-stores login gets every row of every day file", len(a) == 3 and set(a["Date"]) == {"2026-11-24", "2026-11-25"})
out_lim = os.path.join(folder, "out_lim", "product_items.csv.gz")
bg.product_for_group(folder, out_lim, [bg.clean("Madno Koramangala")])
l = pd.read_csv(out_lim, dtype=str, keep_default_na=False)
check("a store-limited login gets only its stores' rows", len(l) == 2 and "Other Store" not in set(l["Branch"]), str(len(l)))
check("the columns are the ones the page reads", list(l.columns) == ps.PRODUCT_COLS)
empty = tempfile.mkdtemp()
out_empty = os.path.join(empty, "out", "product_items.csv.gz")
bg.product_for_group(empty, out_empty, None)
e = pd.read_csv(out_empty, dtype=str, keep_default_na=False)
check("with no day files yet, the file has the header and no rows", len(e) == 0 and list(e.columns) == ps.PRODUCT_COLS)
print(f"{PASS} passed, {FAIL} failed")
sys.exit(1 if FAIL else 0)
