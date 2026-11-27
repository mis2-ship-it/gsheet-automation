"""Item lines of the cancelled orders, and the merge keyed by line. Run from the repo root: python tests/test_cancel_items.py"""
import os, sys, tempfile
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
import pandas as pd
import cancel_sales as cs
import merge_cancel as mc

PASS = FAIL = 0


def check(name, cond, extra=""):
    global PASS, FAIL
    PASS, FAIL = (PASS + 1, FAIL) if cond else (PASS, FAIL + 1)
    print(("PASS " if cond else "FAIL ") + name + (f"  [{extra}]" if extra else ""))


base = {"invoiceDate": "2026-11-20T13:00:00", "branchName": "Madno Koramangala", "channel": "Swiggy",
        "netAmount": 400, "chargeAmount": 0, "grossAmount": 440, "discountAmount": 0}
raw = [
    {**base, "invoiceNumber": "C1", "status": "Closed", "items": [{"shortName": "Belgian Shake", "categoryName": "Shakes",
                                                                  "quantity": 1, "baseGrossAmount": 200, "baseNetAmount": 190}]},
    {**base, "invoiceNumber": "C2", "status": "Cancelled", "statusInfo": {"reason": "Cancelled by swiggy",
        "sourceReason": "as the ordered item(s) are now out of stock", "remarks": ""},
     "items": [{"shortName": "Belgian Shake", "categoryName": "Shakes", "quantity": 2, "baseGrossAmount": 400, "baseNetAmount": 380},
               {"shortName": "Cookie", "categoryName": "Bakery", "quantity": 1, "baseGrossAmount": 60, "baseNetAmount": 50}]},
    {**base, "invoiceNumber": "C3", "status": "Voided", "statusInfo": {"reason": "", "sourceReason": "", "remarks": ""}},
]
reasons = cs.load_reasons(os.path.join(ROOT, "data", "cancel_reasons.csv"))
maps = ({"Madno Koramangala": "COCO"}, {"Madno Koramangala": "KA"}, {"Swiggy": "Swiggy"}, {"Swiggy": "Frozen Bottle"})
items = cs.build_item_rows(raw, reasons, maps, "auto", "auto", ("cancel", "void"))
check("one row per item line of each cancelled order (C2 has two items, C3 has none, C1 is closed)",
      len(items) == 3, str([(r["Invoice No"], r["Item Name"]) for r in items]))
c2 = [r for r in items if r["Invoice No"] == "C2"]
check("the item rows keep the order's bucket and listed flag", all(r["Bucket"] == "Item OOS" and r["Listed"] == "Yes" for r in c2))
check("the item fields: name, category, qty and net", c2[0]["Item Name"] == "Belgian Shake" and c2[0]["Category"] == "Shakes"
      and c2[0]["Qty"] == 2 and c2[0]["Item Net"] == 380, str(c2[0]))
check("an order with no items keeps one line, named (no item)", [r["Item Name"] for r in items if r["Invoice No"] == "C3"] == ["(no item)"])
check("the lines are numbered within the order", [r["Line"] for r in c2] == [1, 2])
check("the invoice count is unchanged: two cancelled orders", len(cs.build_rows(raw, reasons, maps, "auto", "auto", ("cancel", "void"))[0]) == 2)
# the merge keyed by line: a run keeps two lines of one order, and a repeated run adds nothing
work = tempfile.mkdtemp()
new = os.path.join(work, "items.csv")
pd.DataFrame(items, columns=cs.ITEM_COLS).to_csv(new, index=False)
master = os.path.join(work, "cancel_items.csv.gz")
key = "Invoice No,Branch,Date,Line"
mc.main(["--new", new, "--master", master, "--key", key])
mc.main(["--new", new, "--master", master, "--key", key])
m = pd.read_csv(master, dtype=str, keep_default_na=False)
check("the stored items keep both lines of an order, and a repeated run adds nothing", len(m) == 3 and (m["Invoice No"] == "C2").sum() == 2, str(len(m)))
print(f"{PASS} passed, {FAIL} failed")
sys.exit(1 if FAIL else 0)
