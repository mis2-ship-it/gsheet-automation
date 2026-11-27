"""Product sales: item lines, closed orders only, distinct orders, the business day, and the per-day files with their
retention. Run from the repo root (it imports sync_sales.py and cancel_sales.py): python tests/test_product_sales.py"""
import os, sys, tempfile
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
import pandas as pd
import product_sales as ps

PASS = FAIL = 0


def check(name, cond, extra=""):
    global PASS, FAIL
    PASS, FAIL = (PASS + 1, FAIL) if cond else (PASS, FAIL + 1)
    print(("PASS " if cond else "FAIL ") + name + (f"  [{extra}]" if extra else ""))


base = {"invoiceDate": "2026-11-20T13:00:00", "branchName": "Madno Koramangala", "channel": "Swiggy", "brandName": "Madno",
        "status": "Closed"}
maps = ({"Madno Koramangala": "COCO"}, {"Madno Koramangala": "KA"}, {"Swiggy": "Swiggy"}, {"Swiggy": "Frozen Bottle"})
shake = {"shortName": "Belgian Shake", "categoryName": "Shakes", "quantity": 1, "baseGrossAmount": 200,
         "baseNetAmount": 190, "baseNetDiscountAmount": -20}
raw = [
    {**base, "invoiceNumber": "A1", "items": [shake, dict(shake, quantity=2, baseGrossAmount=400, baseNetAmount=380,
                                                          baseNetDiscountAmount=-40)]},   # two lines, one order
    {**base, "invoiceNumber": "A2", "items": [shake]},
    {**base, "invoiceNumber": "A3", "status": "Voided", "items": [shake]},                # not a closed order
    {**base, "invoiceNumber": "A4", "invoiceDate": "2026-11-21T02:30:00", "items": [shake]},  # before 5 AM: 20 Nov
]
check("the discount is kept as a positive amount", ps.product_lines(raw[0])[0][5] == 20.0)
check("an order with no items gives one (no item) line", ps.product_lines({"invoiceNumber": "X"}) == [("(no item)", "", 0.0, 0.0, 0.0, 0.0)])
agg = ps.aggregate(raw, maps)
check("only closed orders are counted: the voided order A3 is left out", len(agg) > 0)
day20 = agg[agg["Date"] == "2026-11-20"]
check("the early-morning order belongs to the day before (20 Nov)", len(agg[agg["Date"] == "2026-11-21"]) == 0)
one = day20[(day20["Item"] == "Belgian Shake")].iloc[0]
check("Orders counts distinct orders: A1 (two lines), A2 and A4 (before 5 AM) = 3, so the voided A3 is not in it",
      one["Orders"] == 3, str(one["Orders"]))
check("Qty is 5 (A1: 1 + 2, A2: 1, A4: 1)", one["Qty"] == 5, str(one["Qty"]))
check("Gross is 1,000 (200 + 400 + 200 + 200)", one["Gross"] == 1000, str(one["Gross"]))
check("Discount is 100 (20 + 40 + 20 + 20)", one["Discount"] == 100, str(one["Discount"]))
check("Net is 950 (190 + 380 + 190 + 190)", one["Net"] == 950, str(one["Net"]))
check("the group carries the store, brand, region, type and source", one["Branch"] == "Madno Koramangala" and one["Region"] == "KA"
      and one["Store Type"] == "COCO" and one["Source"] == "Swiggy" and one["Brand"] == "Madno" and one["Category"] == "Shakes")
# the files: one per day, a failed day is skipped, and a folder keeps only keep_days
work = tempfile.mkdtemp()
old = os.path.join(work, "2026-09-01.csv.gz")
open(old, "wb").write(b"x")
rep = ps.write_days(agg, work, keep_days=62, skip_days={"2026-11-21"})
names = sorted(f for f in os.listdir(work))
check("one file per day, with the day as its name", names == ["2026-11-20.csv.gz"], str(names))
check("the report gives the rows and bytes of each day written", rep and rep[0][0] == "2026-11-20" and rep[0][1] > 0 and rep[0][2] > 0)
check("a file older than the 62 days is removed", not os.path.exists(old))
back = pd.read_csv(os.path.join(work, "2026-11-20.csv.gz"), compression="gzip")
check("the day file reads back with the same columns", list(back.columns) == ps.PRODUCT_COLS, str(list(back.columns)))
work2 = tempfile.mkdtemp()
ps.write_days(agg, work2, keep_days=62, skip_days={"2026-11-20"})
check("a day with a failed branch-day is not written", os.listdir(work2) == [], str(os.listdir(work2)))
print(f"{PASS} passed, {FAIL} failed")
sys.exit(1 if FAIL else 0)
