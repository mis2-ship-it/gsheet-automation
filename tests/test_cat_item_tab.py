"""Checks the product detail tab (Disc_Cat_Item) in write_discount_sheet.py. Run from the repo root:
    python tests/test_cat_item_tab.py
"""
import os, sys
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
import pandas as pd
import write_discount_sheet as w

rows = []
items = [f"Item {i:02d}" for i in range(1, 13)]
for m, d in [("2026-07", "2026-07-10"), ("2026-08", "2026-08-10"), ("2026-09", "2026-09-10"),
              ("2026-10", "2026-10-10"), ("2026-11", "2026-11-10")]:
    for i, it in enumerate(items, 1):
        rows.append(dict(day=d, brand="Madno", region="KA", source="Swiggy", category="Thickshakes", item=it,
                         invoiceNumber=f"{m}-{it}", qty=1, gross=i * 100 + 10, disc=10, net=i * 100))
    rows.append(dict(day=d, brand="Madno", region="KA", source="In Store", category="Thickshakes", item="Item 12",
                     invoiceNumber=f"{m}-off", qty=1, gross=60, disc=10, net=50))
    rows.append(dict(day=d, brand="Madno", region="KA", source="Swiggy", category="Charges", item=w.CHARGE_ITEM,
                     invoiceNumber=f"{m}-chg", qty=1, gross=40, disc=0, net=40))
df = pd.DataFrame(rows)
tab = w.build_cat_item_tab(df)
PASS = FAIL = 0


def check(name, cond):
    global PASS, FAIL
    PASS, FAIL = (PASS + 1, FAIL) if cond else (PASS, FAIL + 1)
    print(("PASS " if cond else "FAIL ") + name)


check("the columns are the ones the page reads", list(tab.columns) == w.CAT_ITEM_COLS)
months = sorted(tab["Date"].str[:7].unique())
check("only the last four months are kept (July is left out)", months == ["2026-08", "2026-09", "2026-10", "2026-11"])
nov = tab[tab["Date"].str[:7] == "2026-11"]
named = sorted(set(nov["Item"]) - {w.OTHER_ITEMS})
check("10 items are named, the rest are grouped as (other items)", len(named) == 10 and w.OTHER_ITEMS in set(nov["Item"]))
check("Item 12 is named and Item 01 is grouped", "Item 12" in named and "Item 01" not in named)
kept = df[(df["day"].str[:7].isin(months)) & (df["category"] != "Charges")]["net"].sum()
check("the net sales add up to the product rows (charges left out)", abs(tab["Net"].sum() - kept) < 0.01)
check("both channels are kept", set(tab["Channel"]) == {"Offline", "Online"})
check("charges are not in the tab", not (tab["Category"] == "Charges").any())
print(f"{PASS} passed, {FAIL} failed")
sys.exit(1 if FAIL else 0)
