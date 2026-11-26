"""The Sales Page layout: status and reason inside statusInfo. Run from the repo root: python tests/test_cancel_nested.py"""
import os, sys
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
import cancel_sales as cs

PASS = FAIL = 0


def check(name, cond, extra=""):
    global PASS, FAIL
    PASS, FAIL = (PASS + 1, FAIL) if cond else (PASS, FAIL + 1)
    print(("PASS " if cond else "FAIL ") + name + (f"  [{extra}]" if extra else ""))


base = {"invoiceDate": "2026-11-20T13:00:00", "branchName": "Madno Koramangala", "channel": "Swiggy",
        "netAmount": 100, "chargeAmount": 0, "grossAmount": 110, "discountAmount": 10}
raw = [
    # 1. a closed order: not cancelled
    {**base, "invoiceNumber": "C1", "status": "Closed", "statusInfo": {"status": "Closed", "reason": None,
                                                                      "sourceReason": None, "remarks": None}},
    # 2. Closed at the top, Cancelled inside statusInfo; the reason is in the source reason (out of stock)
    {**base, "invoiceNumber": "C2", "status": "Closed", "statusInfo": {
        "status": "Cancelled", "reason": "Cancelled by swiggy. Accepted a minute ago. Cancelled before pickup",
        "sourceReason": "as the ordered item(s) are now out of stock", "remarks": ""}},
    # 3. Voided at the top; a reason with no listed keyword: Others
    {**base, "invoiceNumber": "C3", "status": "Voided", "statusInfo": {
        "reason": "Cancelled by swiggy. Accepted a minute ago", "sourceReason": "", "remarks": ""}},
    # 4. Cancelled inside statusInfo; outlet closed
    {**base, "invoiceNumber": "C4", "status": "Closed", "statusInfo": {
        "status": "Cancelled", "reason": "Outlet closed early", "sourceReason": "", "remarks": ""}},
    # 5. Cancelled at the top with no reason: Others, blank
    {**base, "invoiceNumber": "C5", "status": "Cancelled"},
]
ROOT_REASONS = os.path.join(ROOT, "data", "cancel_reasons.csv")
reasons = cs.load_reasons(ROOT_REASONS)
maps = ({"Madno Koramangala": "COCO"}, {"Madno Koramangala": "KA"}, {"Swiggy": "Swiggy"}, {"Swiggy": "Frozen Bottle"})
rows, status_count, reason_count = cs.build_rows(raw, reasons, maps, "auto", "auto", ("cancel", "void"))
ids = [r["Invoice No"] for r in rows]
check("four cancelled orders are found (the closed one is not)", ids == ["C2", "C3", "C4", "C5"], str(ids))
by = {r["Invoice No"]: r for r in rows}
check("a cancelled order whose reason is in the source reason is classified (out of stock = Item OOS)",
      by["C2"]["Bucket"] == "Item OOS" and by["C2"]["Listed"] == "Yes", by["C2"]["Bucket"])
check("the source reason is kept on the row", by["C2"]["Source Reason"].startswith("as the ordered item(s) are now out of stock"))
check("a Voided order is cancelled too", by["C3"]["Bucket"] == "Others" and by["C3"]["Listed"] == "No")
check("outlet closed is Closed in hours", by["C4"]["Bucket"] == "Closed in hours", by["C4"]["Bucket"])
check("a blank reason is Others, shown as (blank)", by["C5"]["Bucket"] == "Others" and by["C5"]["Reason"] == "(blank)")
check("the status shows the fields it came from", by["C2"]["Status"] == "status=Closed | statusInfo.status=Cancelled", by["C2"]["Status"])
check("every order is counted in the status totals", sum(status_count.values()) == 5, str(status_count))
check("a flat layout (reason at the top) still works", cs.reason_parts({"reason": "out of stock"})[0] == "out of stock")
check("a field named cancelReason is found by its name", cs.reason_parts({"cancelReason": "merchant device"})[0] == "merchant device")
check("a fulfillment status of Cancelled counts as cancelled",
      cs.is_cancelled({"status": "Closed", "fulfillmentStatus": "Cancelled"}, "auto", ("cancel", "void")))
check("a Closed order with a Delivered fulfillment status is not cancelled",
      not cs.is_cancelled({"status": "Closed", "fulfillmentStatus": "Delivered"}, "auto", ("cancel", "void")))
check("a Closed order whose reason text says Cancelled is not cancelled (reason text is not a status)",
      not cs.is_cancelled({"status": "Closed", "statusInfo.reason": "Cancelled by swiggy", "statusInfo.status": "Closed"},
                          "auto", ("cancel", "void")))
print(f"{PASS} passed, {FAIL} failed")
sys.exit(1 if FAIL else 0)
