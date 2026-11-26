"""Keeps the cancelled orders in one file for the dashboard: data/cancel_orders.csv.gz.

Each Cancel run writes data/cancel/cancel_invoices_<from>_<to>.csv. This adds those rows to the stored file. A row with
the same Invoice No, Branch and Date replaces the stored row, so a run can be repeated without duplicates.

Usage: python merge_cancel.py --new data/cancel/cancel_invoices_2025-04-01_2025-04-30.csv --master data/cancel_orders.csv.gz
"""
import argparse
import os

import pandas as pd

KEY = ["Invoice No", "Branch", "Date"]


def read_table(path):
    """A CSV (gzipped or not) as text. A missing or empty file gives an empty table."""
    if not path or not os.path.exists(path) or os.path.getsize(path) == 0:
        return pd.DataFrame()
    return pd.read_csv(path, dtype=str, keep_default_na=False)


def merge(master, new):
    """The stored rows with the new rows added. A new row replaces a stored row with the same key."""
    if new.empty:
        return master
    combined = new if master.empty else pd.concat([master, new], ignore_index=True)
    combined = combined.drop_duplicates(subset=KEY, keep="last")
    return combined.sort_values(["Date", "Branch", "Invoice No"]).reset_index(drop=True)


def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("--new", required=True, help="the invoices file of one Cancel run")
    ap.add_argument("--master", default="data/cancel_orders.csv.gz", help="the stored file")
    a = ap.parse_args(argv)
    master, new = read_table(a.master), read_table(a.new)
    if new.empty:
        print(f"no new cancelled orders in {a.new}; {a.master} unchanged")
        return 0
    before = len(master)
    out = merge(master, new)
    os.makedirs(os.path.dirname(a.master) or ".", exist_ok=True)
    out.to_csv(a.master, index=False, compression="gzip")
    print(f"cancel orders stored: {len(out)} (before {before}, from this run {len(new)}) -> {a.master}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
