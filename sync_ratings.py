#!/usr/bin/env python3
"""
Read the Swiggy, Zomato and Google rating sheets and write data/ratings.csv.

Needs GOOGLE_CREDENTIALS (service account JSON, same secret as rista_live.py).
The three sheets are shared with the service account (Viewer or Editor).

Columns are found by name. Matching ignores case and punctuation. An exact name is tried
first, then a name the header contains. The log prints the column chosen for each field,
so a wrong pick is easy to spot.

Output columns: Date (YYYY-MM-DD), Platform, Store Name, Brand Name, Store Type, Region,
Orders (Orders Count), Rated Orders (1 when the row has a rating, else 0), Avg Rating,
Comments Group (the issue, blank when there is none), Item Name, Category Name and Review Text
(the customer comment). build_site.py turns this into the ratings file.

Usage: python sync_ratings.py --out data/ratings.csv
"""
import argparse
import json
import os
import re
import sys

import pandas as pd

SHEETS = {
    "Swiggy": {"id": "1R5kdiLGiNV2zSxs2hyObxUF216iqeaDVTggM7mub0Ck", "tab": "Raw Data", "header_row": 2},
    "Zomato": {"id": "1V-tFd3I9CRxDUWSrU9Zc6ODW9Qhbs4C0SXZq4mW6tsg", "tab": "Raw Data", "header_row": 2},
    "Google": {"id": "1HlYNDNXig3PCjIFpQdI8B1lEzubsttUEPj_57DgpJoE", "tab": "Complaints", "header_row": 1},
}
# Swiggy and Zomato show dates month-first: 10-11-2026 is 11 October 2026
# (Week 41 and Month "October-26" on the same row confirm it).
DATE_FORMATS = ["%m-%d-%Y", "%Y-%m-%d"]
# Google's Date column is day-first: 11-10-2026 is 11 October 2026 (Month "Oct-26", Week 41).
GOOGLE_DATE_FORMATS = ["%d-%m-%Y", "%Y-%m-%d"]

# field: (exact names tried first, words the header may contain)
FIELD_NAMES = {
    "date": (["date"], ["date"]),
    "store": (["store name"], ["store name"]),
    "brand": (["brand name", "location name"], ["brand", "location"]),
    "store_type": (["store type"], ["store type"]),
    "region": (["region"], ["region"]),
    "orders": (["orders count"], ["orders count"]),
    "rated": (["rated orders"], ["rated order", "rated"]),
    "rating": (["avg rating", "rating"], ["avg rating", "rating"]),
    "comments": (["comments group", "group comments"], ["comments group", "group comments"]),
    "item": (["item name"], ["item name"]),
    "category": (["category name", "category"], ["category name"]),
    "review": (["customer comments", "comments", "review"], ["customer comments", "review"]),
}
GOOGLE_OPTIONAL = {"orders", "rated", "comments"}   # Google reviews: one rating per row, no order count or comment group
OPTIONAL_ALL = {"comments", "item", "category", "review"}


def norm(text):
    return re.sub(r"[^a-z0-9]+", " ", str(text).lower()).strip()


def clean_name(value):
    """Collapse repeated spaces so 'Hyderabad  Warehouse' matches 'Hyderabad Warehouse'."""
    return " ".join(str(value).split())


def find_column(head, field, required=True):
    exact, words = FIELD_NAMES[field]
    normed = [norm(h) for h in head]
    for name in exact:
        if name in normed:
            return head[normed.index(name)]
    for word in words:
        for i, h in enumerate(normed):
            if h and word in h:
                return head[i]
    if required:
        names = [h for h in head if h]
        raise KeyError(f"No column found for '{field}'. Column names in this sheet: {names}")
    return None


def parse_dates(text_series, formats):
    out = pd.Series(pd.NaT, index=text_series.index)
    for fmt in formats:
        todo = out.isna()
        out[todo] = pd.to_datetime(text_series[todo], format=fmt, errors="coerce")
    return out


def num(series):
    return pd.to_numeric(series.astype(str).str.replace(",", "").str.strip(), errors="coerce")


def unique_headers(head):
    """Sheets can repeat a header or leave it blank. Rename repeats so each name is unique."""
    seen, out = {}, []
    for h in head:
        if h in seen:
            seen[h] += 1
            out.append(f"{h} ({seen[h]})")
        else:
            seen[h] = 1
            out.append(h)
    return out


def parse_platform(platform, values, header_row):
    """values = the sheet as a list of rows (get_all_values output)."""
    head = unique_headers([h.strip() for h in values[header_row - 1]])
    width = len(head)
    rows = [(list(r) + [""] * width)[:width] for r in values[header_row:]]
    df = pd.DataFrame(rows, columns=head)

    cols = {}
    for field in FIELD_NAMES:
        optional = field in OPTIONAL_ALL or (platform == "Google" and field in GOOGLE_OPTIONAL)
        cols[field] = find_column(head, field, required=not optional)
    print(f"{platform} columns used: " + ", ".join(f"{f} = '{c}'" for f, c in cols.items() if c))

    def text(field):
        return df[cols[field]].astype(str).map(clean_name)

    comments = text("comments") if cols["comments"] else pd.Series("", index=df.index)
    items = text("item") if cols["item"] else pd.Series("", index=df.index)
    categories = text("category") if cols["category"] else pd.Series("", index=df.index)
    reviews = df[cols["review"]].astype(str).map(clean_name) if cols["review"] else pd.Series("", index=df.index)
    date_text = df[cols["date"]].astype(str).str.strip()
    formats = GOOGLE_DATE_FORMATS if platform == "Google" else DATE_FORMATS
    dates = parse_dates(date_text, formats)
    unreadable = (date_text != "") & dates.isna()
    if unreadable.any():
        examples = sorted(set(date_text[unreadable]))[:5]
        print(f"Warning: {platform}: {int(unreadable.sum())} row(s) have a date that could not be read "
              f"and were left out. Examples: {examples}")

    if platform == "Google":
        out = pd.DataFrame({
            "Date": dates,
            "Platform": "Google",
            "Store Name": text("store"),
            "Brand Name": text("brand"),
            "Store Type": text("store_type"),
            "Region": text("region"),
            "Orders": 1,
            "Rated Orders": 1,
            "Avg Rating": num(df[cols["rating"]]),
            "Comments Group": comments,
            "Item Name": items,
            "Category Name": categories,
            "Review Text": reviews,
        })
    else:
        out = pd.DataFrame({
            "Date": dates,
            "Platform": platform,
            "Store Name": text("store"),
            "Brand Name": text("brand"),
            "Store Type": text("store_type"),
            "Region": text("region"),
            "Orders": num(df[cols["orders"]]),
            "Rated Orders": num(df[cols["rated"]]),
            "Avg Rating": num(df[cols["rating"]]),
            "Comments Group": comments,
            "Item Name": items,
            "Category Name": categories,
            "Review Text": reviews,
        })
    out = out.dropna(subset=["Date", "Store Name"])
    out = out[out["Store Name"] != ""]
    out["Orders"] = out["Orders"].fillna(0)
    out["Rated Orders"] = out["Rated Orders"].fillna(0)
    out["Avg Rating"] = out["Avg Rating"].fillna(0)
    out["Comments Group"] = out["Comments Group"].fillna("").astype(str)
    for c in ["Item Name", "Category Name", "Review Text"]:
        out[c] = out[c].fillna("").astype(str)
    out["Date"] = out["Date"].dt.strftime("%Y-%m-%d")
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default="data/ratings.csv")
    args = ap.parse_args()

    import gspread
    from google.oauth2.service_account import Credentials
    creds = Credentials.from_service_account_info(
        json.loads(os.environ["GOOGLE_CREDENTIALS"]),
        scopes=["https://www.googleapis.com/auth/spreadsheets.readonly"])
    gc = gspread.authorize(creds)

    frames = []
    for platform, cfg in SHEETS.items():
        ws = gc.open_by_key(cfg["id"]).worksheet(cfg["tab"])
        values = ws.get_all_values()
        part = parse_platform(platform, values, cfg["header_row"])
        print(f"{platform}: {len(part)} rows")
        frames.append(part)

    out = pd.concat(frames, ignore_index=True)
    os.makedirs(os.path.dirname(args.out) or ".", exist_ok=True)
    out.to_csv(args.out, index=False)
    print(f"wrote {args.out} with {len(out)} rows")


if __name__ == "__main__":
    sys.exit(main())
