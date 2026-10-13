#!/usr/bin/env python3
"""
Read the Swiggy, Zomato and Google rating sheets and write data/ratings.csv.

Needs GOOGLE_CREDENTIALS (service account JSON, same secret as rista_live.py).
The three sheets are shared 'anyone with the link'; the service account can read them.

Output columns: Date (YYYY-MM-DD), Platform, Store Name, Brand Name, Store Type,
Region, Orders, Rated Orders, Avg Rating. build_site.py turns this into the ratings file.

Usage: python scripts/sync_ratings.py --out data/ratings.csv
"""
import argparse
import json
import os
import sys

import pandas as pd

SHEETS = {
    "Swiggy": {"id": "1R5kdiLGiNV2zSxs2hyObxUF216iqeaDVTggM7mub0Ck", "tab": "Raw Data", "header_row": 2},
    "Zomato": {"id": "1V-tFd3I9CRxDUWSrU9Zc6ODW9Qhbs4C0SXZq4mW6tsg", "tab": "Raw Data", "header_row": 2},
    "Google": {"id": "1HlYNDNXig3PCjIFpQdI8B1lEzubsttUEPj_57DgpJoE", "tab": "Complaints", "header_row": 1},
}
# The sheets display dates month-first: 10-11-2026 is 11 October 2026
# (Week 41 and Month "October-26" on the same row confirm it).
DATE_FORMATS = ["%m-%d-%Y", "%Y-%m-%d"]


def parse_dates(series):
    out = pd.Series(pd.NaT, index=series.index)
    for fmt in DATE_FORMATS:
        todo = out.isna()
        out[todo] = pd.to_datetime(series[todo], format=fmt, errors="coerce")
    return out


def num(series):
    return pd.to_numeric(series.astype(str).str.replace(",", "").str.strip(), errors="coerce")


def unique_headers(head):
    """Some sheets have two columns with the same header. Rename repeats (the first one is used)."""
    seen, out = {}, []
    for h in head:
        if h in seen:
            seen[h] += 1
            new = f"{h} ({seen[h]})"
            print(f"Warning: duplicate column '{h}' renamed to '{new}'. The first one is used.")
            out.append(new)
        else:
            seen[h] = 1
            out.append(h)
    return out


def parse_platform(platform, values, header_row):
    """values = the sheet as a list of rows (get_all_values output)."""
    head = unique_headers([h.strip() for h in values[header_row - 1]])
    df = pd.DataFrame(values[header_row:], columns=head)
    if platform == "Google":
        out = pd.DataFrame({
            "Date": parse_dates(df["Date"]),
            "Platform": "Google",
            "Store Name": df["Store Name"].str.strip(),
            "Brand Name": df["Location Name"].str.strip(),
            "Store Type": df["Store Type"].str.strip(),
            "Region": df["Region"].str.strip(),
            "Orders": 0,
            "Rated Orders": 1,
            "Avg Rating": num(df["Rating"]),
        })
    else:
        out = pd.DataFrame({
            "Date": parse_dates(df["Date"]),
            "Platform": platform,
            "Store Name": df["Store Name"].str.strip(),
            "Brand Name": df["Brand Name"].str.strip(),
            "Store Type": df["Store Type"].str.strip(),
            "Region": df["Region"].str.strip(),
            "Orders": num(df["Orders Count"]),
            "Rated Orders": num(df["Rated Orders"]),
            "Avg Rating": num(df["Avg. Rating"]),
        })
    out = out.dropna(subset=["Date", "Store Name"])
    out["Orders"] = out["Orders"].fillna(0)
    out["Rated Orders"] = out["Rated Orders"].fillna(0)
    out["Avg Rating"] = out["Avg Rating"].fillna(0)
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
