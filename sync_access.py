#!/usr/bin/env python3
"""
Read the User Access tab of POS_Master_Dashboard and write data/access.csv.

One row per store per email: Store Name, Email, Role (AM or TM).
A cell may hold more than one email (comma, space or semicolon separated). Emails are lower-cased.
A login sees the stores where its email is listed in AM Mail or TM Mail (see build_groups.py).

Needs GOOGLE_CREDENTIALS. The POS_Master_Dashboard sheet must be shared with the service account.
Usage: python sync_access.py --out data/access.csv
"""
import argparse
import json
import os
import re
import sys

import pandas as pd

SHEET_ID = "1g4vuRZPy7qsUvDzF5yYM60VKWTL2r0VSDvtvNl06hiY"
TAB = "User Access"
EMAIL = re.compile(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}")


def norm(text):
    return re.sub(r"[^a-z0-9]+", " ", str(text).lower()).strip()


def clean_name(value):
    return " ".join(str(value).split())


def find(head, name):
    normed = [norm(h) for h in head]
    if norm(name) in normed:
        return head[normed.index(norm(name))]
    raise KeyError(f"No column '{name}' in the {TAB} tab. Columns found: {[h for h in head if h]}")


def parse(values):
    head = [h.strip() for h in values[0]]
    rows = [(list(r) + [""] * len(head))[:len(head)] for r in values[1:]]
    df = pd.DataFrame(rows, columns=head)
    store_col, am_col, tm_col = find(head, "Store Name"), find(head, "AM Mail"), find(head, "TM Mail")
    out = []
    for _, r in df.iterrows():
        store = clean_name(r[store_col])
        if not store:
            continue
        for role, col in [("AM", am_col), ("TM", tm_col)]:
            for email in sorted(set(e.lower() for e in EMAIL.findall(str(r[col])))):
                out.append((store, email, role))
    return pd.DataFrame(out, columns=["Store Name", "Email", "Role"]).drop_duplicates()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default="data/access.csv")
    args = ap.parse_args()

    import gspread
    from google.oauth2.service_account import Credentials
    creds = Credentials.from_service_account_info(
        json.loads(os.environ["GOOGLE_CREDENTIALS"]),
        scopes=["https://www.googleapis.com/auth/spreadsheets.readonly"])
    values = gspread.authorize(creds).open_by_key(SHEET_ID).worksheet(TAB).get_all_values()
    out = parse(values)
    if out.empty:
        sys.exit(f"The {TAB} tab gave no store and email rows. Check the tab and its sharing.")
    os.makedirs(os.path.dirname(args.out) or ".", exist_ok=True)
    out.to_csv(args.out, index=False)
    print(f"wrote {args.out}: {out['Store Name'].nunique()} stores, {out['Email'].nunique()} emails")


if __name__ == "__main__":
    main()
