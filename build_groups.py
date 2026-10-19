#!/usr/bin/env python3
"""
Build one data set per login, so each user receives only their own stores.

Who sees what:
  - An email listed in the User Access tab (AM Mail or TM Mail) sees only the stores it is listed against.
  - Any other @frozenbottle.in login sees all stores.
  - Anyone else is refused.

Output:
  site/_g/<random token>/          one full data set per group (manifest.json, rollup, stores, ...)
  functions/_access.json           email -> token. Only the Pages Function in functions/data/[[path]].js reads it.
Tokens are random and are never written into a public file, so one login cannot guess another group's folder.

Usage (the same data flags as build_site.py, plus --access):
  python build_groups.py --access data/access.csv --history historical_data/historical_sales.csv.gz \
      --new data/new_sales.csv.gz --new-hourly data/new_hourly.csv.gz --live data/live_sales.csv --ratings data/ratings.csv
"""
import argparse
import json
import os
import secrets
import shutil
import subprocess
import sys
import tempfile

import pandas as pd

DOMAIN = "frozenbottle.in"


def clean(text):
    return " ".join(str(text).split())


def branches_with_data(paths):
    names = set()
    for p in paths:
        if p and os.path.exists(p) and os.path.getsize(p) > 0:
            df = pd.read_csv(p, compression="infer", usecols=["Branch"], low_memory=False)
            names |= set(df["Branch"].dropna().astype(str).map(clean))
    return names


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--access", required=True)
    ap.add_argument("--history", required=True)
    ap.add_argument("--new")
    ap.add_argument("--new-hourly")
    ap.add_argument("--live")
    ap.add_argument("--ratings")
    ap.add_argument("--kpt")
    ap.add_argument("--site", default="site")
    ap.add_argument("--functions", default="functions")
    args = ap.parse_args()

    if not os.path.exists(args.access) or os.path.getsize(args.access) == 0:
        sys.exit("data/access.csv is missing or empty. Stopping: a missing list would open every store to every login.")
    acc = pd.read_csv(args.access, dtype=str).fillna("")
    acc["Store Name"] = acc["Store Name"].map(clean)
    acc["Email"] = acc["Email"].str.strip().str.lower()
    acc = acc[(acc["Store Name"] != "") & acc["Email"].str.contains("@")]

    data_branches = branches_with_data([args.history, args.new, args.new_hourly, args.live])
    all_token = secrets.token_hex(16)
    groups = [(all_token, None, "all stores")]
    emails = {}
    for email, grp in acc.groupby("Email"):
        listed = sorted(set(grp["Store Name"]))
        have = sorted(set(listed) & data_branches)
        missing = sorted(set(listed) - data_branches)
        if missing:
            print(f"WARNING {email}: these store names are not in the sales data (check spelling): {missing}")
        if not have:
            emails[email] = None
            print(f"{email}: no stores with sales data, this login will be refused")
            continue
        token = secrets.token_hex(16)
        emails[email] = token
        groups.append((token, have, f"{len(have)} stores for {email}"))

    shutil.rmtree(os.path.join(args.site, "_g"), ignore_errors=True)
    shutil.rmtree(os.path.join(args.site, "data"), ignore_errors=True)   # old public data folder: must not be deployed
    os.makedirs(os.path.join(args.site, "_g"), exist_ok=True)
    os.makedirs(args.functions, exist_ok=True)
    with open(os.path.join(args.functions, "_access.json"), "w", encoding="utf-8") as f:
        json.dump({"domain": DOMAIN, "all": all_token, "emails": emails}, f, indent=1)

    tmp = tempfile.mkdtemp()
    for i, (token, stores, label) in enumerate(groups):
        out = os.path.join(args.site, "_g", token)
        cmd = [sys.executable, "build_site.py", "--history", args.history, "--out", out, "--icons-dir", args.site]
        for flag, value in [("--new", args.new), ("--new-hourly", args.new_hourly),
                            ("--live", args.live), ("--ratings", args.ratings), ("--kpt", args.kpt)]:
            if value:
                cmd += [flag, value]
        if stores is not None:
            lst = os.path.join(tmp, f"stores_{i}.json")
            with open(lst, "w", encoding="utf-8") as f:
                json.dump(stores, f)
            cmd += ["--only-stores", lst]
        print(f"building {label}")
        subprocess.run(cmd, check=True)
    refused = sum(1 for t in emails.values() if t is None)
    print(f"done: {len(groups)} data sets; {len(emails) - refused} logins with stores; {refused} refused")


if __name__ == "__main__":
    main()
