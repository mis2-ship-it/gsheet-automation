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


def metrics_for_group(path_in, path_out, stores):
    """Writes one login's metrics file. stores=None (all stores): the file is copied as it is. Otherwise only the rows
    of that login's stores are kept, and the store list in the file is cut to those stores too."""
    import gzip
    with gzip.open(path_in, "rt", encoding="utf-8") as f:
        d = json.load(f)
    rows, names = d.get("rows", []), d.get("stores")
    if stores is None:
        kept, out_stores = rows, names
    elif names is None:
        kept, out_stores = [], []          # an old file has no store names: a store-limited login sees none of it
    else:
        allowed = set(stores)
        src = [r for r in rows if len(r) > 10 and r[10] is not None and 0 <= r[10] < len(names)
               and clean(names[r[10]]) in allowed]
        used = sorted({names[r[10]] for r in src})
        pos = {n: i for i, n in enumerate(used)}
        kept = [r[:10] + [pos[names[r[10]]]] for r in src]
        out_stores = used
    out = dict(d)
    out["rows"] = kept
    if names is not None:
        out["stores"] = out_stores
    os.makedirs(os.path.dirname(path_out) or ".", exist_ok=True)
    with gzip.open(path_out, "wt", encoding="utf-8") as f:
        json.dump(out, f, separators=(",", ":"))
    label = "all stores" if stores is None else f"{len(out_stores)} stores"
    print(f"metrics for {label}: {len(kept)} rows -> {path_out}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--access", required=True)
    ap.add_argument("--history", required=True)
    ap.add_argument("--new")
    ap.add_argument("--new-hourly")
    ap.add_argument("--live")
    ap.add_argument("--ratings")
    ap.add_argument("--kpt")
    ap.add_argument("--metrics", help="data/metrics.json.gz: cut to each login's stores")
    ap.add_argument("--product-mix", help="data/product_mix.json.gz: given to the all-stores login only")
    ap.add_argument("--discounts")
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
        cmd += ["--access", args.access]
        for flag, value in [("--new", args.new), ("--new-hourly", args.new_hourly),
                            ("--live", args.live), ("--ratings", args.ratings), ("--kpt", args.kpt), ("--discounts", args.discounts)]:
            if value:
                cmd += [flag, value]
        if stores is not None:
            lst = os.path.join(tmp, f"stores_{i}.json")
            with open(lst, "w", encoding="utf-8") as f:
                json.dump(stores, f)
            cmd += ["--only-stores", lst]
        print(f"building {label}")
        subprocess.run(cmd, check=True)
        if args.metrics and os.path.exists(args.metrics):
            metrics_for_group(args.metrics, os.path.join(out, "metrics.json.gz"), stores)
        if args.product_mix and os.path.exists(args.product_mix) and stores is None:
            os.makedirs(out, exist_ok=True)
            shutil.copyfile(args.product_mix, os.path.join(out, "product_mix.json.gz"))
            print("product mix: all-stores login only")
    refused = sum(1 for t in emails.values() if t is None)
    print(f"done: {len(groups)} data sets; {len(emails) - refused} logins with stores; {refused} refused")


if __name__ == "__main__":
    main()
