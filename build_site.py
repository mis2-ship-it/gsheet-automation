#!/usr/bin/env python3
"""
Build the data files for the Frozen Bottle sales and ratings dashboard.

Inputs (CSV files, optional except --history):
  --history  historical sales file (same columns as historical_sales.csv.gz)
  --new      completed days after the history (written by sync_sales.py)
  --live     today's business day so far (written by sync_sales.py)
  --ratings  Swiggy / Zomato / Google review rows (written by sync_ratings.py)

Outputs (in --out, normally site/data):
  manifest.json          dimensions, store list, last day, flags
  rollup.json.gz         all stores together (store column is 0)
  stores/<i>.json.gz     one file per store, used when a store is selected
  live.json              today's rows (only when --live is given)
  ratings.json.gz        ratings rows (only when --ratings is given)

Sales row:   [day, store, brand, store_type, region, source, session, net_paise, gross_paise, discount_paise, orders]
Ratings row: [day, store, brand, store_type, region, platform, rated_orders, rating_sum_x100, orders]
Day 0 is 2025-01-01.
"""
import argparse
import gzip
import json
import os
from datetime import datetime, timezone

import numpy as np
import pandas as pd

BASE = pd.Timestamp("2025-01-01")
BRAND_ORDER = ["Frozen Bottle", "Madno", "Boba Bar", "Lubov"]
SOURCE_ORDER = ["In Store", "Swiggy", "Zomato", "Others", "Ownly"]
SESSION_ORDER = ["Breakfast", "Lunch", "Snacks", "Dinner", "Post Dinner", "Not tagged"]
# Late Night and Closing in the history are the same session as Post Dinner in the live script.
SESSION_ALIASES = {"Late Night": "Post Dinner", "Closing": "Post Dinner"}
TYPE_ORDER = ["COCO", "FOFO", "Unmapped"]
PLATFORMS = ["Swiggy", "Zomato", "Google"]


def ordered(present, preferred):
    """Preferred order first, then any unexpected values alphabetically."""
    present = set(present)
    return [p for p in preferred if p in present] + sorted(v for v in present if v not in preferred)


def clean_sales(df):
    df = df.copy()
    df["Date"] = pd.to_datetime(df["Date"])
    df["Branch"] = df["Branch"].astype(str).str.strip()
    df["Brand Name"] = df["Brand Name"].astype(str).str.strip()
    df = df.dropna(subset=["Brand Name", "Source"])
    df = df[df["Brand Name"].isin(BRAND_ORDER)]
    df["Source"] = df["Source"].astype(str).str.strip()
    df["Session"] = df["Session"].fillna("Not tagged").astype(str).str.strip()
    df["Session"] = df["Session"].replace(SESSION_ALIASES)
    df["Store Type"] = df["Store Type"].fillna("Unmapped").astype(str).str.strip()
    df["Region"] = df["Region"].fillna("Unmapped").astype(str).str.strip()
    for c in ["Net Sales", "Gross Sales", "Discount", "Orders"]:
        df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0)
    return df


def make_icons(site_dir):
    """Draw the home-screen icons, so only text files need uploading. Needs Pillow."""
    try:
        from PIL import Image, ImageDraw
    except ImportError:
        print("Pillow not installed: skipping app icons")
        return
    teal = (45, 106, 110)
    for name, size in [("icon-192.png", 192), ("icon-512.png", 512), ("apple-touch-icon.png", 180)]:
        img = Image.new("RGB", (size, size), teal)
        draw = ImageDraw.Draw(img)
        left, right, base = 0.27 * size, 0.73 * size, 0.70 * size
        w = (right - left) / 5
        bars = zip([0.20, 0.32, 0.44], [(191, 227, 224), (255, 255, 255), (191, 227, 224)])
        for i, (h, colour) in enumerate(bars):
            x0 = left + i * (w * 1.67)
            x1 = x0 + w * 1.2
            top = base - h * size * 1.05
            draw.rounded_rectangle([x0, top, x1, base], radius=int(size * 0.03), fill=colour)
        img.save(os.path.join(site_dir, name), "PNG")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--history", required=True)
    ap.add_argument("--new")
    ap.add_argument("--live")
    ap.add_argument("--ratings")
    ap.add_argument("--out", default="site/data")
    args = ap.parse_args()

    os.makedirs(os.path.join(args.out, "stores"), exist_ok=True)
    hist = pd.read_csv(args.history, compression="infer", low_memory=False)
    hist = clean_sales(hist)
    dropped_history = None

    sales = hist
    if args.new:
        new = clean_sales(pd.read_csv(args.new, compression="infer", low_memory=False))
        cut = new["Date"].min()
        sales = pd.concat([hist[hist["Date"] < cut], new], ignore_index=True)

    live = None
    if args.live and os.path.exists(args.live) and os.path.getsize(args.live) > 0:
        live = clean_sales(pd.read_csv(args.live, low_memory=False))
        if live.empty:
            live = None

    ratings = None
    if args.ratings and os.path.exists(args.ratings) and os.path.getsize(args.ratings) > 0:
        ratings = pd.read_csv(args.ratings, low_memory=False)
        ratings["Date"] = pd.to_datetime(ratings["Date"])
        ratings["Store Name"] = ratings["Store Name"].astype(str).str.strip()
        ratings["Brand Name"] = ratings["Brand Name"].astype(str).str.strip()
        ratings["Store Type"] = ratings["Store Type"].fillna("Unmapped").astype(str).str.strip()
        ratings["Region"] = ratings["Region"].fillna("Unmapped").astype(str).str.strip()
        ratings = ratings[ratings["Brand Name"].isin(BRAND_ORDER)]

    # Store master: the latest real store type and region seen for each store.
    sales["Store"] = sales["Branch"]
    known = sales[sales["Store Type"] != "Unmapped"].sort_values("Date").groupby("Store").last()
    store_attr = {}
    for name in set(sales["Store"]) | (set(live["Branch"]) if live is not None else set()) \
            | (set(ratings["Store Name"]) if ratings is not None else set()):
        if name in known.index:
            store_attr[name] = (known.loc[name, "Store Type"], known.loc[name, "Region"])
        else:
            store_attr[name] = ("Unmapped", "Unmapped")
    stores = sorted(store_attr)
    sidx = {s: i for i, s in enumerate(stores)}

    brands = ordered(list(sales["Brand Name"]) + (list(ratings["Brand Name"]) if ratings is not None else []), BRAND_ORDER)
    bidx = {b: i for i, b in enumerate(brands)}
    types = ordered(
        [a[0] for a in store_attr.values()] + list(sales["Store Type"]),
        TYPE_ORDER)
    tidx = {t: i for i, t in enumerate(types)}
    regions = ordered([a[1] for a in store_attr.values()] + list(sales["Region"]), ["KA", "TN", "MH", "ROI", "AP & TL", "Kerela", "Unmapped"])
    ridx = {r: i for i, r in enumerate(regions)}
    sources = ordered(sales["Source"], SOURCE_ORDER)
    soidx = {s: i for i, s in enumerate(sources)}
    sessions = ordered(sales["Session"], SESSION_ORDER)
    sesidx = {s: i for i, s in enumerate(sessions)}

    def sales_rows(df, with_store):
        t = pd.DataFrame({
            "d": (df["Date"] - BASE).dt.days,
            "st": df["Store"].map(sidx) if with_store else 0,
            "b": df["Brand Name"].map(bidx),
            "t": df["Store Type"].map(tidx),
            "r": df["Region"].map(ridx),
            "s": df["Source"].map(soidx),
            "ses": df["Session"].map(sesidx),
            "net": (df["Net Sales"] * 100).round().astype("int64"),
            "gross": (df["Gross Sales"] * 100).round().astype("int64"),
            "disc": (df["Discount"] * 100).round().astype("int64"),
            "orders": df["Orders"].round().astype("int64"),
        })
        keys = ["d", "st", "b", "t", "r", "s", "ses"] if with_store else ["d", "b", "t", "r", "s", "ses"]
        t = t.groupby(keys, as_index=False).sum()
        if not with_store:
            t.insert(1, "st", 0)
        return t[["d", "st", "b", "t", "r", "s", "ses", "net", "gross", "disc", "orders"]]

    def dump(path, obj):
        raw = json.dumps(obj, separators=(",", ":")).encode("utf-8")
        with gzip.open(path, "wb", compresslevel=9) as f:
            f.write(raw)

    all_rows = sales_rows(sales, with_store=True)
    rollup = all_rows.drop(columns="st").groupby(["d", "b", "t", "r", "s", "ses"], as_index=False).sum()
    rollup.insert(1, "st", 0)
    dump(os.path.join(args.out, "rollup.json.gz"), {"rows": rollup.values.tolist()})

    per_store = 0
    by_store = dict(list(all_rows.groupby("st")))
    for i in range(len(stores)):   # every store gets a file, even if it only appears in ratings
        grp = by_store.get(i)
        rows = grp.values.tolist() if grp is not None else []
        dump(os.path.join(args.out, "stores", f"{i}.json.gz"), {"rows": rows})
        per_store += 1

    # Bucket tables use the same bands as the history file, applied to each branch-day-session row.
    AOV_LABELS = ["0-100", "100-200", "200-300", "300-400", "400-500", "500-600", "600-900", ">900"]
    AOV_EDGES = [100, 200, 300, 400, 500, 600, 900]
    DISC_LABELS = ["0%", "1%-10%", "10%-20%", "20%-30%", "30%-40%", "40%-50%", "50%-60%",
                   "60%-70%", "70%-80%", "80%-90%", "90%-100%"]
    DISC_EDGES = [10, 20, 30, 40, 50, 60, 70, 80, 90, 100]
    base_cols = ["d", "b", "t", "r", "s"]
    rb = all_rows[all_rows["orders"] > 0].copy()
    rb["bk"] = np.searchsorted(AOV_EDGES, (rb["net"] / 100 / rb["orders"]).values, side="left")
    aov_out = rb.groupby(base_cols + ["bk"], as_index=False)[["orders", "net"]].sum()
    dump(os.path.join(args.out, "aov_buckets.json.gz"), {"rows": aov_out.values.tolist()})
    rg = all_rows[all_rows["gross"] > 0].copy()
    dis = (rg["disc"] / rg["gross"] * 100).values
    rg["bk"] = np.where(dis == 0, 0, np.searchsorted(DISC_EDGES, dis, side="left") + 1)
    disc_out = rg.groupby(base_cols + ["bk"], as_index=False)[["orders", "net"]].sum()
    dump(os.path.join(args.out, "disc_buckets.json.gz"), {"rows": disc_out.values.tolist()})
    store_brands = {int(k): sorted(int(x) for x in v) for k, v in all_rows.groupby("st")["b"].unique().items()}

    has_live = False
    live_day = None
    if live is not None:
        live_rows = sales_rows(live.assign(Store=live["Branch"]), with_store=True)
        live_day = int(live_rows["d"].max())
        with open(os.path.join(args.out, "live.json"), "w", encoding="utf-8") as f:
            json.dump({"rows": live_rows.values.tolist()}, f, separators=(",", ":"))
        has_live = True
    elif os.path.exists(os.path.join(args.out, "live.json")):
        os.remove(os.path.join(args.out, "live.json"))

    has_ratings = False
    if ratings is not None and not ratings.empty:
        r = pd.DataFrame({
            "d": (ratings["Date"] - BASE).dt.days,
            "st": ratings["Store Name"].map(sidx),
            "b": ratings["Brand Name"].map(bidx),
            "t": ratings["Store Type"].map(tidx),
            "r": ratings["Region"].map(ridx),
            "p": ratings["Platform"].map(PLATFORMS.index),
            "rated": pd.to_numeric(ratings["Rated Orders"], errors="coerce").fillna(0).round().astype("int64"),
            "rsum": (pd.to_numeric(ratings["Avg Rating"], errors="coerce").fillna(0)
                     * pd.to_numeric(ratings["Rated Orders"], errors="coerce").fillna(0) * 100).round().astype("int64"),
            "orders": pd.to_numeric(ratings["Orders"], errors="coerce").fillna(0).round().astype("int64"),
        })
        r = r.groupby(["d", "st", "b", "t", "r", "p"], as_index=False).sum()
        dump(os.path.join(args.out, "ratings.json.gz"), {"rows": r.values.tolist()})
        has_ratings = True

    s26 = sales[sales["Date"].dt.year == 2026]
    manifest = {
        "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "base": BASE.strftime("%Y-%m-%d"),
        "last_day": int((sales["Date"].max() - BASE).days),
        "ratings_last_day": int((pd.to_datetime(ratings["Date"]).max() - BASE).days) if has_ratings else None,
        "live_day": live_day,
        "brands": brands,
        "regions": regions,
        "types": types,
        "sources": sources,
        "sessions": sessions,
        "platforms": PLATFORMS,
        "aov_labels": AOV_LABELS,
        "disc_labels": DISC_LABELS,
        "stores": [{"i": sidx[s], "name": s, "type": store_attr[s][0], "region": store_attr[s][1],
                    "brands": store_brands.get(sidx[s], [])} for s in stores],
        "has_live": has_live,
        "has_ratings": has_ratings,
        "stats": {
            "sales_rows": int(len(sales)),
            "unmapped_store_type_share_2026_pct": round(
                float(s26.loc[s26["Store Type"] == "Unmapped", "Net Sales"].sum()) / float(s26["Net Sales"].sum()) * 100, 2)
            if len(s26) and s26["Net Sales"].sum() else 0,
            "store_files": per_store,
        },
    }
    with open(os.path.join(args.out, "manifest.json"), "w", encoding="utf-8") as f:
        json.dump(manifest, f, indent=1)
    make_icons(os.path.dirname(os.path.normpath(args.out)))
    print(f"sales rows {len(sales)}, rollup rows {len(rollup)}, store files {per_store}, "
          f"live {has_live}, ratings {has_ratings}, last day {manifest['last_day']}")


if __name__ == "__main__":
    main()
