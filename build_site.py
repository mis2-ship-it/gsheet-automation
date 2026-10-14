#!/usr/bin/env python3
"""
Build the data files for the Frozen Bottle sales and ratings dashboard.

Inputs (CSV files, optional except --history):
  --history     historical sales file (same columns as historical_sales.csv.gz)
  --new         completed days after the history, daily rows (data/new_sales.csv.gz)
  --new-hourly  hourly rows for the last 14 business days (data/new_hourly.csv.gz)
  --live        hourly rows for today's business day (data/live_sales.csv)
                live_meta.json beside it gives the business day and the time of the pull
  --ratings     Swiggy / Zomato / Google review rows (data/ratings.csv)

Outputs (in --out, normally site/data):
  manifest.json          dimensions, store list, days, flags
  rollup.json.gz         all stores together, daily (store column is 0)
  stores/<i>.json.gz     one file per store, daily
  aov_buckets.json.gz    orders by AOV band
  disc_buckets.json.gz   orders by discount band
  hourly.json.gz         hourly rows: the last 14 business days and today
  ratings.json.gz        ratings rows (when --ratings is given)
  products.json.gz       monthly item and category rows (Swiggy and Zomato)
  comments.json.gz       recent customer comments by platform, star and month

Daily row:   [day, store, brand, store_type, region, source, session, net_paise, gross_paise, discount_paise, orders]
Hourly row:  [day, store, brand, store_type, region, source, session, hour, net_paise, gross_paise, discount_paise, orders]
Ratings row: [day, store, brand, store_type, region, platform, rated_orders, rating_sum_x100, orders]
Day 0 is 2025-01-01. Hour 0 is 9 AM.
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
AOV_LABELS = ["0-100", "100-200", "200-300", "300-400", "400-500", "500-600", "600-900", ">900"]
AOV_EDGES = [100, 200, 300, 400, 500, 600, 900]
DISC_LABELS = ["0%", "1%-10%", "10%-20%", "20%-30%", "30%-40%", "40%-50%", "50%-60%",
               "60%-70%", "70%-80%", "80%-90%", "90%-100%"]
DISC_EDGES = [10, 20, 30, 40, 50, 60, 70, 80, 90, 100]


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


def clean_hourly(df):
    df = clean_sales(df)
    df["Hour"] = pd.to_numeric(df["Hour"], errors="coerce").fillna(0).astype(int)
    return df


def read_if_present(path):
    if path and os.path.exists(path) and os.path.getsize(path) > 0:
        df = pd.read_csv(path, compression="infer", low_memory=False)
        return df if not df.empty else None
    return None


ISSUE_ORDER = ["quantity", "quality", "packing", "melt", "wrong", "missing"]


def issue_index(text):
    """Map a Comments Group value to one of the six hygiene issues (0-5), or -1 for other issues."""
    t = str(text).lower()
    for i, key in enumerate(ISSUE_ORDER):
        if key in t:
            return i
    if "ice" in t:
        return 3
    if "pack" in t:
        return 2
    return -1


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
    ap.add_argument("--new-hourly")
    ap.add_argument("--live")
    ap.add_argument("--ratings")
    ap.add_argument("--out", default="site/data")
    args = ap.parse_args()

    os.makedirs(os.path.join(args.out, "stores"), exist_ok=True)

    # Daily sales: history, then the completed days after it.
    sales = clean_sales(pd.read_csv(args.history, compression="infer", low_memory=False))
    new_raw = read_if_present(args.new)
    if new_raw is not None:
        new = clean_sales(new_raw)
        cut = new["Date"].min()
        sales = pd.concat([sales[sales["Date"] < cut], new], ignore_index=True)

    # Hourly sales: the last 14 business days, and today so far.
    nh_raw = read_if_present(args.new_hourly)
    nh = clean_hourly(nh_raw) if nh_raw is not None else None
    lh_raw = read_if_present(args.live)
    lh = clean_hourly(lh_raw) if lh_raw is not None else None
    live_meta = None
    if args.live:
        meta_path = os.path.join(os.path.dirname(args.live) or ".", "live_meta.json")
        if os.path.exists(meta_path):
            with open(meta_path, encoding="utf-8") as f:
                live_meta = json.load(f)

    ratings = None
    rat_raw = read_if_present(args.ratings)
    if rat_raw is not None:
        ratings = rat_raw
        ratings["Date"] = pd.to_datetime(ratings["Date"])
        ratings["Store Name"] = ratings["Store Name"].astype(str).str.strip()
        ratings["Brand Name"] = ratings["Brand Name"].astype(str).str.strip()
        ratings["Store Type"] = ratings["Store Type"].fillna("Unmapped").astype(str).str.strip()
        ratings["Region"] = ratings["Region"].fillna("Unmapped").astype(str).str.strip()
        ratings = ratings[ratings["Brand Name"].isin(BRAND_ORDER)]
        if ratings.empty:
            ratings = None

    hourly_frames = [f for f in [nh, lh] if f is not None]
    for f in [sales] + hourly_frames:
        f["Store"] = f["Branch"]

    # Store master: the latest real store type and region seen for each store.
    known = sales[sales["Store Type"] != "Unmapped"].sort_values("Date").groupby("Store").last()
    names = set(sales["Store"])
    for f in hourly_frames:
        names |= set(f["Store"])
    if ratings is not None:
        names |= set(ratings["Store Name"])
    store_attr = {}
    for name in names:
        if name in known.index:
            store_attr[name] = (known.loc[name, "Store Type"], known.loc[name, "Region"])
        else:
            store_attr[name] = ("Unmapped", "Unmapped")
    stores = sorted(store_attr)
    sidx = {s: i for i, s in enumerate(stores)}

    every = [sales] + hourly_frames
    brands = ordered([b for f in every for b in f["Brand Name"]] + (list(ratings["Brand Name"]) if ratings is not None else []), BRAND_ORDER)
    bidx = {b: i for i, b in enumerate(brands)}
    types = ordered([a[0] for a in store_attr.values()] + [t for f in every for t in f["Store Type"]], TYPE_ORDER)
    tidx = {t: i for i, t in enumerate(types)}
    regions = ordered([a[1] for a in store_attr.values()] + [r for f in every for r in f["Region"]],
                      ["KA", "TN", "MH", "ROI", "AP & TL", "Kerela", "Unmapped"])
    ridx = {r: i for i, r in enumerate(regions)}
    sources = ordered([s for f in every for s in f["Source"]], SOURCE_ORDER)
    soidx = {s: i for i, s in enumerate(sources)}
    sessions = ordered([s for f in every for s in f["Session"]], SESSION_ORDER)
    sesidx = {s: i for i, s in enumerate(sessions)}

    def common(df):
        return {
            "d": (df["Date"] - BASE).dt.days,
            "st": df["Store"].map(sidx),
            "b": df["Brand Name"].map(bidx),
            "t": df["Store Type"].map(tidx),
            "r": df["Region"].map(ridx),
            "s": df["Source"].map(soidx),
            "ses": df["Session"].map(sesidx),
            "net": (df["Net Sales"] * 100).round().astype("int64"),
            "gross": (df["Gross Sales"] * 100).round().astype("int64"),
            "disc": (df["Discount"] * 100).round().astype("int64"),
            "orders": df["Orders"].round().astype("int64"),
        }

    def daily_rows(df):
        t = pd.DataFrame(common(df))
        keys = ["d", "st", "b", "t", "r", "s", "ses"]
        t = t.groupby(keys, as_index=False).sum()
        return t[keys + ["net", "gross", "disc", "orders"]]

    def hourly_rows(df):
        t = pd.DataFrame(common(df))
        t["h"] = df["Hour"].values
        keys = ["d", "st", "b", "t", "r", "s", "ses", "h"]
        t = t.groupby(keys, as_index=False).sum()
        return t[keys + ["net", "gross", "disc", "orders"]]

    def dump(path, obj):
        raw = json.dumps(obj, separators=(",", ":")).encode("utf-8")
        with gzip.open(path, "wb", compresslevel=9) as f:
            f.write(raw)

    # Daily files
    all_rows = daily_rows(sales)
    rollup = all_rows.drop(columns="st").groupby(["d", "b", "t", "r", "s", "ses"], as_index=False).sum()
    rollup.insert(1, "st", 0)
    dump(os.path.join(args.out, "rollup.json.gz"), {"rows": rollup.values.tolist()})

    per_store = 0
    by_store = dict(list(all_rows.groupby("st")))
    for i in range(len(stores)):   # every store gets a file, even if it only appears in hourly or ratings
        grp = by_store.get(i)
        dump(os.path.join(args.out, "stores", f"{i}.json.gz"), {"rows": grp.values.tolist() if grp is not None else []})
        per_store += 1

    # Bucket tables: every original row gets one AOV band and one discount band, as the history file labels them.
    base_cols = ["d", "b", "t", "r", "s"]
    orig = pd.DataFrame({
        "d": (sales["Date"] - BASE).dt.days.values,
        "b": sales["Brand Name"].map(bidx).values,
        "t": sales["Store Type"].map(tidx).values,
        "r": sales["Region"].map(ridx).values,
        "s": sales["Source"].map(soidx).values,
        "net": (sales["Net Sales"] * 100).round().astype("int64").values,
        "gross": (sales["Gross Sales"] * 100).round().astype("int64").values,
        "disc": (sales["Discount"] * 100).round().astype("int64").values,
        "orders": sales["Orders"].round().astype("int64").values,
    })
    rb = orig[orig["orders"] > 0].copy()
    rb["bk"] = np.searchsorted(AOV_EDGES, (rb["net"] / 100 / rb["orders"]).values, side="left")
    aov_out = rb.groupby(base_cols + ["bk"], as_index=False)[["orders", "net"]].sum()
    dump(os.path.join(args.out, "aov_buckets.json.gz"), {"rows": aov_out.values.tolist()})
    rg = orig[orig["gross"] > 0].copy()
    dis = (rg["disc"] / rg["gross"] * 100).values
    rg["bk"] = np.where(dis == 0, 0, np.searchsorted(DISC_EDGES, dis, side="left") + 1)
    disc_out = rg.groupby(base_cols + ["bk"], as_index=False)[["orders", "net"]].sum()
    dump(os.path.join(args.out, "disc_buckets.json.gz"), {"rows": disc_out.values.tolist()})
    store_brands = {int(k): sorted(int(x) for x in v) for k, v in all_rows.groupby("st")["b"].unique().items()}

    # Hourly file: last 14 business days and today.
    hourly_parts = [hourly_rows(f) for f in hourly_frames]
    hourly_all = pd.concat(hourly_parts, ignore_index=True) if hourly_parts else pd.DataFrame()
    hourly_values = hourly_all.values.tolist() if not hourly_all.empty else []
    dump(os.path.join(args.out, "hourly.json.gz"), {"rows": hourly_values})
    stale = os.path.join(args.out, "live.json")
    if os.path.exists(stale):
        os.remove(stale)

    live_day = None
    if live_meta is not None:
        live_day = int((pd.to_datetime(live_meta["business_day"]) - BASE).days)
    elif lh is not None:
        live_day = int((lh["Date"].max() - BASE).days)

    has_ratings = False
    if ratings is not None:
        rt = ratings
        rated_flag = (pd.to_numeric(rt["Rated Orders"], errors="coerce").fillna(0).values > 0) & \
                     (pd.to_numeric(rt["Avg Rating"], errors="coerce").fillna(0).values > 0)
        orders_n = pd.to_numeric(rt["Orders"], errors="coerce").fillna(0).values
        rating = pd.to_numeric(rt["Avg Rating"], errors="coerce").fillna(0).values
        w = np.where(rated_flag, orders_n, 0.0)          # rated orders = Orders Count of rows that have a rating
        star = np.clip(np.round(rating), 1, 5).astype(int)
        comm = rt["Comments Group"].fillna("").astype(str).str.strip().values if "Comments Group" in rt.columns \
            else np.array([""] * len(rt))
        issue = rated_flag & (comm != "")
        cat = np.array([issue_index(c) if c else -1 for c in comm])
        rf = pd.DataFrame({
            "d": (rt["Date"] - BASE).dt.days.values,
            "st": rt["Store Name"].map(sidx).values,
            "b": rt["Brand Name"].map(bidx).values,
            "t": rt["Store Type"].map(tidx).values,
            "r": rt["Region"].map(ridx).values,
            "p": rt["Platform"].map(PLATFORMS.index).values,
            "rated": w.astype("int64"),
            "rsum": np.round(rating * w * 100).astype("int64"),
            "orders": orders_n.round().astype("int64"),
        })
        for k in range(1, 6):
            rf[f"s{k}"] = (w * (star == k)).astype("int64")
        rf["iss"] = (w * issue).astype("int64")
        for j in range(6):
            rf[f"c{j}"] = (w * (issue & (cat == j))).astype("int64")
        keys = ["d", "st", "b", "t", "r", "p"]
        rf = rf.groupby(keys, as_index=False).sum()
        rcols = ["rated", "rsum", "orders", "s1", "s2", "s3", "s4", "s5", "iss", "c0", "c1", "c2", "c3", "c4", "c5"]
        dump(os.path.join(args.out, "ratings.json.gz"), {"rows": rf[keys + rcols].values.tolist()})
        has_ratings = True

        # Product level (monthly): items and categories, by platform, brand and region.
        mon = ((rt["Date"].dt.year - 2025) * 12 + rt["Date"].dt.month - 1).values
        item_txt = rt["Item Name"].fillna("").astype(str).str.strip().values if "Item Name" in rt.columns else np.array([""] * len(rt))
        cat_txt = rt["Category Name"].fillna("").astype(str).str.strip().values if "Category Name" in rt.columns else np.array([""] * len(rt))
        review = rt["Review Text"].fillna("").astype(str).str.replace(r"\s+", " ", regex=True).str.strip().str.slice(0, 220).values \
            if "Review Text" in rt.columns else np.array([""] * len(rt))
        prod_names, prod_id = [], {}

        def name_id(text):
            if not text:
                return -1
            if text not in prod_id:
                prod_id[text] = len(prod_names)
                prod_names.append(text)
            return prod_id[text]

        base = pd.DataFrame({
            "m": mon, "p": rt["Platform"].map(PLATFORMS.index).values,
            "b": rt["Brand Name"].map(bidx).values, "r": rt["Region"].map(ridx).values,
            "rated": w.astype("int64"), "rsum": np.round(rating * w * 100).astype("int64"),
            "orders": orders_n.round().astype("int64"),
        })
        for k in range(1, 6):
            base[f"s{k}"] = (w * (star == k)).astype("int64")
        prod_rows = []
        for kind, txt in [(0, item_txt), (1, cat_txt)]:
            sub = base.copy()
            sub["n"] = [name_id(t) for t in txt]
            sub = sub[sub["n"] >= 0]
            if sub.empty:
                continue
            g = sub.groupby(["m", "p", "b", "r", "n"], as_index=False).sum()
            g["kind"] = kind
            prod_rows += g[["m", "p", "b", "r", "kind", "n", "rated", "rsum", "orders", "s1", "s2", "s3", "s4", "s5"]].values.tolist()
        dump(os.path.join(args.out, "products.json.gz"), {"names": prod_names, "rows": prod_rows})

        # Customer comments: the 200 most recent with text, per platform, star and month.
        cdf = pd.DataFrame({
            "d": (rt["Date"] - BASE).dt.days.values, "m": mon,
            "p": rt["Platform"].map(PLATFORMS.index).values, "star": star,
            "b": rt["Brand Name"].map(bidx).values, "r": rt["Region"].map(ridx).values,
            "st": rt["Store Name"].map(sidx).values,
            "item": [name_id(t) for t in item_txt], "cat": [name_id(t) for t in cat_txt],
            "text": review,
        })
        cdf = cdf[(review != "") & (rating > 0)]
        cdf = cdf.sort_values("d", ascending=False).groupby(["p", "star", "m"]).head(200)
        comm_rows = cdf[["d", "p", "star", "b", "r", "st", "item", "cat", "text"]].values.tolist()
        dump(os.path.join(args.out, "comments.json.gz"), {"rows": comm_rows})

    s26 = sales[sales["Date"].dt.year == 2026]
    manifest = {
        "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "base": BASE.strftime("%Y-%m-%d"),
        "last_day": int((sales["Date"].max() - BASE).days),
        "ratings_last_day": int((pd.to_datetime(ratings["Date"]).max() - BASE).days) if has_ratings else None,
        "live_day": live_day,
        "live_as_of": live_meta.get("as_of") if live_meta else None,
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
        "has_live": live_meta is not None,
        "has_hourly": bool(hourly_values),
        "has_ratings": has_ratings,
        "has_products": has_ratings and bool(prod_rows),
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
    print(f"daily rows {len(sales)}, rollup rows {len(rollup)}, store files {per_store}, "
          f"hourly rows {len(hourly_values)}, live {manifest['has_live']}, ratings {has_ratings}, "
          f"last day {manifest['last_day']}")


if __name__ == "__main__":
    main()
