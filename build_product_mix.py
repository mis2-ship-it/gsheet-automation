"""Builds the Morning Call product mix from the product rows the discount job writes (data/product_rows.csv).

Keeps the last 14 days up to the latest day in the file, by day, brand, region, source and category:
net and gross sales (paise), discount, quantity and orders (distinct invoices). Charges and other differences are left out.
Output: product_mix.json.gz

    python build_product_mix.py --rows data/product_rows.csv --out data/product_mix.json.gz
"""
import argparse, datetime as dt, gzip, json, os, sys

import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from write_discount_sheet import NON_PRODUCT  # the same list the discount sheet uses

EPOCH = dt.date(2025, 1, 1)
DAYS = 14


def build(rows, days=DAYS):
    r = rows.copy()
    r = r[~r["item"].isin(NON_PRODUCT)]
    for c in ["qty", "gross", "disc", "net"]:
        r[c] = pd.to_numeric(r[c], errors="coerce").fillna(0.0)
    r["day"] = r["day"].astype(str).str[:10]
    r["invoiceNumber"] = r["invoiceNumber"].astype(str)
    latest = r["day"].max()
    first = (dt.date.fromisoformat(latest) - dt.timedelta(days=days - 1)).isoformat()
    r = r[(r["day"] >= first) & (r["day"] <= latest)]
    names = {k: sorted(r[k].astype(str).unique().tolist()) for k in ["brand", "region", "source", "category"]}
    idx = {k: {v: i for i, v in enumerate(names[k])} for k in names}
    g = (r.groupby(["day", "brand", "region", "source", "category"])
           .agg(net=("net", "sum"), gross=("gross", "sum"), disc=("disc", "sum"), qty=("qty", "sum"),
                orders=("invoiceNumber", "nunique")).reset_index())
    out_rows = []
    for _, x in g.iterrows():
        d = (dt.date.fromisoformat(x["day"]) - EPOCH).days
        out_rows.append([d, idx["brand"][str(x["brand"])], idx["region"][str(x["region"])],
                         idx["source"][str(x["source"])], idx["category"][str(x["category"])],
                         int(round(x["net"] * 100)), int(round(x["gross"] * 100)), int(round(x["disc"] * 100)),
                         round(float(x["qty"]), 2), int(x["orders"])])
    return {"ref": latest, "dayBase": "2025-01-01", "brands": names["brand"], "regions": names["region"],
            "sources": names["source"], "categories": names["category"],
            "fields": ["day", "brand", "region", "source", "category", "net_paise", "gross_paise", "disc_paise", "qty", "orders"],
            "rows": out_rows}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rows", default="data/product_rows.csv")
    ap.add_argument("--out", default="data/product_mix.json.gz")
    a = ap.parse_args()
    rows = pd.read_csv(a.rows)
    data = build(rows)
    os.makedirs(os.path.dirname(a.out) or ".", exist_ok=True)
    with gzip.open(a.out, "wt", encoding="utf-8") as fh:
        json.dump(data, fh, separators=(",", ":"))
    print(f"product_mix: {len(data['rows'])} rows, latest {data['ref']}, {len(data['categories'])} categories -> {a.out}")


if __name__ == "__main__":
    main()
