"""Category and product level ratings for Swiggy and Zomato.

The rating sheets carry a category and an item name on each row. This module finds those columns by name
(so a renamed header still matches), and builds the item-level rows the dashboard reads: one row per
store, day, platform, category and item, with rated orders, rating sum (x100) and orders.
Rows with no category or item (Google, and any sheet without the columns) are grouped as
"(no category)" and "(all items)"."""
import re

import pandas as pd

CATEGORY_KEYS = ["category", "categoryname", "itemcategory", "productcategory"]
ITEM_KEYS = ["itemname", "item", "productname", "product", "skuname"]
NO_CATEGORY = "(no category)"
NO_ITEM = "(all items)"


def _norm(name):
    return re.sub(r"[^a-z]", "", str(name).lower())


def _find(columns, keys):
    by_norm = {_norm(c): c for c in columns}
    for k in keys:
        if k in by_norm:
            return by_norm[k]
    return None


def detect_item_columns(columns):
    """(category column, item column) from the sheet headers; None where a column is not there."""
    return _find(columns, CATEGORY_KEYS), _find(columns, ITEM_KEYS)


def item_rating_rows(df, base, sidx, bidx, tidx, ridx, pidx):
    """df: rating rows with Date (datetime), Store Name, Brand Name, Store Type, Region, Platform,
    Orders, Rated Orders, Avg Rating, Category and Item Name (the last two may be blank).
    Returns (frame, categories, items). Frame columns: d, st, b, t, r, p, c, i, rated, rsum, orders,
    where c and i index into categories and items, and rsum = avg x rated x 100 (rounded)."""
    d = df.copy()
    d["Category"] = d["Category"].fillna("").astype(str).str.strip().replace("", NO_CATEGORY)
    d["Item Name"] = d["Item Name"].fillna("").astype(str).str.strip().replace("", NO_ITEM)
    cats = sorted(d["Category"].unique())
    items = sorted(d["Item Name"].unique())
    cidx = {c: i for i, c in enumerate(cats)}
    iidx = {x: i for i, x in enumerate(items)}
    rated = pd.to_numeric(d["Rated Orders"], errors="coerce").fillna(0)
    avg = pd.to_numeric(d["Avg Rating"], errors="coerce").fillna(0)
    out = pd.DataFrame({
        "d": (d["Date"] - base).dt.days,
        "st": d["Store Name"].map(sidx),
        "b": d["Brand Name"].map(bidx),
        "t": d["Store Type"].map(tidx),
        "r": d["Region"].map(ridx),
        "p": d["Platform"].map(pidx),
        "c": d["Category"].map(cidx),
        "i": d["Item Name"].map(iidx),
        "rated": rated.round().astype("int64"),
        "rsum": (avg * rated * 100).round().astype("int64"),
        "orders": pd.to_numeric(d["Orders"], errors="coerce").fillna(0).round().astype("int64"),
    })
    out = out.groupby(["d", "st", "b", "t", "r", "p", "c", "i"], as_index=False).sum()
    return out, cats, items
