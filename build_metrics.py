"""Builds the Sales > Metrics data (data/metrics.json.gz) from the four metrics tabs:
Z_Funnel and Z_Ads (Zomato), S_Funnel and S_Ads (Swiggy). Each row is summed per day, source, brand, region and store type.

Definitions (ratios are taken from the sums, never averaged):
  Menu Open    = Zomato menu_opens   | Swiggy menu_sessions
  Cart         = Zomato cart_built   | Swiggy cart_session
  Orders       = Zomato order_makes  | Swiggy order_session        (funnel orders)
  M2O%         = Orders / Menu Open
  C2O%         = Orders / Cart
  Clicks       = Zomato clicks       | Swiggy clicks               (ads)
  Ad Orders    = Zomato ad_orders    | Swiggy ads_orders
  In-Organic%  = Ad Orders / Orders
  Organic%     = 1 - In-Organic%

Usage:  python build_metrics.py --xlsx Metrics_Data.xlsx --out data/metrics.json.gz
        (or --gsheet <sheet id> with a service account, see read_gsheet)
"""
import argparse, datetime as dt, gzip, json, os, re

import pandas as pd

EPOCH = dt.date(2025, 1, 1)
UNMAPPED = "Unmapped"
TABS = {"Z_Funnel", "Z_Ads", "S_Funnel", "S_Ads"}


EXCEL_EPOCH = dt.date(1899, 12, 30)   # Google Sheets' date serial numbers count days from here


def iso_day(x):
    """A date as YYYY-MM-DD. Accepts datetimes, ISO text, day-first or month-first text, and Google Sheets' date
    serial numbers. Blank cells give None."""
    if x is None or (not isinstance(x, str) and pd.isna(x)) or (isinstance(x, str) and x.strip() == ""):
        return None
    if isinstance(x, (dt.date, dt.datetime, pd.Timestamp)):
        return pd.Timestamp(x).strftime("%Y-%m-%d")
    if isinstance(x, (int, float)) and not isinstance(x, bool):
        return (EXCEL_EPOCH + dt.timedelta(days=int(x))).isoformat()
    s = str(x).strip()
    if re.fullmatch(r"\d+(\.\d+)?", s):
        return (EXCEL_EPOCH + dt.timedelta(days=int(float(s)))).isoformat()
    s = s[:10]
    for f in ("%Y-%m-%d", "%m/%d/%Y", "%d/%m/%Y"):
        try:
            return dt.datetime.strptime(s, f).strftime("%Y-%m-%d")
        except ValueError:
            pass
    raise ValueError(f"unknown date: {x!r}")


def text(x):
    s = "" if pd.isna(x) else str(x).strip()
    return s if s else UNMAPPED


def num(series):
    """A number. Text such as "1,234" (how a sheet can show numbers) is read as 1234; blanks count as 0."""
    s = series.astype(str).str.replace(",", "", regex=False).str.strip()
    return pd.to_numeric(s, errors="coerce").fillna(0.0)


def normalise(frames):
    """Returns one frame per source: date, source, brand, region, store_type, menu_open, cart, orders, clicks, ad_orders."""
    zf, za, sf, sa = frames["Z_Funnel"], frames["Z_Ads"], frames["S_Funnel"], frames["S_Ads"]
    z_funnel = pd.DataFrame({
        "date": zf["Aggregation"].map(iso_day), "source": "Zomato",
        "brand": zf["Brand Name"].map(text), "region": zf["Region"].map(text), "store_type": zf["Store Type"].map(text),
        "store": zf["Store Name"].map(text),
        "menu_open": num(zf["menu_opens"]), "cart": num(zf["cart_built"]), "orders": num(zf["order_makes"]),
        "clicks": 0.0, "ad_orders": 0.0})
    z_ads = pd.DataFrame({
        "date": za["duration"].map(iso_day), "source": "Zomato",
        "brand": za["Brand Name"].map(text), "region": za["Region"].map(text), "store_type": za["Store Type"].map(text),
        "store": za["Store Name"].map(text),
        "menu_open": 0.0, "cart": 0.0, "orders": 0.0,
        "clicks": num(za["clicks"]), "ad_orders": num(za["ad_orders"])})
    s_funnel = pd.DataFrame({
        "date": sf["date"].map(iso_day), "source": "Swiggy",
        "brand": sf["Brand Name"].map(text), "region": sf["Region"].map(text), "store_type": sf["Store Type"].map(text),
        "store": sf["Store Name"].map(text),
        "menu_open": num(sf["menu_sessions"]), "cart": num(sf["cart_session"]), "orders": num(sf["order_session"]),
        "clicks": 0.0, "ad_orders": 0.0})
    s_ads = pd.DataFrame({
        "date": sa["dt"].map(iso_day), "source": "Swiggy",
        "brand": sa["Brand Name"].map(text), "region": sa["Region"].map(text), "store_type": sa["Store Type"].map(text),
        "store": sa["Store Name"].map(text),
        "menu_open": 0.0, "cart": 0.0, "orders": 0.0,
        "clicks": num(sa["clicks"]), "ad_orders": num(sa["ads_orders"])})
    out = pd.concat([z_funnel, z_ads, s_funnel, s_ads], ignore_index=True)
    return out[out["date"].notna()].reset_index(drop=True)


def aggregate(rows):
    keys = ["date", "source", "brand", "region", "store_type", "store"]
    return rows.groupby(keys, as_index=False)[["menu_open", "cart", "orders", "clicks", "ad_orders"]].sum()


def build(frames):
    rows = aggregate(normalise(frames))
    rows = rows[rows[["menu_open", "cart", "orders", "clicks", "ad_orders"]].abs().sum(axis=1) > 0]
    sources = sorted(rows["source"].unique())
    brands = sorted(rows["brand"].unique())
    regions = sorted(rows["region"].unique())
    types = sorted(rows["store_type"].unique())
    stores = sorted(rows["store"].unique())
    idx = lambda vals: {v: i for i, v in enumerate(vals)}
    si, bi, ri, ti, sti = idx(sources), idx(brands), idx(regions), idx(types), idx(stores)
    out = []
    for _, r in rows.iterrows():
        d = (dt.date.fromisoformat(r["date"]) - EPOCH).days
        out.append([d, si[r["source"]], bi[r["brand"]], ri[r["region"]], ti[r["store_type"]],
                    round(r["menu_open"], 2), round(r["cart"], 2), round(r["orders"], 2),
                    round(r["clicks"], 2), round(r["ad_orders"], 2), sti[r["store"]]])
    out.sort()
    return {"epoch": "2025-01-01", "sources": sources, "brands": brands, "regions": regions, "storeTypes": types,
            "stores": stores,
            "dateMin": rows["date"].min(), "dateMax": rows["date"].max(),
            "fields": ["day", "source", "brand", "region", "storeType", "menuOpen", "cart", "orders", "clicks", "adOrders", "store"],
            "rows": out}


def read_xlsx(path):
    return {name: pd.read_excel(path, sheet_name=name) for name in TABS}


def read_gsheet(sheet_id, creds):
    """Reads the four tabs from the Google Sheet with a service account. creds is the path to the key file, or the key
    itself as text (the GOOGLE_CREDENTIALS secret). Values are read as the sheet stores them: dates as serial numbers."""
    import gspread
    if creds.strip().startswith("{"):
        gc = gspread.service_account_from_dict(json.loads(creds))
    else:
        gc = gspread.service_account(filename=creds)
    try:
        sh = gc.open_by_key(sheet_id)
    except Exception as e:
        raise SystemExit(f"could not open the Google Sheet {sheet_id}: {e}. "
                         "Share the sheet with the service account email (Viewer is enough).")
    names = [w.title for w in sh.worksheets()]
    missing = sorted(TABS - set(names))
    if missing:
        raise SystemExit(f"tabs missing in the Google Sheet: {missing}. Tabs found: {names}")
    out = {}
    for name in sorted(TABS):
        vals = sh.worksheet(name).get_all_values(value_render_option="UNFORMATTED_VALUE")
        head = [str(h).strip() for h in vals[0]]
        dup = sorted({h for h in head if h and head.count(h) > 1})
        if dup:
            raise SystemExit(f"{name}: duplicate column names {dup}")
        rows = [list(r) + [""] * (len(head) - len(r)) for r in vals[1:]]
        out[name] = pd.DataFrame(rows, columns=head)
    return out


def summary_text(data):
    """The lines shown on the GitHub run page: how many rows, which days, how many stores."""
    return ("### Metrics from the Google Sheet\n"
            f"- rows: {len(data['rows'])}\n"
            f"- days in the data: {data['dateMin']} to {data['dateMax']}\n"
            f"- stores: {len(data.get('stores', []))}\n")


def write_step_summary(data):
    path = os.environ.get("GITHUB_STEP_SUMMARY")
    if path:
        with open(path, "a", encoding="utf-8") as fh:
            fh.write(summary_text(data))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--xlsx")
    ap.add_argument("--gsheet")
    ap.add_argument("--creds", default=os.environ.get("GOOGLE_CREDENTIALS") or "service_account.json")
    ap.add_argument("--out", default="data/metrics.json.gz")
    a = ap.parse_args()
    frames = read_xlsx(a.xlsx) if a.xlsx else read_gsheet(a.gsheet, a.creds)
    data = build(frames)
    os.makedirs(os.path.dirname(a.out) or ".", exist_ok=True)
    # mtime=0: the same data always gives the same bytes, so the repository only changes when the data does
    with open(a.out, "wb") as raw, gzip.GzipFile(filename="", fileobj=raw, mode="wb", mtime=0) as gz:
        gz.write(json.dumps(data, separators=(",", ":")).encode("utf-8"))
    print(f"metrics: {len(data['rows'])} rows, {data['dateMin']} to {data['dateMax']}, "
          f"sources {data['sources']}, brands {len(data['brands'])}, regions {len(data['regions'])} -> {a.out}")
    write_step_summary(data)


if __name__ == "__main__":
    main()
