"""Writes tests/fixtures/metrics.json.gz and tests/fixtures/metrics_expected.json from the Metrics workbook.
The fixtures hold company figures, so keep them out of git (add tests/fixtures/ to .gitignore).

    python tests/make_metrics_expected.py path/to/Metrics_Data.xlsx
"""
import datetime as dt, gzip, json, os, subprocess, sys

import pandas as pd

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(HERE)
FIX = os.path.join(HERE, "fixtures")
EPOCH = dt.date(2025, 1, 1)


def day(d):
    return (d - EPOCH).days


def ddate(x):
    return pd.Timestamp(x).date() if not isinstance(x, str) else dt.date.fromisoformat(x[:10])


def main(xlsx):
    os.makedirs(FIX, exist_ok=True)
    subprocess.run([sys.executable, os.path.join(REPO, "repo-root", "build_metrics.py"), "--xlsx", xlsx,
                    "--out", os.path.join(FIX, "metrics.json.gz")], check=True)
    fr = {n: pd.read_excel(xlsx, sheet_name=n) for n in ["Z_Funnel", "Z_Ads", "S_Funnel", "S_Ads"]}
    zf, za, sf, sa = fr["Z_Funnel"], fr["Z_Ads"], fr["S_Funnel"], fr["S_Ads"]
    n = lambda s: pd.to_numeric(s, errors="coerce").fillna(0)
    raw = pd.concat([
        pd.DataFrame({"day": zf["Aggregation"].map(lambda x: day(ddate(x))), "source": "Zomato", "brand": zf["Brand Name"],
                      "region": zf["Region"], "type": zf["Store Type"], "menu": n(zf["menu_opens"]),
                      "cart": n(zf["cart_built"]), "orders": n(zf["order_makes"]), "clicks": 0.0, "ad": 0.0}),
        pd.DataFrame({"day": za["duration"].map(lambda x: day(ddate(x))), "source": "Zomato", "brand": za["Brand Name"],
                      "region": za["Region"], "type": za["Store Type"], "menu": 0.0, "cart": 0.0, "orders": 0.0,
                      "clicks": n(za["clicks"]), "ad": n(za["ad_orders"])}),
        pd.DataFrame({"day": sf["date"].map(lambda x: day(ddate(x))), "source": "Swiggy", "brand": sf["Brand Name"],
                      "region": sf["Region"], "type": sf["Store Type"], "menu": n(sf["menu_sessions"]),
                      "cart": n(sf["cart_session"]), "orders": n(sf["order_session"]), "clicks": 0.0, "ad": 0.0}),
        pd.DataFrame({"day": sa["dt"].map(lambda x: day(ddate(x))), "source": "Swiggy", "brand": sa["Brand Name"],
                      "region": sa["Region"], "type": sa["Store Type"], "menu": 0.0, "cart": 0.0, "orders": 0.0,
                      "clicks": n(sa["clicks"]), "ad": n(sa["ads_orders"])}),
    ], ignore_index=True)
    last = int(raw["day"].max())
    A, B = (last - 6, last), (last - 13, last - 7)
    combos = {"none": {}, "zomato_ka": {"source": "Zomato", "region": "KA"}, "swiggy_coco": {"source": "Swiggy", "type": "COCO"}}

    def ratios(t):
        div = lambda a, b: a / b * 100 if b else None
        inorg = div(t["ad"], t["orders"])
        return {"menuOpen": t["menu"], "orders": t["orders"], "cart": t["cart"], "clicks": t["clicks"], "adOrders": t["ad"],
                "m2o": div(t["orders"], t["menu"]), "c2o": div(t["orders"], t["cart"]), "inOrg": inorg,
                "org": None if inorg is None else 100 - inorg}

    out = {"periods": {"A": A, "B": B}, "combos": {}}
    for name, f in combos.items():
        x = raw
        for k, col in (("source", "source"), ("region", "region"), ("type", "type")):
            if k in f:
                x = x[x[col] == f[k]]
        out["combos"][name] = {}
        for pk, (a, b) in (("A", A), ("B", B)):
            y = x[(x["day"] >= a) & (x["day"] <= b)]
            tot = {"menu": y["menu"].sum(), "cart": y["cart"].sum(), "orders": y["orders"].sum(),
                   "clicks": y["clicks"].sum(), "ad": y["ad"].sum()}
            out["combos"][name][pk] = {"total": ratios(tot),
                                       "ordersByDay": [float(y[y["day"] == d]["orders"].sum()) for d in range(a, b + 1)]}
    with open(os.path.join(FIX, "metrics_expected.json"), "w") as fh:
        json.dump(out, fh)
    print("fixtures written to", FIX)


if __name__ == "__main__":
    main(sys.argv[1])
