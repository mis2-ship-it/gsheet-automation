SALES > METRICS: SWIGGY AND ZOMATO FUNNEL AND ADS

WHAT IT SHOWS
  A "Metrics" sub-tab under Sales.
  - Comparison A and comparison B, each a from and to date (defaults: the last 7 days, and the 7 days before).
  - Filters: source (All, Swiggy, Zomato), brand, store type and region. They apply to the whole tab.
  - A summary table: Menu Open, Orders, M2O%, C2O%, Clicks, Ad Orders, Organic%, In-Organic%, and the A vs B change.
  - Three line charts, A solid and B dashed, drawn by day of the period (Day 1, Day 2 ...):
      M2O% and C2O%;  Organic% and In-Organic%;  Clicks (left scale) and Ad Orders (right scale).
  - A day-by-day table for comparison A and one for B, with Date, Menu Open, Orders, M2O%, C2O%, Clicks,
    Ad Orders, Organic% and In-Organic%.

CALCULATIONS (ratios come from the sums, never averaged)
  Menu Open = Zomato menu_opens | Swiggy menu_sessions
  Orders    = Zomato order_makes | Swiggy order_session  (funnel orders)
  Cart      = Zomato cart_built  | Swiggy cart_session
  M2O%      = Orders / Menu Open
  C2O%      = Orders / Cart
  Clicks    = ad clicks (Zomato clicks | Swiggy clicks)
  Ad Orders = Zomato ad_orders | Swiggy ads_orders
  In-Organic% = Ad Orders / Orders;  Organic% = 1 - In-Organic%

FILES
  repo-root/build_metrics.py   builds data/metrics.json.gz from the four tabs (an xlsx file now; the Google Sheet with --gsheet)
  site/metrics.js              the Metrics tab (loaded after productmix.js)
  site/index.html              adds: <script src="metrics.js"></script> after productmix.js
  tests/test_metrics.js        checks the tab against the raw figures
  tests/make_metrics_expected.py   writes the test fixtures from the workbook (keep tests/fixtures/ out of git)

TO CONNECT THE DATA (not yet done)
  1. Share the Google Sheet with the service account used by the discount sheet writer.
  2. In the sales workflow, after the data sync, run:
       python build_metrics.py --gsheet <sheet id> --creds <service account json> --out data/metrics.json.gz
  3. build_groups.py has to copy data/metrics.json.gz into each login's data folder, as for the other data.
     The metrics have brand, region and store type but no store names. Decide whether each login sees all regions.

DEPLOY
  Upload site/metrics.js and site/index.html next to the page, then hard-refresh.
