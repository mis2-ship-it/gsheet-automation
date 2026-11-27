PRODUCT LEVEL TAB: CLOSED ITEM SALES OF THE LAST 62 DAYS, BY STORE, BRAND, REGION, STORE TYPE, SOURCE, CATEGORY AND ITEM

WHAT IT DOES
  Sales > Product Level
    Filters: Brand, Region, Store type, Store name, Area manager; From and To dates (default: the last 7 days).
    Measure: Net sales, Orders, Dis % (discount over gross), Discount, AOV (net over orders). Growth % (points for Dis %)
             is against the previous period of the same length.
    Each product opens to its categories underneath.
    Columns: Brand, Source, Region or Store. The top 12 by net sales, and Others for the rest.

HOW THE DATA FLOWS
  product.yml (Product sales pull): every morning at 06:15 IST it pulls the last two business days, writes one file per
    day in data/product/ (closed orders only, as the discount job), keeps the last 62 days, and commits the folder.
  build_groups.py: each login gets the day files, cut to its stores, as data/product_items.csv.gz.
  site/product.js: the tab reads that file and works out the table in the browser.

FILES
  product.yml                          .github/workflows (new)
  update-sales-dashboard.yml           .github/workflows (passes the day files to the build)
  build_groups.py                      repo root (adds --product)
  product_sales.py                     repo root (new): the pull and the per-day files
  cancel_sales.py                      repo root: NOT in this package; use the latest one from the cancellation work,
                                       product_sales.py imports pull(), business_day() and days_between() from it
  site/product.js                      new
  add_cancel_tab.py                    repo root (now adds any number of page scripts)

STEPS
  1. Copy the files. Make sure cancel_sales.py is the latest version (it is the one with item lines).
  2. Add the scripts to the page:   python add_cancel_tab.py site/index.html cancel.js product.js
  3. Back-fill the 62 days: Actions > "Product sales pull" > Run workflow, with from and to. A long range takes a long time,
     so run it in chunks of about 7 to 14 days, one after another.
  4. In the run log, read the step "Show the sizes": it prints the number of day files and the folder size.
     Send us that output. Each day file should be a few hundred KB; if the 62 days are much bigger than about 100 MB,
     we will narrow the window.
  5. Run "Update sales dashboard". The build log shows "product sales for N stores: R rows from D day file(s)" for each login.
  6. Open Sales > Product Level.

LIMITS
  - Orders counts the distinct orders with the product in each group. A product sold in two categories counts an order
    once per category, so the product total can count an order twice in that case.
  - AOV for a product is its net divided by the orders that contain it, not the average order value of the store.
  - Charges and other differences are not products and are not in these lines.

TESTS (run from the repo root, where sync_sales.py exists)
  python tests/test_product_sales.py          (15)
  python tests/test_build_groups_product.py   (4)
  node tests/test_product_tab.js              (23)
  python tests/test_add_cancel_tab.py         (6)
