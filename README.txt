CANCELLATIONS: PRODUCTS (ITEM NAME AND CATEGORY) ON THE CANCELLATION TAB

WHAT CHANGED
  cancel_sales.py     writes cancel_items_<from>_<to>.csv: one row per item line of each cancelled order, with
                      Item Name, Category, Qty, Item Gross and Item Net, and the order's reason, bucket and branch.
  merge_cancel.py     can key rows by line (--key "Invoice No,Branch,Date,Line"), so each item line is kept.
  cancel.yml          merges the item lines into data/cancel_items.csv.gz and commits it with the orders file.
  build_groups.py     copies each login's item lines (cut to its stores) into the login folder.
  update-sales-dashboard.yml   passes the item file to the build.
  site/cancel.js      the Cancellation tab: "Products on cancellation" under the table. Grouped by category; click a
                      category for its items. Columns: Cancelled orders | Cancel (RDC) | Other | Cancelled net sales.
                      The day, the channel and the page's filters apply, as in the table above it.

IMPORTANT: EARLIER PERIODS HAVE NO ITEM LINES
  The item lines exist only for cancel runs made with this version. Run the Cancel workflow again for the periods you
  need (for example 1 April 2025 to 30 April 2025). Re-running a period adds its item lines and replaces nothing else.

STEPS
  1. Replace cancel_sales.py, merge_cancel.py and build_groups.py; replace the two workflow files; replace site/cancel.js.
  2. Run the Cancel workflow for a period (probe unticked). The step "Add the cancelled orders to the stored file" runs
     twice: once for the orders, once for the item lines.
  3. Run "Update sales dashboard".
  4. Sales > Cancellation: the day chosen on the Daily tab; scroll to "Products on cancellation".

LIMITS
  - Products show the cancelled orders only. "Which products are down on cancellation" is read from these counts.
    A cancel rate per product also needs the closed sales per product, which the dashboard does not store yet.
  - Item Net is the item's base net amount from the Sales Page report, in rupees.

TESTS (run from the repo root, where site/index.html and sync_sales.py exist)
  python tests/test_cancel_items.py        (7)
  python tests/test_cancel_nested.py       (10)
  python tests/test_cancel_sales.py        (22)
  python tests/test_cancel_speed.py        (10)
  python tests/test_cancel_probe.py        (7)
  node tests/test_cancel_products.js       (10)
  node tests/test_cancel_tab.js            (19)
