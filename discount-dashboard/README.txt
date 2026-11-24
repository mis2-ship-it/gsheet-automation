CATEGORY PERFORMANCE: SAME DAY IN EVERY MONTH

WHAT CHANGED
  The Category performance table compares like with like. Current month = 1st to the date chosen.
  Last month, last 2 months and last 3 months are cut at the same day of the month: for 23 Nov,
  1-23 Nov is compared with 1-23 Oct, 1-23 Sep and 1-23 Aug (before, they ran to the end of each month).
  The column spans now read "1-23 Oct" etc. The note above the table explains it.

FILES
  apps-script/Code.gs      paste into Code.gs, keep your DISCOUNT_SHEET_ID line
  apps-script/Index.html   paste into Index.html
  repo-root/write_discount_sheet.py   replace (repo root). Adds a Day column to Disc_Cat (the last column).
  tests/test_cat_day.js    optional check of the same-day cut

STEPS
  1. Paste Code.gs and Index.html into the Apps Script project. Deploy -> Manage deployments -> edit -> New version -> Deploy.
  2. Replace write_discount_sheet.py in the repo.
  3. Run "Discount Code, product and reconciliation rows (sales/page)" with from and to empty.
     This rewrites Disc_Cat with the Day column. Until it runs, older rows (with no day) are still cut at the end of each month.
  4. In the log, "category rows: N" shows the count. The table has about 30 times more category rows now
     (one row per day), so check the sheet is not near Google's 10 million cell limit.
  5. Check: Current Month Net Sales (All channels) equals the Product tab's Net revenue for the same month and filters.
