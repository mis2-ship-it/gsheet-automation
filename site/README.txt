MORNING CALL ON YESTERDAY, AND THE PRODUCT MIX AND METRICS LOAD ERRORS

WHAT CHANGED
  The Morning Call reference day is yesterday: the last completed business day (a business day starts at 05:00 IST).
  Today's figures are still being collected, which is why most of the Morning Call was blank.
  The product mix uses the same reference day.
  The product mix shows the same clear message as Metrics, and has a "Load the product mix file" control.

WHY "Failed to fetch" APPEARS (Metrics and Product mix)
  The page asks the server for data/metrics.json.gz and data/product_mix.json.gz. Neither file is published for
  this login yet, so the browser gets no response. Until the sales workflow publishes them, load the file on the page:
    Metrics:      Sales > Metrics > "Load the metrics file" > choose metrics.json.gz
    Product mix:  Sales > Morning Call > Product mix > "Load the product mix file" > choose product_mix.json.gz
  The product mix file is made from data/product_rows.csv (the discount job's product rows), for example:
    python build_product_mix.py --rows data/product_rows.csv --out product_mix.json.gz

FILES
  site/morning.js        replaces the current morning.js
  site/productmix.js     replaces the current productmix.js
  site/insights.js       unchanged (included so the tests run from one folder)
  tests/                 test_mc_metrics.js, test_source.js, test_mc_refday.js, test_pm.js, test_pm_load.js
  tests/fixtures/        synthetic data only

RUN THE TESTS (from the repo root)
  node tests/test_mc_metrics.js   30 passed
  node tests/test_source.js       17 passed
  node tests/test_mc_refday.js     6 passed
  node tests/test_pm.js           11 passed
  node tests/test_pm_load.js       6 passed
