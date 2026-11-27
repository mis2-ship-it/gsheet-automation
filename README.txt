CANCELLATION TAB: NOT SHOWING ON THE DASHBOARD

WHY IT IS MISSING (check in this order)
  1. The page does not load cancel.js. The tab is added by that script, so without the script tag there is no tab.
     Fix: run   python add_cancel_tab.py site/index.html    (adds <script src="cancel.js"></script> before </body>; safe to run twice)
  2. site/cancel.js is not in the site folder. Copy cancel.js into site/ (the site folder is what is deployed).
  3. The stored file is not in the repository yet. The tab reads data/cancel_orders.csv.gz. Run the Cancel workflow
     once by hand (Actions > Cancellation pull > Run workflow), then check that data/cancel_orders.csv.gz is in the repo.
  4. The login's data was built before the file existed. The cancellation file is copied into each login by the dashboard
     build, so run "Update sales dashboard" once after step 3. The build log then shows "cancel orders for N stores".
     The Cancel workflow does not start the dashboard build itself.

CHECK IN THE BROWSER (after the deploy)
  Open the dashboard, press F12, and in the Console type:
     typeof FB_CANCEL                -> "object" means cancel.js is loaded ("undefined" means step 1 or 2 is missing)
     SUBS.sales.map(x => x[0])       -> includes "cancel" means the tab is registered
  If the tab is registered but the page says the cancelled orders could not be read, step 3 or 4 is missing.

FILES
  add_cancel_tab.py                   run once, from the repo root
  tests/test_add_cancel_tab.py        (4)   python tests/test_add_cancel_tab.py
