METRICS: CLEAR LOAD ERRORS, AND LOADING THE FILE FROM YOUR COMPUTER

WHY "Failed to fetch" APPEARED
  The page asks the server for data/metrics.json.gz. The browser never got an answer, which happens when the
  request is sent to the login page or the file is not published for this login. Metrics data is not published
  yet, because the sales workflow does not copy it into the login data folders yet.

WHAT CHANGED
  site/metrics.js          the error now says what happened and what to do
  The Metrics tab has a "Load the metrics file" control, so the file can be loaded straight from your computer.
  Use metrics.json.gz (produced by build_metrics.py from the workbook). Nothing needs publishing for this to work.

FILES
  site/metrics.js             replaces the current metrics.js
  tests/test_metrics_load.js  checks the two failure messages and loading a file

TESTS
  node tests/test_metrics_load.js   expected: 9 passed, 0 failed
  The earlier test_metrics.js still passes (29 of 29).

Only data files from your own workbook should be loaded; do not commit metrics.json.gz to git.
