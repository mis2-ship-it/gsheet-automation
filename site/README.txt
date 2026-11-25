MORNING CALL: STORES BY AREA MANAGER, GROWTH METRIC FILTER, EXCEL GROUPING

WHAT CHANGED (Morning Call only)
  Stores by area manager: each area manager's row, with its stores listed underneath it on the page.
  In Excel the stores are grouped under the manager and collapsed, so the + expands them.
  Growth metric filter (Net Sales, Dis %, Orders, AOV) above the tables. It changes every Morning Call
  table on the page and the Excel file (the Excel file shows the metric selected when you download).
  Dis % growth is in points and a fall in discount is shown in green. Growth headers say (pts) in Dis % mode.
  The channel bar (All sources, Offline, Online) sits at the top, as before.

FILES (replace or add in the repo)
  site/morning.js          replaces the current morning.js
  site/insights.js         unchanged in this round (included so the tests run from one folder)
  tests/test_mc_metrics.js new: checks each metric, the area manager table and the Excel grouping
  tests/test_source.js     the earlier source and channel checks, now with repo-relative paths
  tests/fixtures/          synthetic data only (no company figures)

RUN THE TESTS (from the repo root)
  node tests/test_mc_metrics.js     expected: 30 passed, 0 failed
  node tests/test_source.js         expected: 17 passed, 0 failed

NOT YET
  The metric filter is on the Morning Call only. The Sales tabs (Daily, Weekly, Monthly) keep Net Sales.
  The Product mix table keeps Net Sales (its mix share only makes sense for Net Sales and Orders).
