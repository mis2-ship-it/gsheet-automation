DASHBOARD BUILD ERROR FIX: build_groups.py and the metrics cut

WHAT WAS WRONG
  The workflow passes --discounts to build_groups.py. The build_groups.py that was sent earlier did not accept it,
  so the build stopped with "unrecognized arguments: --discounts".

WHAT CHANGED
  build_groups.py is now based on the version that takes --discounts, and it also:
    - copies data/metrics.json.gz into each login's folder; a store-limited login gets only its stores' rows
    - gives data/product_mix.json.gz to the all-stores login only
  update-sales-dashboard.yml already passes --discounts and the two data files.

FILES
  build_groups.py                                 repo root (replaces the current one)
  .github/workflows/update-sales-dashboard.yml    replaces the current workflow
  tests/test_build_groups_run.py                  runs the build with the workflow's arguments (8 checks)
  tests/test_build_groups_metrics.py              the metrics cut for each login (10 checks)
  tests/fixtures/metrics.json.gz                  test data (keep it out of git)

STEPS
  1. Replace build_groups.py and the workflow file.
  2. Run "Update sales dashboard" (Actions > Run workflow).
  3. In the build log, check the lines "metrics for N stores: R rows", one per login.
