#!/usr/bin/env python3
"""
Historical discount data. Pulls the discount rows one month at a time (Swiggy and Zomato, COCO), archives each month
as soon as it is complete, and stops safely before the job's time limit. Run it again with the same dates and it
carries on from the first month that is not archived yet.

  python backfill_discount_history.py --from 2025-11-01 --to 2026-08-31 [--budget-minutes 330] [--commit]

--from / --to     first and last day to cover; each month in that range is pulled whole
--budget-minutes  no new month is started after this many minutes (keep it below the job's time limit)
--commit          commit and push each newly archived month (the job needs contents: write)
--workers         parallel branch requests per day (passed to sync_discounts.py)
Months that have not ended yet (plus the archive grace period) are left for the nightly job.
"""
import argparse
import json
import os
import subprocess
import sys
import time
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import write_discount_sheet as wd  # noqa: E402  (the same archive code the nightly job uses)


def month_list(first, last):
    """'YYYY-MM' months from the month of first to the month of last, inclusive."""
    out = []
    y, m = first.year, first.month
    while (y, m) <= (last.year, last.month):
        out.append(f"{y:04d}-{m:02d}")
        m += 1
        if m == 13:
            y, m = y + 1, 1
    return out


def month_days(month):
    """First and last day of a 'YYYY-MM' month, as 'YYYY-MM-DD'."""
    y, m = map(int, month.split("-"))
    return f"{month}-01", f"{month}-{wd.calendar.monthrange(y, m)[1]:02d}"


def archived_months(archive_dir):
    path = os.path.join(archive_dir, "index.json")
    if not os.path.exists(path):
        return set()
    with open(path, encoding="utf-8") as f:
        return set(json.load(f).get("months", []))


def pull_month(month, rows_path, workers):
    """Discount rows for one month into the rows file (products and reconciliation are not pulled here)."""
    first, last = month_days(month)
    cmd = [sys.executable, os.path.join(HERE, "sync_discounts.py"), "--out", rows_path,
           "--products-out", "", "--recon-out", "", "--from", first, "--to", last, "--workers", str(workers)]
    return subprocess.run(cmd, cwd=HERE).returncode


def archive_from_rows(rows_path, archive_dir, today):
    rows = wd.read_csv_if_present(rows_path)
    if rows is None:
        return set()
    rows = wd.keep_store(rows, wd.DEFAULT_STORE_TYPE, "discount rows")
    return set(wd.archive_closed_months(rows, archive_dir, today))


def commit_and_push(month, archive_dir):
    """Commits the archive folder and pushes it to the branch the job runs on."""
    branch = os.environ.get("GITHUB_REF_NAME", "main")
    subprocess.run(["git", "add", archive_dir], check=True)
    if subprocess.run(["git", "diff", "--cached", "--quiet"]).returncode == 0:
        return
    subprocess.run(["git", "-c", "user.name=github-actions[bot]",
                    "-c", "user.email=41898282+github-actions[bot]@users.noreply.github.com",
                    "commit", "-m", f"Discount archive: {month}"], check=True)
    subprocess.run(["git", "pull", "--rebase", "origin", branch], check=True)
    subprocess.run(["git", "push", "origin", f"HEAD:{branch}"], check=True)


def run_backfill(first, last, rows_path, archive_dir, budget_minutes, commit, workers, today,
                 sync_fn=pull_month, archive_fn=archive_from_rows, commit_fn=commit_and_push, clock=time.time):
    started = clock()
    months = month_list(first, last)
    print(f"Months {months[0]} to {months[-1]}: {len(months)} in the range")
    for month in months:
        if month in archived_months(archive_dir):
            print(f"{month}: already archived, skipped")
            continue
        if wd.month_end(month) + timedelta(days=wd.ARCHIVE_GRACE_DAYS) >= today:
            print(f"{month}: not closed yet, the nightly job archives it")
            continue
        if (clock() - started) / 60 >= budget_minutes:
            print(f"Time budget reached before {month}. Run again with the same dates to continue.")
            return 0
        code = sync_fn(month, rows_path, workers)
        if code != 0:
            print(f"ERROR: pulling {month} failed (exit {code}). Nothing archived for it. Run again to retry.")
            return 1
        before = archived_months(archive_dir)
        after = archive_fn(rows_path, archive_dir, today)
        if month in after and month not in before:
            print(f"{month}: archived")
            if commit:
                commit_fn(month, archive_dir)
        else:
            print(f"{month}: pulled, but not archived (a day has no rows; see the message above)")
    missing = [m for m in months
               if m not in archived_months(archive_dir) and wd.month_end(m) + timedelta(days=wd.ARCHIVE_GRACE_DAYS) < today]
    print("Closed months not archived yet:", ", ".join(missing) if missing else "none")
    return 0


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--from", dest="start", required=True, help="first day, YYYY-MM-DD (its month is included)")
    ap.add_argument("--to", dest="end", required=True, help="last day, YYYY-MM-DD (its month is included)")
    ap.add_argument("--rows", default="data/discount_rows.csv")
    ap.add_argument("--archive-dir", default=wd.ARCHIVE_DIR)
    ap.add_argument("--budget-minutes", type=float, default=330)
    ap.add_argument("--workers", type=int, default=4)
    ap.add_argument("--commit", action="store_true")
    args = ap.parse_args()
    first = datetime.strptime(args.start, "%Y-%m-%d").date()
    last = datetime.strptime(args.end, "%Y-%m-%d").date()
    today = (datetime.now(ZoneInfo("Asia/Kolkata")) - timedelta(hours=5)).date()
    code = run_backfill(first, last, args.rows, args.archive_dir, args.budget_minutes, args.commit,
                        args.workers, today)
    sys.exit(code)


if __name__ == "__main__":
    main()
