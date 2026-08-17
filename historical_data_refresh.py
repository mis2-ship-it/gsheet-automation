from __future__ import annotations

import calendar
from pathlib import Path

import pandas as pd
import requests


# =========================================================
# CONFIG
# =========================================================

OWNER = "mis2-ship-it"
REPO = "gsheet-automation"
BRANCH = "main"

MONTHLY_ROOT = "monthly_data"

OUTPUT_DIR = Path(
    "historical_data"
)

OUTPUT_FILE = (
    OUTPUT_DIR
    / "historical_sales.csv.gz"
)


# =========================================================
# MONTHS
# =========================================================

def get_months():

    months = []

    # 2025 Jan-Dec
    for month in range(1, 13):

        months.append(
            (2025, month)
        )

    # 2026 Jan-Aug
    for month in range(1, 9):

        months.append(
            (2026, month)
        )

    return months


# =========================================================
# GITHUB URL
# =========================================================

def build_url(
    year,
    month
):

    month_name = (
        calendar.month_abbr[
            month
        ]
    )

    filename = (
        f"MTD_{month_name}_{str(year)[-2:]}.csv"
    )

    return (
        f"https://raw.githubusercontent.com/"
        f"{OWNER}/{REPO}/{BRANCH}/"
        f"{MONTHLY_ROOT}/{year}/{filename}"
    )


# =========================================================
# DOWNLOAD ONE FILE
# =========================================================

def download_month(
    year,
    month
):

    url = build_url(
        year,
        month
    )

    print("=" * 60)
    print(
        f"📥 Loading {year}-{month:02d}"
    )
    print(
        url
    )

    response = requests.get(
        url,
        timeout=120
    )

    if response.status_code == 404:

        print(
            f"⚠️ File not found: "
            f"{year}-{month:02d}"
        )

        return None

    response.raise_for_status()

    temp_file = (
        OUTPUT_DIR
        / (
            f"_tmp_"
            f"{year}_"
            f"{month:02d}.csv"
        )
    )

    temp_file.write_bytes(
        response.content
    )

    df = pd.read_csv(
        temp_file
    )

    temp_file.unlink(
        missing_ok=True
    )

    print(
        f"✅ Rows: {len(df):,}"
    )

    return df


# =========================================================
# BUILD DATASET
# =========================================================

def build_dataset():

    OUTPUT_DIR.mkdir(
        parents=True,
        exist_ok=True
    )

    parts = []

    for year, month in get_months():

        try:

            df = download_month(
                year,
                month
            )

            if df is not None and not df.empty:

                df["_history_year"] = (
                    year
                )

                df["_history_month"] = (
                    month
                )

                parts.append(
                    df
                )

        except Exception as e:

            print(
                f"❌ Failed "
                f"{year}-{month:02d}: "
                f"{e}"
            )

    if not parts:

        raise RuntimeError(
            "No historical files were loaded."
        )

    print("=" * 60)
    print(
        "📊 COMBINING HISTORICAL DATA"
    )
    print("=" * 60)

    combined = pd.concat(
        parts,
        ignore_index=True,
        sort=False
    )

    # Remove exact duplicate rows.
    combined = (
        combined
        .drop_duplicates()
        .reset_index(
            drop=True
        )
    )

    print(
        f"✅ Combined rows: "
        f"{len(combined):,}"
    )

    print(
        f"✅ Columns: "
        f"{len(combined.columns)}"
    )

    # -----------------------------------------------------
    # OUTPUT
    # -----------------------------------------------------

    combined.to_csv(
        OUTPUT_FILE,
        index=False,
        compression="gzip"
    )

    print("=" * 60)
    print(
        "✅ HISTORICAL DATA READY"
    )
    print(
        "Output:",
        OUTPUT_FILE
    )
    print(
        "Rows:",
        f"{len(combined):,}"
    )
    print(
        "Size:",
        f"{OUTPUT_FILE.stat().st_size / 1024 / 1024:.2f} MB"
    )
    print("=" * 60)


# =========================================================
# MAIN
# =========================================================

if __name__ == "__main__":

    build_dataset()
