historical_sales.py

Historical sales engine for AI MIS WhatsApp.

Source:
    GitHub repository:
    https://github.com/mis2-ship-it/gsheet-automation

Expected monthly files:
    monthly_data/2025/MTD_Jan_25.csv
    ...
    monthly_data/2025/MTD_Dec_25.csv
    monthly_data/2026/MTD_Jan_26.csv
    ...
    monthly_data/2026/MTD_Aug_26.csv

The engine downloads only the monthly CSVs needed for a query, caches them
locally, normalizes column names, and exposes simple functions for:
    - last N months performance
    - store performance
    - brand performance
    - region performance
    - period comparison
    - seasonality
    - best / worst month
    - growth and trend summaries

It is designed to be imported by whatsapp_webhook.py.

Notes:
    1. The GitHub repository is public, so no GitHub token is required.
    2. The code deliberately detects column names instead of assuming one exact
       capitalization/spacing.
    3. If the historical CSV schema changes, update COLUMN_ALIASES below.
"""

from __future__ import annotations

import calendar
import io
import os
import re
import time
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Iterable, Optional

import pandas as pd
import requests


# =========================================================
# CONFIGURATION
# =========================================================

GITHUB_OWNER = os.getenv(
    "HISTORICAL_GITHUB_OWNER",
    "mis2-ship-it",
)

GITHUB_REPO = os.getenv(
    "HISTORICAL_GITHUB_REPO",
    "gsheet-automation",
)

GITHUB_BRANCH = os.getenv(
    "HISTORICAL_GITHUB_BRANCH",
    "main",
)

MONTHLY_ROOT = os.getenv(
    "HISTORICAL_MONTHLY_ROOT",
    "monthly_data",
)

CACHE_DIR = Path(
    os.getenv(
        "HISTORICAL_CACHE_DIR",
        ".historical_cache",
    )
)

CACHE_TTL_SECONDS = int(
    os.getenv(
        "HISTORICAL_CACHE_TTL_SECONDS",
        "1800",
    )
)

REQUEST_TIMEOUT = int(
    os.getenv(
        "HISTORICAL_REQUEST_TIMEOUT",
        "45",
    )
)

HTTP_SESSION = requests.Session()
HTTP_SESSION.headers.update(
    {
        "User-Agent": "AI-MIS-Historical-Sales/1.0",
        "Accept": "text/csv,application/json;q=0.9,*/*;q=0.8",
    }
)


# =========================================================
# COLUMN ALIASES
# =========================================================

# The monthly files in the repository may use slightly different names.
# Keep the canonical names on the left and accepted variants on the right.
COLUMN_ALIASES: dict[str, list[str]] = {
    "date": [
        "date",
        "business date",
        "business_date",
        "invoice date",
        "invoice_date",
        "invoice day",
        "invoice_day",
        "day",
    ],
    "store": [
        "store",
        "store name",
        "store_name",
        "branch",
        "branch name",
        "branch_name",
        "outlet",
        "outlet name",
        "outlet_name",
    ],
    "region": [
        "region",
        "state",
        "area",
        "zone",
    ],
    "brand": [
        "brand",
        "brand name",
        "brand_name",
    ],
    "net": [
        "net",
        "net sales",
        "net revenue",
        "net_sales",
        "net_revenue",
        "net amount",
        "net_amount",
    ],
    "gross": [
        "gross",
        "gross sales",
        "gross revenue",
        "gross_sales",
        "gross_revenue",
        "gross amount",
        "gross_amount",
    ],
    "discount": [
        "discount",
        "discount amount",
        "discount_amount",
        "discount %",
        "discount_percent",
        "discount percentage",
    ],
    "transactions": [
        "transactions",
        "transaction",
        "orders",
        "order count",
        "order_count",
        "txn",
        "txns",
    ],
    "quantity": [
        "quantity",
        "qty",
        "qty sold",
        "qty_sold",
        "units",
        "items",
    ],
    "aov": [
        "aov",
        "average order value",
        "average_order_value",
    ],
}


# =========================================================
# DATA CLASS
# =========================================================

@dataclass
class HistoricalSummary:
    scope: str
    label: str
    start_date: date
    end_date: date
    gross: float
    net: float
    discount: float
    transactions: float
    quantity: float

    @property
    def aov(self) -> float:
        if self.transactions:
            return self.net / self.transactions
        return 0.0


# =========================================================
# GENERIC HELPERS
# =========================================================

def _normalize_column(value: object) -> str:
    text = str(value).strip().lower()
    text = re.sub(r"[^a-z0-9]+", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def _normalize_text(value: object) -> str:
    text = str(value or "").strip().lower()
    text = re.sub(r"[^a-z0-9]+", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def _safe_float(value: object) -> float:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return 0.0

    try:
        text = str(value).strip()
        text = text.replace(",", "")
        text = text.replace("₹", "")
        text = text.replace("%", "")
        text = text.replace("L", "")
        text = text.replace("K", "")
        return float(text)
    except (TypeError, ValueError):
        return 0.0


def _growth(current: float, previous: float) -> float:
    if previous in (0, None):
        if current > 0:
            return 100.0
        return 0.0
    return ((current - previous) / previous) * 100.0


def _month_end(year: int, month: int) -> date:
    return date(
        year,
        month,
        calendar.monthrange(year, month)[1],
    )


def _month_start(year: int, month: int) -> date:
    return date(year, month, 1)


def _month_range(
    end: Optional[date] = None,
    months: int = 6,
) -> list[tuple[int, int]]:
    if months <= 0:
        raise ValueError("months must be greater than zero")

    end_date = end or date.today()
    result: list[tuple[int, int]] = []

    year = end_date.year
    month = end_date.month

    for _ in range(months):
        result.append((year, month))
        month -= 1
        if month == 0:
            month = 12
            year -= 1

    return list(reversed(result))


def _cache_path(year: int, month: int) -> Path:
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    return CACHE_DIR / f"MTD_{calendar.month_abbr[month]}_{str(year)[-2:]}.csv"


def _raw_url(year: int, month: int) -> str:
    filename = f"MTD_{calendar.month_abbr[month]}_{str(year)[-2:]}.csv"
    return (
        f"https://raw.githubusercontent.com/"
        f"{GITHUB_OWNER}/{GITHUB_REPO}/{GITHUB_BRANCH}/"
        f"{MONTHLY_ROOT}/{year}/{filename}"
    )


def _csv_exists_on_github(year: int, month: int) -> bool:
    url = _raw_url(year, month)
    try:
        response = HTTP_SESSION.head(
            url,
            timeout=REQUEST_TIMEOUT,
            allow_redirects=True,
        )
        return response.status_code == 200
    except requests.RequestException:
        return False


# =========================================================
# LOAD MONTHLY CSV
# =========================================================

def load_monthly_data(
    year: int,
    month: int,
    force_refresh: bool = False,
) -> pd.DataFrame:
    """
    Load one monthly MTD CSV from GitHub.

    Returns a normalized DataFrame with canonical columns where available.
    """
    if not 1 <= month <= 12:
        raise ValueError("month must be between 1 and 12")

    cache_file = _cache_path(year, month)

    use_cache = (
        cache_file.exists()
        and not force_refresh
        and (
            time.time() - cache_file.stat().st_mtime
            < CACHE_TTL_SECONDS
        )
    )

    if use_cache:
        try:
            df = pd.read_csv(cache_file)
            return normalize_dataframe(df)
        except Exception:
            # Corrupt cache -> re-download.
            pass

    url = _raw_url(year, month)

    response = HTTP_SESSION.get(
        url,
        timeout=REQUEST_TIMEOUT,
    )

    if response.status_code == 404:
        raise FileNotFoundError(
            f"Historical file not found: {url}"
        )

    response.raise_for_status()

    cache_file.write_bytes(response.content)

    df = pd.read_csv(
        io.BytesIO(response.content)
    )

    return normalize_dataframe(df)


# =========================================================
# NORMALIZE DATAFRAME
# =========================================================

def resolve_column(
    columns: Iterable[object],
    canonical_name: str,
) -> Optional[str]:
    normalized_columns = {
        _normalize_column(col): str(col)
        for col in columns
    }

    aliases = COLUMN_ALIASES.get(
        canonical_name,
        [],
    )

    normalized_aliases = [
        _normalize_column(alias)
        for alias in aliases
    ]

    # Exact normalized match first.
    for alias in normalized_aliases:
        if alias in normalized_columns:
            return normalized_columns[alias]

    # Conservative partial match second.
    for alias in normalized_aliases:
        for normalized_col, original_col in normalized_columns.items():
            if (
                normalized_col.startswith(alias)
                or alias.startswith(normalized_col)
            ):
                return original_col

    return None


def normalize_dataframe(
    df: pd.DataFrame,
) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame()

    result = df.copy()

    rename_map: dict[str, str] = {}

    for canonical_name in COLUMN_ALIASES:
        resolved = resolve_column(
            result.columns,
            canonical_name,
        )

        if resolved and resolved != canonical_name:
            rename_map[resolved] = canonical_name

    if rename_map:
        result = result.rename(
            columns=rename_map
        )

    # Date normalization.
    if "date" in result.columns:
        result["date"] = pd.to_datetime(
            result["date"],
            errors="coerce",
            dayfirst=False,
        ).dt.date

    # Numeric normalization.
    for col in [
        "net",
        "gross",
        "discount",
        "transactions",
        "quantity",
        "aov",
    ]:
        if col in result.columns:
            result[col] = pd.to_numeric(
                result[col]
                .astype(str)
                .str.replace(",", "", regex=False)
                .str.replace("₹", "", regex=False)
                .str.replace("%", "", regex=False),
                errors="coerce",
            ).fillna(0.0)

    # Ensure dimensions exist so aggregation code stays simple.
    for col in [
        "store",
        "region",
        "brand",
    ]:
        if col not in result.columns:
            result[col] = ""

    for col in [
        "net",
        "gross",
        "discount",
        "transactions",
        "quantity",
    ]:
        if col not in result.columns:
            result[col] = 0.0

    result["store"] = (
        result["store"]
        .fillna("")
        .astype(str)
        .str.strip()
    )

    result["region"] = (
        result["region"]
        .fillna("")
        .astype(str)
        .str.strip()
    )

    result["brand"] = (
        result["brand"]
        .fillna("")
        .astype(str)
        .str.strip()
    )

    return result


# =========================================================
# LOAD MULTIPLE MONTHS
# =========================================================

def load_months(
    months: int = 6,
    end: Optional[date] = None,
    force_refresh: bool = False,
) -> pd.DataFrame:
    parts: list[pd.DataFrame] = []

    requested = _month_range(
        end=end,
        months=months,
    )

    for year, month in requested:
        try:
            df = load_monthly_data(
                year,
                month,
                force_refresh=force_refresh,
            )

            if not df.empty:
                parts.append(df)

        except FileNotFoundError:
            # Historical folders may not contain future/current months.
            continue

    if not parts:
        raise RuntimeError(
            "No historical monthly CSV files could be loaded."
        )

    combined = pd.concat(
        parts,
        ignore_index=True,
    )

    combined = combined.drop_duplicates()

    if "date" in combined.columns:
        combined = combined[
            combined["date"].notna()
        ].copy()

    return combined


# =========================================================
# FILTER HELPERS
# =========================================================

def _match_dimension(
    series: pd.Series,
    query: str,
) -> pd.Series:
    q = _normalize_text(query)

    if not q:
        return pd.Series(
            True,
            index=series.index,
        )

    normalized = (
        series
        .fillna("")
        .astype(str)
        .map(_normalize_text)
    )

    exact = normalized == q

    if exact.any():
        return exact

    return (
        normalized.str.contains(
            re.escape(q),
            na=False,
        )
        |
        normalized.map(
            lambda value:
                q in value
                or value in q
        )
    )


# =========================================================
# AGGREGATION
# =========================================================

def _aggregate(
    df: pd.DataFrame,
    group_by: Optional[str] = None,
) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame()

    numeric = [
        col
        for col in [
            "gross",
            "net",
            "discount",
            "transactions",
            "quantity",
        ]
        if col in df.columns
    ]

    if group_by:
        grouped = (
            df.groupby(
                group_by,
                dropna=False,
            )[numeric]
            .sum()
            .reset_index()
        )
    else:
        grouped = pd.DataFrame(
            [
                {
                    col: float(df[col].sum())
                    for col in numeric
                }
            ]
        )

    if "net" in grouped.columns and "transactions" in grouped.columns:
        grouped["aov"] = (
            grouped["net"]
            /
            grouped["transactions"].replace(
                0,
                pd.NA,
            )
        ).fillna(0.0)

    return grouped


# =========================================================
# MONTHLY SUMMARY
# =========================================================

def monthly_summary(
    months: int = 6,
    end: Optional[date] = None,
    force_refresh: bool = False,
) -> pd.DataFrame:
    df = load_months(
        months=months,
        end=end,
        force_refresh=force_refresh,
    )

    if df.empty:
        return pd.DataFrame()

    df["month"] = pd.to_datetime(
        df["date"]
    ).dt.to_period("M").astype(str)

    monthly = _aggregate(
        df,
        group_by="month",
    )

    monthly = monthly.sort_values(
        "month"
    ).reset_index(
        drop=True
    )

    if "net" in monthly.columns:
        monthly["growth_pct"] = (
            monthly["net"].pct_change() * 100
        ).fillna(0.0)

    return monthly


# =========================================================
# LAST N MONTHS PERFORMANCE
# =========================================================

def get_last_n_months_performance(
    months: int = 6,
    end: Optional[date] = None,
) -> dict:
    monthly = monthly_summary(
        months=months,
        end=end,
    )

    if monthly.empty:
        return {
            "months": [],
            "total_net": 0.0,
            "average_monthly_net": 0.0,
            "best_month": None,
            "worst_month": None,
            "growth_pct": 0.0,
        }

    total_net = float(
        monthly["net"].sum()
    )

    avg_net = float(
        monthly["net"].mean()
    )

    best_row = monthly.loc[
        monthly["net"].idxmax()
    ]

    worst_row = monthly.loc[
        monthly["net"].idxmin()
    ]

    growth = 0.0

    if len(monthly) >= 2:
        first = float(
            monthly.iloc[0]["net"]
        )
        last = float(
            monthly.iloc[-1]["net"]
        )
        growth = _growth(
            last,
            first,
        )

    return {
        "months": monthly.to_dict(
            orient="records"
        ),
        "total_net": total_net,
        "average_monthly_net": avg_net,
        "best_month": {
            "month": best_row["month"],
            "net": float(best_row["net"]),
        },
        "worst_month": {
            "month": worst_row["month"],
            "net": float(worst_row["net"]),
        },
        "growth_pct": growth,
    }


# =========================================================
# DIMENSION PERFORMANCE
# =========================================================

def get_dimension_performance(
    dimension: str,
    value: str,
    months: int = 6,
    end: Optional[date] = None,
) -> pd.DataFrame:
    if dimension not in {
        "store",
        "region",
        "brand",
    }:
        raise ValueError(
            "dimension must be store, region, or brand"
        )

    df = load_months(
        months=months,
        end=end,
    )

    if df.empty:
        return pd.DataFrame()

    mask = _match_dimension(
        df[dimension],
        value,
    )

    filtered = df.loc[
        mask
    ].copy()

    if filtered.empty:
        return pd.DataFrame()

    filtered["month"] = pd.to_datetime(
        filtered["date"]
    ).dt.to_period("M").astype(str)

    result = _aggregate(
        filtered,
        group_by="month",
    ).sort_values(
        "month"
    ).reset_index(
        drop=True
    )

    if "net" in result.columns:
        result["growth_pct"] = (
            result["net"].pct_change() * 100
        ).fillna(0.0)

    return result


def get_store_performance(
    store_name: str,
    months: int = 6,
    end: Optional[date] = None,
) -> pd.DataFrame:
    return get_dimension_performance(
        "store",
        store_name,
        months=months,
        end=end,
    )


def get_brand_performance(
    brand_name: str,
    months: int = 6,
    end: Optional[date] = None,
) -> pd.DataFrame:
    return get_dimension_performance(
        "brand",
        brand_name,
        months=months,
        end=end,
    )


def get_region_performance(
    region_name: str,
    months: int = 6,
    end: Optional[date] = None,
) -> pd.DataFrame:
    return get_dimension_performance(
        "region",
        region_name,
        months=months,
        end=end,
    )


# =========================================================
# COMPARISON
# =========================================================

def compare_periods(
    dimension: Optional[str] = None,
    value: Optional[str] = None,
    current_months_ago: int = 0,
    previous_months_ago: int = 1,
    end: Optional[date] = None,
) -> dict:
    """
    Compare two calendar months.

    current_months_ago=0 -> latest month
    current_months_ago=1 -> previous month
    """
    end_date = end or date.today()

    current_year = end_date.year
    current_month = end_date.month

    def shift_month(
        year: int,
        month: int,
        offset: int,
    ) -> tuple[int, int]:
        index = year * 12 + (month - 1) - offset
        return (
            index // 12,
            (index % 12) + 1,
        )

    cy, cm = shift_month(
        current_year,
        current_month,
        current_months_ago,
    )

    py, pm = shift_month(
        current_year,
        current_month,
        previous_months_ago,
    )

    needed_months = (
        max(
            current_months_ago,
            previous_months_ago,
        )
        + 1
    )

    df = load_months(
        months=needed_months,
        end=end_date,
    )

    current_key = f"{cy:04d}-{cm:02d}"
    previous_key = f"{py:04d}-{pm:02d}"

    df["month"] = pd.to_datetime(
        df["date"]
    ).dt.to_period("M").astype(str)

    if dimension and value:
        df = df.loc[
            _match_dimension(
                df[dimension],
                value,
            )
        ].copy()

    current_net = float(
        df.loc[
            df["month"] == current_key,
            "net",
        ].sum()
    )

    previous_net = float(
        df.loc[
            df["month"] == previous_key,
            "net",
        ].sum()
    )

    return {
        "current_month": current_key,
        "previous_month": previous_key,
        "current_net": current_net,
        "previous_net": previous_net,
        "growth_pct": _growth(
            current_net,
            previous_net,
        ),
    }


# =========================================================
# SEASONALITY
# =========================================================

def get_seasonality(
    dimension: Optional[str] = None,
    value: Optional[str] = None,
    years: int = 2,
    end: Optional[date] = None,
) -> pd.DataFrame:
    """
    Returns average net sales by calendar month.

    Example:
        Jan average across available years
        Feb average across available years
        ...
    """
    end_date = end or date.today()

    df = load_months(
        months=years * 12,
        end=end_date,
    )

    if df.empty:
        return pd.DataFrame()

    if dimension and value:
        df = df.loc[
            _match_dimension(
                df[dimension],
                value,
            )
        ].copy()

    df["month_no"] = pd.to_datetime(
        df["date"]
    ).dt.month

    seasonal = (
        df.groupby(
            "month_no"
        )["net"]
        .agg(
            total_net="sum",
            average_net="mean",
            observations="count",
        )
        .reset_index()
    )

    seasonal["month"] = seasonal[
        "month_no"
    ].map(
        lambda x: calendar.month_abbr[int(x)]
    )

    seasonal["index_vs_average"] = (
        seasonal["average_net"]
        /
        max(
            seasonal["average_net"].mean(),
            1,
        )
        * 100
    )

    return seasonal.sort_values(
        "month_no"
    ).reset_index(
        drop=True
    )


# =========================================================
# BEST / WORST PERFORMANCE
# =========================================================

def get_best_worst(
    dimension: str,
    months: int = 6,
    end: Optional[date] = None,
    top_n: int = 5,
) -> dict:
    if dimension not in {
        "store",
        "region",
        "brand",
    }:
        raise ValueError(
            "dimension must be store, region, or brand"
        )

    df = load_months(
        months=months,
        end=end,
    )

    grouped = _aggregate(
        df,
        group_by=dimension,
    )

    if grouped.empty:
        return {
            "best": [],
            "worst": [],
        }

    best = (
        grouped.sort_values(
            "net",
            ascending=False,
        )
        .head(top_n)
        .to_dict(
            orient="records"
        )
    )

    worst = (
        grouped.sort_values(
            "net",
            ascending=True,
        )
        .head(top_n)
        .to_dict(
            orient="records"
        )
    )

    return {
        "best": best,
        "worst": worst,
    }


# =========================================================
# SIMPLE MANAGEMENT INSIGHT
# =========================================================

def build_management_summary(
    dimension: Optional[str] = None,
    value: Optional[str] = None,
    months: int = 6,
    end: Optional[date] = None,
) -> dict:
    """
    Produces structured information for WhatsApp response generation.
    """
    performance = get_last_n_months_performance(
        months=months,
        end=end,
    )

    result = {
        "scope": "Overall",
        "months": months,
        "total_net": performance["total_net"],
        "average_monthly_net":
            performance["average_monthly_net"],
        "best_month":
            performance["best_month"],
        "worst_month":
            performance["worst_month"],
        "period_growth_pct":
            performance["growth_pct"],
        "monthly": performance["months"],
    }

    if dimension and value:
        result["scope"] = f"{dimension}: {value}"

        data = get_dimension_performance(
            dimension,
            value,
            months=months,
            end=end,
        )

        if not data.empty:
            result["monthly"] = data.to_dict(
                orient="records"
            )

            result["total_net"] = float(
                data["net"].sum()
            )

            result["average_monthly_net"] = float(
                data["net"].mean()
            )

            best_row = data.loc[
                data["net"].idxmax()
            ]

            worst_row = data.loc[
                data["net"].idxmin()
            ]

            result["best_month"] = {
                "month": best_row["month"],
                "net": float(best_row["net"]),
            }

            result["worst_month"] = {
                "month": worst_row["month"],
                "net": float(worst_row["net"]),
            }

            if len(data) >= 2:
                result["period_growth_pct"] = _growth(
                    float(data.iloc[-1]["net"]),
                    float(data.iloc[0]["net"]),
                )
            else:
                result["period_growth_pct"] = 0.0

    return result

# ----------------------
# HISTORICAL DIMENSION
# ----------------------

def extract_historical_dimension(
    message
):
    import re

    text = _normalize_text(
        message
    )

    dimension = None
    value = None

    # -----------------------------------------------------
    # BRAND
    # -----------------------------------------------------

    known_brands = [
        "frozen bottle",
        "madno",
        "boba bar",
        "lubov",
    ]

    for brand in known_brands:

        if brand in text:

            dimension = "brand"
            value = brand
            break

    # -----------------------------------------------------
    # REGION
    # -----------------------------------------------------

    regions = [
        "tn",
        "tamil nadu",
        "ka",
        "karnataka",
        "mh",
        "maharashtra",
        "kl",
        "kerala",
        "kerela",
    ]

    if dimension is None:

        for region in regions:

            if (
                text == region
                or
                f"{region} region" in text
                or
                f"{region} sales" in text
                or
                f"{region} region sales" in text
            ):

                dimension = "region"
                value = region
                break

    # -----------------------------------------------------
    # GENERIC PATTERNS
    # -----------------------------------------------------

    if dimension is None:

        patterns = [

            r"(.+?)\s+last\s+\d+\s+months?",
            r"(.+?)\s+performance",
            r"sales\s+of\s+(.+?)\s+last",
            r"(.+?)\s+trend",
        ]

        for pattern in patterns:

            match = re.search(
                pattern,
                text
            )

            if match:

                candidate = (
                    match.group(1)
                    .strip()
                )

                if candidate not in [
                    "sales",
                    "last",
                    "performance",
                ]:

                    dimension = "store"
                    value = candidate
                    break

    return (
        dimension,
        value
    )

# =========================================================
# NATURAL QUERY ROUTER
# =========================================================

def classify_historical_query(message):

    import re

    text = _normalize_text(
        message
    )

    if not text:

        return None

    months = 6

    month_match = re.search(
        r"(?:last\s+)?(\d+)\s+months?",
        text
    )

    if month_match:

        months = int(
            month_match.group(1)
        )

    elif (
        "last six months" in text
        or
        "six months" in text
    ):

        months = 6

    elif (
        "last twelve months" in text
        or
        "twelve months" in text
    ):

        months = 12

    elif (
        "last year" in text
    ):

        months = 12

    # --------------------------------------------------
    # SEASONALITY
    # --------------------------------------------------

    if any(
        x in text
        for x in [
            "seasonality",
            "seasonal",
            "season trend",
        ]
    ):

        return {
            "intent": "seasonality",
            "dimension": None,
            "value": None,
            "months": max(
                months,
                12
            ),
        }

    # --------------------------------------------------
    # BRAND
    # --------------------------------------------------

    brands = [
        "frozen bottle",
        "madno",
        "boba bar",
        "lubov",
    ]

    for brand in brands:

        if brand in text:

            return {
                "intent":
                    "historical_performance",
                "dimension":
                    "brand",
                "value":
                    brand,
                "months":
                    months,
            }

    # --------------------------------------------------
    # REGION
    # --------------------------------------------------

    regions = {
        "tn": "TN",
        "tamil nadu": "TN",
        "tamilnadu": "TN",

        "ka": "KA",
        "karnataka": "KA",

        "mh": "MH",
        "maharashtra": "MH",

        "kl": "KL",
        "kerala": "KL",
        "kerela": "KL",
    }

    for key, canonical in regions.items():

        if (
            text == key
            or
            text.startswith(
                key + " region"
            )
            or
            text.startswith(
                key + " sales"
            )
            or
            (
                "region" in text
                and key in text
            )
        ):

            return {
                "intent":
                    "historical_performance",
                "dimension":
                    "region",
                "value":
                    canonical,
                "months":
                    months,
            }

    # --------------------------------------------------
    # COMMON WORDS TO REMOVE
    # --------------------------------------------------

    cleaned = text

    for phrase in [
        "last six months",
        "last twelve months",
        "last 12 months",
        "last 6 months",
        "last 3 months",
        "last 2 months",
        "last month",
        "historical performance",
        "historical",
        "performance",
        "sales",
        "trend",
    ]:

        cleaned = cleaned.replace(
            phrase,
            ""
        )

    cleaned = re.sub(
        r"\s+",
        " ",
        cleaned
    ).strip()

    # --------------------------------------------------
    # STORE
    # --------------------------------------------------

    if cleaned:

        return {
            "intent":
                "historical_performance",
            "dimension":
                "store",
            "value":
                cleaned,
            "months":
                months,
        }

    # --------------------------------------------------
    # OVERALL
    # --------------------------------------------------

    return {
        "intent":
            "historical_performance",
        "dimension":
            None,
        "value":
            None,
        "months":
            months,
    }


# =========================================================
# FORMATTING HELPERS FOR WHATSAPP
# =========================================================

def format_lakh(value: float) -> str:
    return f"₹{value / 100000:.2f}L"


def format_history_summary(
    result: dict,
) -> str:
    scope = result.get(
        "scope",
        "Overall",
    )

    growth = float(
        result.get(
            "period_growth_pct",
            0,
        )
        or 0
    )

    if growth > 5:
        status = "🚀 Strong Growth"
    elif growth > 0:
        status = "📈 Growth"
    elif growth < -5:
        status = "🔻 Decline"
    else:
        status = "➡️ Stable"

    lines = [
        "📊 *AI MIS | HISTORICAL SALES*",
        "",
        f"📍 *{scope}*",
        "",
        f"📅 Period: Last {result.get('months', 6)} Months",
        "",
        "💰 *PERFORMANCE*",
        "",
        f"💵 Total Net Revenue: {format_lakh(result.get('total_net', 0))}",
        f"📊 Average Monthly: {format_lakh(result.get('average_monthly_net', 0))}",
        f"📈 Period Growth: {growth:+.1f}%",
        f"🧠 Performance: {status}",
        "",
    ]

    best = result.get("best_month")
    worst = result.get("worst_month")

    if best:
        lines.extend(
            [
                "🏆 *BEST MONTH*",
                f"{best['month']}: {format_lakh(best['net'])}",
                "",
            ]
        )

    if worst:
        lines.extend(
            [
                "⚠️ *LOWEST MONTH*",
                f"{worst['month']}: {format_lakh(worst['net'])}",
                "",
            ]
        )

    lines.append(
        "📅 *MONTHLY TREND*"
    )

    for row in result.get(
        "monthly",
        [],
    ):
        lines.append(
            f"• {row.get('month')}: "
            f"{format_lakh(row.get('net', 0))} "
            f"({float(row.get('growth_pct', 0) or 0):+.1f}%)"
        )

    return "\n".join(lines)


# =========================================================
# CLI / LOCAL TEST
# =========================================================

if __name__ == "__main__":
    print("=" * 70)
    print("📊 AI MIS HISTORICAL SALES ENGINE")
    print("=" * 70)

    result = get_last_n_months_performance(
        months=6,
    )

    print(
        format_history_summary(
            {
                "scope": "Overall",
                "months": 6,
                **result,
            }
        )
    )
