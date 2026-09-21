"""
Daily Sales Report (DSR) Dashboard

Date comparison logic:

Target Day:
    Yesterday / T-1

Last Week:
    Target Day - 7 days

Last Month Same Day:
    Target Day - 28 days

MTD:
    1st of current month -> Target Day

LMTD:
    1st of previous month -> Same day number as Target Day

LY MTD:
    1st of previous year month -> Same day number as Target Day
"""

import os
import glob
import gzip
import json
import logging
import warnings
from typing import Tuple
from datetime import datetime, timedelta

import pandas as pd
import numpy as np
import requests
import smtplib

from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

# Optional Google Sheets imports
try:
    import gspread
    from google.oauth2.service_account import Credentials
except ImportError:
    gspread = None
    Credentials = None


warnings.filterwarnings('ignore')

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger(__name__)


# ============================================================
# EMAIL CONFIGURATION
# ============================================================

EMAIL_CONFIG = {
    'sender_email': os.getenv('SENDER_EMAIL', 'your-email@gmail.com'),
    'sender_password': os.getenv('EMAIL_PASSWORD', 'your-app-password'),
    'email_to': os.getenv(
        'EMAIL_TO',
        'vivek@frozenbottle.in, '
        'mis2@frozenbottle.in, '
        'mis3@frozenbottle.in, '
        'bhaskar@eatfit.in, '
        'scm@frozenbottle.in, '
        'prasanth.a@frozenbottle.in, '
        'sandeep.ss@eatfit.in, '
        'sonal.raj@curefoods.in, '
        'Ops.all@frozenbottle.in, '
        'mayank.agarwal@curefoods.in'
    ),
    'email_cc': os.getenv(
        'EMAIL_CC',
        'pranshul@frozenbottle.in, '
        'arun.k@frozenbottle.in, '
        'samir.pandey@frozenbottle.in'
    ),
    'smtp_server': 'smtp.gmail.com',
    'smtp_port': 587
}


# ============================================================
# DATA FILES
# ============================================================

HISTORICAL_DATA_FILE = 'historical_data/historical_sales.csv.gz'
MONTHLY_DATA_DIR = 'monthly_data'


# ============================================================
# DSR DASHBOARD
# ============================================================

class DSRDashboard:

    def __init__(self):
        self.df = None
        self.today = None
        self.load_data()

    # ========================================================
    # LOAD DATA
    # ========================================================

    def load_data(self):
        try:
            logger.info("Loading sales data...")
            dataframes = []

            # ------------------------------------------------
            # Historical data
            # ------------------------------------------------
            if os.path.exists(HISTORICAL_DATA_FILE):
                with gzip.open(HISTORICAL_DATA_FILE, 'rt') as f:
                    dataframes.append(pd.read_csv(f))

            # ------------------------------------------------
            # Monthly data
            # ------------------------------------------------
            monthly_files = glob.glob(
                os.path.join(MONTHLY_DATA_DIR, '**', '*.csv'),
                recursive=True
            )

            for m_file in monthly_files:
                dataframes.append(pd.read_csv(m_file))

            if not dataframes:
                raise FileNotFoundError("No sales data files found!")

            self.df = pd.concat(dataframes, ignore_index=True)

            # ------------------------------------------------
            # Date column
            # ------------------------------------------------
            date_columns = [
                col for col in self.df.columns
                if col.lower() in ['date', 'sales_date']
            ]

            if not date_columns:
                raise ValueError("Date / Sales_Date column not found!")

            date_col = date_columns[0]
            self.df['Date'] = pd.to_datetime(self.df[date_col])

            # ------------------------------------------------
            # String columns
            # ------------------------------------------------
            for col in [
                'Session',
                'Source',
                'Brand Name',
                'Store Type',
                'Discount Bucket',
                'AOV Bucket'
            ]:
                if col in self.df.columns:
                    self.df[col] = self.df[col].astype(str).str.strip()

            # ------------------------------------------------
            # Remove duplicates
            # ------------------------------------------------
            dedup_cols = [
                c for c in [
                    'Date',
                    'Branch',
                    'Source',
                    'Session',
                    'Brand Name',
                    'Store Type'
                ]
                if c in self.df.columns
            ]

            if dedup_cols:
                self.df.drop_duplicates(subset=dedup_cols, keep='last', inplace=True)

            # ------------------------------------------------
            # Report target date
            # ------------------------------------------------
            self.today = (datetime.now() - timedelta(days=1)).date()

            # ------------------------------------------------
            # Numeric columns
            # ------------------------------------------------
            for col in [
                'Net Sales',
                'Discount',
                'Taxes',
                'Gross Sales',
                'Quantity',
                'Orders'
            ]:
                if col in self.df.columns:
                    self.df[col] = pd.to_numeric(self.df[col], errors='coerce').fillna(0)

            logger.info(f"✓ Data loaded successfully. Report Target Date: {self.today}")

        except Exception as e:
            logger.error(f"Error loading data: {e}")
            raise

    # ========================================================
    # GROWTH
    # ========================================================

    def calculate_growth(self, current: float, previous: float) -> float:
        if previous == 0 or pd.isna(previous):
            return 0.0
        return ((current - previous) / previous) * 100

    # ========================================================
    # HTML GROWTH FORMAT
    # ========================================================

    def format_growth_html(self, val: float) -> str:
        if pd.isna(val):
            return "0.00%"

        color = "#d4edda" if val >= 0 else "#f8d7da"
        text_color = "#155724" if val >= 0 else "#721c24"
        symbol = "↑" if val >= 0 else "↓"

        return (
            f'<span style="background-color: {color}; '
            f'color: {text_color}; '
            f'padding: 3px 6px; '
            f'border-radius: 4px; '
            f'font-weight: bold;">'
            f'{symbol} {abs(val):.2f}%'
            f'</span>'
        )

    # ========================================================
    # METRICS
    # ========================================================

    def get_metrics_dict(self, data: pd.DataFrame) -> dict:
        if data.empty:
            return {
                'net_sales': 0,
                'discount': 0,
                'orders': 0,
                'dis_pct': 0,
                'aov': 0,
                'offline_pct': 0,
                'online_pct': 0
            }

        net_sales = data['Net Sales'].sum()
        discount = data['Discount'].sum()
        orders = data['Orders'].sum()

        gross_sales = (
            data['Gross Sales'].sum()
            if 'Gross Sales' in data.columns
            else (net_sales + discount)
        )

        dis_pct = (discount / gross_sales * 100) if gross_sales > 0 else 0
        aov = (net_sales / orders) if orders > 0 else 0

        offline_sales = data[
            data['Source'].str.lower() == 'in store'
        ]['Net Sales'].sum()

        total_sales = data['Net Sales'].sum()
        offline_pct = (offline_sales / total_sales * 100) if total_sales > 0 else 0
        online_pct = (100.0 - offline_pct) if total_sales > 0 else 0

        return {
            'net_sales': net_sales,
            'discount': discount,
            'orders': orders,
            'dis_pct': dis_pct,
            'aov': aov,
            'offline_pct': offline_pct,
            'online_pct': online_pct
        }

    # ========================================================
    # KPI CARDS
    # ========================================================

    def get_kpi_cards_data(self) -> dict:
        target_day = self.today

        # --- Overall Data ---
        ftd_data = self.df[self.df['Date'].dt.date == target_day]
        ftd = self.get_metrics_dict(ftd_data)

        mtd_start = target_day.replace(day=1)
        mtd_data = self.df[
            (self.df['Date'].dt.date >= mtd_start) &
            (self.df['Date'].dt.date <= target_day)
        ]
        mtd = self.get_metrics_dict(mtd_data)

        previous_month_date = target_day - pd.DateOffset(months=1)
        lmtd_end = previous_month_date.replace(day=target_day.day).date()
        lmtd_start = lmtd_end.replace(day=1)

        lmtd_data = self.df[
            (self.df['Date'].dt.date >= lmtd_start) &
            (self.df['Date'].dt.date <= lmtd_end)
        ]
        lmtd = self.get_metrics_dict(lmtd_data)

        ly_mtd_end = (target_day - pd.DateOffset(years=1)).replace(day=target_day.day).date()
        ly_mtd_start = ly_mtd_end.replace(day=1)

        lytd_data = self.df[
            (self.df['Date'].dt.date >= ly_mtd_start) &
            (self.df['Date'].dt.date <= ly_mtd_end)
        ]
        lytd = self.get_metrics_dict(lytd_data)

        mom = self.calculate_growth(mtd['net_sales'], lmtd['net_sales'])
        yoy = self.calculate_growth(mtd['net_sales'], lytd['net_sales'])

        # --- COCO Specific Data ---
        coco_df = self.df[self.df['Store Type'] == 'COCO']

        coco_ftd_data = coco_df[coco_df['Date'].dt.date == target_day]
        coco_ftd = self.get_metrics_dict(coco_ftd_data)

        coco_mtd_data = coco_df[
            (coco_df['Date'].dt.date >= mtd_start) &
            (coco_df['Date'].dt.date <= target_day)
        ]
        coco_mtd = self.get_metrics_dict(coco_mtd_data)

        coco_lmtd_data = coco_df[
            (coco_df['Date'].dt.date >= lmtd_start) &
            (coco_df['Date'].dt.date <= lmtd_end)
        ]
        coco_lmtd = self.get_metrics_dict(coco_lmtd_data)

        coco_lytd_data = coco_df[
            (coco_df['Date'].dt.date >= ly_mtd_start) &
            (coco_df['Date'].dt.date <= ly_mtd_end)
        ]
        coco_lytd = self.get_metrics_dict(coco_lytd_data)

        coco_mom = self.calculate_growth(coco_mtd['net_sales'], coco_lmtd['net_sales'])
        coco_yoy = self.calculate_growth(coco_mtd['net_sales'], coco_lytd['net_sales'])

        return {
            'overall': {
                'net_sales_lacs': ftd['net_sales'] / 100000.0,
                'orders': ftd['orders'],
                'dis_pct': ftd['dis_pct'],
                'aov': ftd['aov'],
                'mom_growth': mom,
                'yoy_growth': yoy
            },
            'coco': {
                'net_sales_lacs': coco_ftd['net_sales'] / 100000.0,
                'orders': coco_ftd['orders'],
                'dis_pct': coco_ftd['dis_pct'],
                'aov': coco_ftd['aov'],
                'mom_growth': coco_mom,
                'yoy_growth': coco_yoy
            }
        }

    # ========================================================
    # SUMMARY TABLE
    # ========================================================

    def get_summary_table(self, store_type=None) -> pd.DataFrame:
        df_filtered = (
            self.df if store_type is None
            else self.df[self.df['Store Type'] == store_type]
        )

        target_day = self.today
        last_week_day = target_day - timedelta(days=7)
        last_month_same_day = target_day - timedelta(days=28)

        mtd_start = target_day.replace(day=1)

        previous_month_date = target_day - pd.DateOffset(months=1)
        lmtd_end = previous_month_date.replace(day=target_day.day).date()
        lmtd_start = lmtd_end.replace(day=1)

        ly_mtd_end = (target_day - pd.DateOffset(years=1)).replace(day=target_day.day).date()
        ly_mtd_start = ly_mtd_end.replace(day=1)

        yest_m = self.get_metrics_dict(df_filtered[df_filtered['Date'].dt.date == target_day])
        lw_m = self.get_metrics_dict(df_filtered[df_filtered['Date'].dt.date == last_week_day])
        lm_m = self.get_metrics_dict(df_filtered[df_filtered['Date'].dt.date == last_month_same_day])

        mtd_m = self.get_metrics_dict(df_filtered[
            (df_filtered['Date'].dt.date >= mtd_start) &
            (df_filtered['Date'].dt.date <= target_day)
        ])

        lmtd_m = self.get_metrics_dict(df_filtered[
            (df_filtered['Date'].dt.date >= lmtd_start) &
            (df_filtered['Date'].dt.date <= lmtd_end)
        ])

        ly_mtd_m = self.get_metrics_dict(df_filtered[
            (df_filtered['Date'].dt.date >= ly_mtd_start) &
            (df_filtered['Date'].dt.date <= ly_mtd_end)
        ])

        metrics_map = {
            'Net Sales': 'net_sales',
            'Discount': 'discount',
            'Orders': 'orders',
            'Dis%': 'dis_pct',
            'AOV': 'aov',
            'Offline %': 'offline_pct',
            'Online %': 'online_pct'
        }

        rows = []
        for m_label, key in metrics_map.items():
            y_val = yest_m[key]
            lw_val = lw_m[key]
            lm_val = lm_m[key]
            mtd_val = mtd_m[key]
            lmtd_val = lmtd_m[key]
            ly_val = ly_mtd_m[key]

            rows.append({
                'Metrics': m_label,
                'Yesterday': y_val,
                'Last Week': lw_val,
                'Growth% (LW)': self.calculate_growth(y_val, lw_val),
                'Last Month': lm_val,
                'Growth% (LM)': self.calculate_growth(y_val, lm_val),
                'MTD': mtd_val,
                'LMTD': lmtd_val,
                'Growth% (MTD)': self.calculate_growth(mtd_val, lmtd_val),
                'Last Year MTD': ly_val,
                'Growth% (LY)': self.calculate_growth(mtd_val, ly_val)
            })

        return pd.DataFrame(rows)

    # ========================================================
    # DIMENSION SUMMARY
    # ========================================================

    def get_dimension_summary(self, dimension: str) -> pd.DataFrame:
        data = self.df[self.df['Store Type'] == 'COCO'].copy()
        target_day = self.today

        last_week_day = target_day - timedelta(days=7)
        last_month_same_day = target_day - timedelta(days=28)

        mtd_start = target_day.replace(day=1)

        previous_month_date = target_day - pd.DateOffset(months=1)
        lmtd_end = previous_month_date.replace(day=target_day.day).date()
        lmtd_start = lmtd_end.replace(day=1)

        ly_mtd_end = (target_day - pd.DateOffset(years=1)).replace(day=target_day.day).date()
        ly_mtd_start = ly_mtd_end.replace(day=1)

        def group_sales(df_slice):
            if df_slice.empty:
                return pd.Series(dtype=float)
            return df_slice.groupby(dimension)['Net Sales'].sum()

        yest_s = group_sales(data[data['Date'].dt.date == target_day])
        lw_s = group_sales(data[data['Date'].dt.date == last_week_day])
        lm_s = group_sales(data[data['Date'].dt.date == last_month_same_day])

        mtd_s = group_sales(data[
            (data['Date'].dt.date >= mtd_start) &
            (data['Date'].dt.date <= target_day)
        ])

        lmtd_s = group_sales(data[
            (data['Date'].dt.date >= lmtd_start) &
            (data['Date'].dt.date <= lmtd_end)
        ])

        ly_s = group_sales(data[
            (data['Date'].dt.date >= ly_mtd_start) &
            (data['Date'].dt.date <= ly_mtd_end)
        ])

        if dimension == 'Session':
            session_order = [
                'Breakfast', 'Lunch', 'Snacks',
                'Dinner', 'Post Dinner', 'Late Night', 'Closing'
            ]
            existing_sessions = data[dimension].dropna().unique()
            all_keys = [s for s in session_order if s in existing_sessions]
            all_keys += [s for s in existing_sessions if s not in session_order]
        else:
            all_keys = sorted(list(data[dimension].dropna().unique()))

        rows = []
        for k in all_keys:
            y = yest_s.get(k, 0)
            lw = lw_s.get(k, 0)
            lm = lm_s.get(k, 0)
            mtd = mtd_s.get(k, 0)
            lmtd = lmtd_s.get(k, 0)
            ly = ly_s.get(k, 0)

            rows.append({
                dimension: k,
                'Yesterday': y,
                'Last Week': lw,
                'Growth% (LW)': self.calculate_growth(y, lw),
                'Last Month': lm,
                'Growth% (LM)': self.calculate_growth(y, lm),
                'MTD': mtd,
                'LMTD': lmtd,
                'Growth% (MTD)': self.calculate_growth(mtd, lmtd),
                'Last Year MTD': ly,
                'Growth% (LY)': self.calculate_growth(mtd, ly)
            })

        return pd.DataFrame(rows)

    # ========================================================
    # BUCKET ANALYSIS (UPDATED WITH SUM OF ORDERS COLUMN)
    # ========================================================

    def get_bucket_analysis(self, bucket_col: str) -> pd.DataFrame:
        data = self.df[self.df['Store Type'] == 'COCO'].copy()

        # Helper function to get sum of Orders column
        def get_order_count(df_sub):
            if 'Orders' in df_sub.columns:
                return int(df_sub['Orders'].sum())
            return len(df_sub)

        # FTD
        ftd_data = data[data['Date'].dt.date == self.today]

        # MTD
        mtd_start = self.today.replace(day=1)
        mtd_data = data[
            (data['Date'].dt.date >= mtd_start) &
            (data['Date'].dt.date <= self.today)
        ]

        # Overall Sales Totals
        ftd_total = ftd_data['Net Sales'].sum()
        mtd_total = mtd_data['Net Sales'].sum()

        # Source Specific Sales Totals
        f_instore_total = ftd_data[ftd_data['Source'].str.lower() == 'in store']['Net Sales'].sum()
        f_swiggy_total = ftd_data[ftd_data['Source'].str.lower() == 'swiggy']['Net Sales'].sum()
        f_zomato_total = ftd_data[ftd_data['Source'].str.lower() == 'zomato']['Net Sales'].sum()
        f_ownly_total = ftd_data[ftd_data['Source'].str.lower() == 'ownly']['Net Sales'].sum()

        m_instore_total = mtd_data[mtd_data['Source'].str.lower() == 'in store']['Net Sales'].sum()
        m_swiggy_total = mtd_data[mtd_data['Source'].str.lower() == 'swiggy']['Net Sales'].sum()
        m_zomato_total = mtd_data[mtd_data['Source'].str.lower() == 'zomato']['Net Sales'].sum()
        m_ownly_total = mtd_data[mtd_data['Source'].str.lower() == 'ownly']['Net Sales'].sum()

        buckets = sorted(list(data[bucket_col].dropna().unique()))
        rows = []

        # Helper formatter for "Contrib% | Orders"
        def format_val(sales, total_sales, df_sub):
            pct = (sales / total_sales * 100) if total_sales > 0 else 0.0
            orders = get_order_count(df_sub)
            return f"{pct:.2f}% | {orders:,}"

        for b in buckets:
            f_b = ftd_data[ftd_data[bucket_col] == b]
            m_b = mtd_data[mtd_data[bucket_col] == b]

            # FTD Subset Filtered by Source
            f_b_instore = f_b[f_b['Source'].str.lower() == 'in store']
            f_b_swiggy  = f_b[f_b['Source'].str.lower() == 'swiggy']
            f_b_zomato  = f_b[f_b['Source'].str.lower() == 'zomato']
            f_b_ownly   = f_b[f_b['Source'].str.lower() == 'ownly']

            # MTD Subset Filtered by Source
            m_b_instore = m_b[m_b['Source'].str.lower() == 'in store']
            m_b_swiggy  = m_b[m_b['Source'].str.lower() == 'swiggy']
            m_b_zomato  = m_b[m_b['Source'].str.lower() == 'zomato']
            m_b_ownly   = m_b[m_b['Source'].str.lower() == 'ownly']

            rows.append({
                bucket_col: b,
                'FTD Overall Contrib%': format_val(f_b['Net Sales'].sum(), ftd_total, f_b),
                'FTD In Store Contrib%': format_val(f_b_instore['Net Sales'].sum(), f_instore_total, f_b_instore),
                'FTD Swiggy Contrib%': format_val(f_b_swiggy['Net Sales'].sum(), f_swiggy_total, f_b_swiggy),
                'FTD Zomato Contrib%': format_val(f_b_zomato['Net Sales'].sum(), f_zomato_total, f_b_zomato),
                'FTD Ownly Contrib%': format_val(f_b_ownly['Net Sales'].sum(), f_ownly_total, f_b_ownly),
                
                'MTD Overall Contrib%': format_val(m_b['Net Sales'].sum(), mtd_total, m_b),
                'MTD In Store Contrib%': format_val(m_b_instore['Net Sales'].sum(), m_instore_total, m_b_instore),
                'MTD Swiggy Contrib%': format_val(m_b_swiggy['Net Sales'].sum(), m_swiggy_total, m_b_swiggy),
                'MTD Zomato Contrib%': format_val(m_b_zomato['Net Sales'].sum(), m_zomato_total, m_b_zomato),
                'MTD Ownly Contrib%': format_val(m_b_ownly['Net Sales'].sum(), m_ownly_total, m_b_ownly)
            })

        return pd.DataFrame(rows)

    # ========================================================
    # DAY LEVEL PERFORMANCE
    # ========================================================

    def get_day_level_performance(self) -> pd.DataFrame:
        data = self.df[self.df['Store Type'] == 'COCO'].copy()

        mtd_start = self.today.replace(day=1)
        mtd_data = data[
            (data['Date'].dt.date >= mtd_start) &
            (data['Date'].dt.date <= self.today)
        ]

        dates = sorted(mtd_data['Date'].dt.date.unique())
        date_strs = [d.strftime('%d-%b') for d in dates]
        metrics = ['Net Sales', 'Discount', 'Orders', 'Dis%', 'AOV']

        matrix = {m: [] for m in metrics}

        for d in dates:
            sub = mtd_data[mtd_data['Date'].dt.date == d]
            m_dict = self.get_metrics_dict(sub)

            matrix['Net Sales'].append(m_dict['net_sales'])
            matrix['Discount'].append(m_dict['discount'])
            matrix['Orders'].append(m_dict['orders'])
            matrix['Dis%'].append(m_dict['dis_pct'])
            matrix['AOV'].append(m_dict['aov'])

        res = pd.DataFrame(matrix, index=date_strs).T.reset_index()
        res.rename(columns={'index': 'Metrics'}, inplace=True)

        return res

    # ========================================================
    # TOP / BOTTOM STORES
    # ========================================================

    def get_top_bottom_stores(self) -> Tuple[pd.DataFrame, pd.DataFrame]:
        data = self.df[self.df['Store Type'] == 'COCO'].copy()

        mtd_start = self.today.replace(day=1)
        mtd_data = data[
            (data['Date'].dt.date >= mtd_start) &
            (data['Date'].dt.date <= self.today)
        ]

        grouped = []
        for branch, b_df in mtd_data.groupby('Branch'):
            m = self.get_metrics_dict(b_df)

            grouped.append({
                'Branch': branch,
                'Net Sales': m['net_sales'],
                'Dis%': m['dis_pct'],
                'Offline%': m['offline_pct'],
                'Online%': m['online_pct']
            })

        summary = pd.DataFrame(grouped).sort_values(by='Net Sales', ascending=False)
        top10 = summary.head(10)
        bottom10 = summary.tail(10).sort_values(by='Net Sales', ascending=True)

        return top10, bottom10

    # ========================================================
    # HTML TABLE
    # ========================================================

    def render_table_html(self, df: pd.DataFrame) -> str:
        html = (
            '<table style="width:100%; border-collapse: collapse; font-size: 12px; margin-bottom: 20px;">'
            '<thead><tr style="background-color: #2c3e50; color: white;">'
        )

        for col in df.columns:
            html += f'<th style="padding: 8px; border: 1px solid #ddd; text-align: left;">{col}</th>'

        html += '</tr></thead><tbody>'

        percentage_metrics = [
            'Dis%', 'Offline %', 'Online %', 'Dis',
            'Offline', 'Online', 'Offline%', 'Online%'
        ]

        for _, row in df.iterrows():
            metric_label = str(row.get('Metrics', ''))
            is_pct_metric_row = metric_label in percentage_metrics

            html += '<tr>'

            for col in df.columns:
                val = row[col]
                bg_style = ""

                if 'Contrib%' in col and isinstance(val, (int, float)):
                    max_val = df[col].max()
                    intensity = min(int((val / max_val) * 100), 100) if max_val > 0 else 0
                    bg_style = (
                        f'background-color: rgba(46, 204, 113, {intensity / 100:.2f}); '
                        f'font-weight: bold;'
                    )

                if 'Growth%' in col:
                    cell_content = self.format_growth_html(val)
                elif 'Contrib%' in col or 'Dis%' in col or 'Offline%' in col or 'Online%' in col:
                    cell_content = f"{float(val):.2f}%" if isinstance(val, (int, float)) else str(val)
                elif is_pct_metric_row and isinstance(val, (int, float)):
                    cell_content = f"{val:.2f}%"
                elif isinstance(val, (int, float)):
                    if (
                        'Sales' in col or 'Discount' in col or 'AOV' in col or
                        'Yesterday' in col or 'MTD' in col or 'Last' in col or 'FTD' in col
                    ):
                        cell_content = f"₹{val:,.0f}"
                    else:
                        cell_content = f"{val:,.0f}"
                else:
                    cell_content = str(val)

                html += f'<td style="padding: 8px; border: 1px solid #ddd; {bg_style}">{cell_content}</td>'

            html += '</tr>'

        html += '</tbody></table>'
        return html

    # ========================================================
    # HTML REPORT
    # ========================================================

    def generate_html_report(self) -> str:
        kpi = self.get_kpi_cards_data()

        html = f"""
<html>
<body style="font-family: Arial, sans-serif; background-color: #f4f6f9; padding: 20px; color: #333;">
<div style="max-width: 1200px; margin: 0 auto; background: white; padding: 25px; border-radius: 8px; box-shadow: 0 0 10px rgba(0,0,0,0.1);">

<h2 style="color: #2c3e50; border-bottom: 2px solid #3498db; padding-bottom: 10px;">
📊 Daily Sales Report (DSR) Dashboard
</h2>

<p style="color: #7f8c8d; font-size: 14px;">
Report Date: <strong>{self.today.strftime('%d %B %Y')}</strong>
</p>

<!-- Overall KPI Cards -->
<h4 style="color: #2c3e50; margin-bottom: 8px;">Overall Performance</h4>
<div style="display: flex; gap: 15px; margin-bottom: 20px;">

<div style="flex: 1; background: #ebf5fb; padding: 15px; border-radius: 5px; text-align: center; border-left: 4px solid #3498db;">
<span style="font-size: 12px; color: #5d6d7e;">Net Sales (FTD)</span>
<h3 style="margin: 5px 0; color: #2e86c1;">₹{kpi['overall']['net_sales_lacs']:.2f} Lacs</h3>
</div>

<div style="flex: 1; background: #ebf5fb; padding: 15px; border-radius: 5px; text-align: center; border-left: 4px solid #3498db;">
<span style="font-size: 12px; color: #5d6d7e;">Orders (FTD)</span>
<h3 style="margin: 5px 0; color: #2e86c1;">{kpi['overall']['orders']:,}</h3>
</div>

<div style="flex: 1; background: #ebf5fb; padding: 15px; border-radius: 5px; text-align: center; border-left: 4px solid #3498db;">
<span style="font-size: 12px; color: #5d6d7e;">Dis %</span>
<h3 style="margin: 5px 0; color: #2e86c1;">{kpi['overall']['dis_pct']:.2f}%</h3>
</div>

<div style="flex: 1; background: #ebf5fb; padding: 15px; border-radius: 5px; text-align: center; border-left: 4px solid #3498db;">
<span style="font-size: 12px; color: #5d6d7e;">AOV</span>
<h3 style="margin: 5px 0; color: #2e86c1;">₹{kpi['overall']['aov']:,.0f}</h3>
</div>

<div style="flex: 1; background: #ebf5fb; padding: 15px; border-radius: 5px; text-align: center; border-left: 4px solid #3498db;">
<span style="font-size: 12px; color: #5d6d7e;">MoM% Growth</span>
<h3 style="margin: 5px 0;">{self.format_growth_html(kpi['overall']['mom_growth'])}</h3>
</div>

<div style="flex: 1; background: #ebf5fb; padding: 15px; border-radius: 5px; text-align: center; border-left: 4px solid #3498db;">
<span style="font-size: 12px; color: #5d6d7e;">YoY% Growth</span>
<h3 style="margin: 5px 0;">{self.format_growth_html(kpi['overall']['yoy_growth'])}</h3>
</div>

</div>

<!-- COCO KPI Cards -->
<h4 style="color: #27ae60; margin-bottom: 8px;">COCO Performance</h4>
<div style="display: flex; gap: 15px; margin-bottom: 25px;">

<div style="flex: 1; background: #eafaf1; padding: 15px; border-radius: 5px; text-align: center; border-left: 4px solid #27ae60;">
<span style="font-size: 12px; color: #4a6572;">Net Sales (FTD)</span>
<h3 style="margin: 5px 0; color: #1e8449;">₹{kpi['coco']['net_sales_lacs']:.2f} Lacs</h3>
</div>

<div style="flex: 1; background: #eafaf1; padding: 15px; border-radius: 5px; text-align: center; border-left: 4px solid #27ae60;">
<span style="font-size: 12px; color: #4a6572;">Orders (FTD)</span>
<h3 style="margin: 5px 0; color: #1e8449;">{kpi['coco']['orders']:,}</h3>
</div>

<div style="flex: 1; background: #eafaf1; padding: 15px; border-radius: 5px; text-align: center; border-left: 4px solid #27ae60;">
<span style="font-size: 12px; color: #4a6572;">Dis %</span>
<h3 style="margin: 5px 0; color: #1e8449;">{kpi['coco']['dis_pct']:.2f}%</h3>
</div>

<div style="flex: 1; background: #eafaf1; padding: 15px; border-radius: 5px; text-align: center; border-left: 4px solid #27ae60;">
<span style="font-size: 12px; color: #4a6572;">AOV</span>
<h3 style="margin: 5px 0; color: #1e8449;">₹{kpi['coco']['aov']:,.0f}</h3>
</div>

<div style="flex: 1; background: #eafaf1; padding: 15px; border-radius: 5px; text-align: center; border-left: 4px solid #27ae60;">
<span style="font-size: 12px; color: #4a6572;">MoM% Growth</span>
<h3 style="margin: 5px 0;">{self.format_growth_html(kpi['coco']['mom_growth'])}</h3>
</div>

<div style="flex: 1; background: #eafaf1; padding: 15px; border-radius: 5px; text-align: center; border-left: 4px solid #27ae60;">
<span style="font-size: 12px; color: #4a6572;">YoY% Growth</span>
<h3 style="margin: 5px 0;">{self.format_growth_html(kpi['coco']['yoy_growth'])}</h3>
</div>

</div>

<h3 style="color: #34495e;">1. Overall Sales Summary (COCO + FOFO)</h3>
{self.render_table_html(self.get_summary_table())}

<h3 style="color: #34495e;">2. COCO Sales Summary</h3>
{self.render_table_html(self.get_summary_table(store_type='COCO'))}

<h3 style="color: #34495e;">3. Brand Sales Summary (COCO)</h3>
{self.render_table_html(self.get_dimension_summary('Brand Name'))}

<h3 style="color: #34495e;">4. Region Sales Summary (COCO)</h3>
{self.render_table_html(self.get_dimension_summary('Region'))}

<h3 style="color: #34495e;">5. Source Sales Summary (COCO)</h3>
{self.render_table_html(self.get_dimension_summary('Source'))}

<h3 style="color: #34495e;">6. Session Sales Summary (COCO)</h3>
{self.render_table_html(self.get_dimension_summary('Session'))}

<h3 style="color: #34495e;">7. Discount Bucket Breakdown (COCO)</h3>
{self.render_table_html(self.get_bucket_analysis('Discount Bucket'))}

<h3 style="color: #34495e;">8. AOV Bucket Breakdown (COCO)</h3>
{self.render_table_html(self.get_bucket_analysis('AOV Bucket'))}

<h3 style="color: #34495e;">9. Current Month Day Level Performance (COCO)</h3>
{self.render_table_html(self.get_day_level_performance())}
"""

        top10, bottom10 = self.get_top_bottom_stores()

        html += f"""
<h3 style="color: #34495e;">10. Top 10 Branches (COCO)</h3>
{self.render_table_html(top10)}

<h3 style="color: #34495e;">11. Bottom 10 Stores (COCO)</h3>
{self.render_table_html(bottom10)}

</div>
</body>
</html>
"""
        return html

    # ========================================================
    # SEND EMAIL
    # ========================================================

    def send_dashboard_email(self):
        try:
            logger.info("Generating dashboard email...")
            html_content = self.generate_html_report()

            to_list = [addr.strip() for addr in EMAIL_CONFIG['email_to'].split(',') if addr.strip()]
            cc_list = [addr.strip() for addr in EMAIL_CONFIG['email_cc'].split(',') if addr.strip()]
            all_recipients = to_list + cc_list

            msg = MIMEMultipart('alternative')
            msg['Subject'] = f"Daily Sales Report _ {self.today.strftime('%b %Y')}"
            msg['From'] = EMAIL_CONFIG['sender_email']
            msg['To'] = ", ".join(to_list)
            msg['Cc'] = ", ".join(cc_list)

            msg.attach(MIMEText(html_content, 'html'))

            logger.info(f"Sending email to {len(all_recipients)} recipients...")

            with smtplib.SMTP(EMAIL_CONFIG['smtp_server'], EMAIL_CONFIG['smtp_port']) as server:
                server.starttls()
                server.login(EMAIL_CONFIG['sender_email'], EMAIL_CONFIG['sender_password'])
                server.sendmail(EMAIL_CONFIG['sender_email'], all_recipients, msg.as_string())

            logger.info("✅ DSR Dashboard email sent successfully to all recipients!")
            return True

        except Exception as e:
            logger.error(f"❌ Error sending email: {e}")
            return False


# ============================================================
# TELEGRAM SUMMARY FUNCTION
# ============================================================

def send_telegram_summary(self):
    bot_token = os.getenv("TELEGRAM_BOT_TOKEN")
    chat_id = os.getenv("TELEGRAM_CHAT_ID")

    if not bot_token or not chat_id:
        logger.info("⚠️ Telegram credentials missing. Skipping notification.")
        return False

    def post_message(text):
        url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
        payload = {"chat_id": chat_id, "text": text, "parse_mode": "Markdown"}
        return requests.post(url, json=payload, timeout=10)

    try:
        ftd = self.get_ftd_metrics() if hasattr(self, 'get_ftd_metrics') else {}
        mtd = self.get_mtd_metrics() if hasattr(self, 'get_mtd_metrics') else {}

        if not ftd or not mtd:
            logger.info("⚠️ Metric methods for Telegram not defined. Skipping Telegram.")
            return False

        # ---------------------------------------------------
        # 1. FTD MESSAGE
        # ---------------------------------------------------
        ftd_msg = [
            "📊 *COCO _ FTD SALES REPORT*",
            f"📅 *{self.today.strftime('%d-%b-%Y')}*\n",
            "💰 *Business Overview*",
            f"💵 *Net Revenue:* ₹{ftd['net_rev']:.2f}L (Gross: ₹{ftd['gross_sales']:.2f}L)",
            f"🧾 *Transactions:* {ftd['txns']:,}",
            f"🛒 *Qty Sold:* {ftd.get('qty', 0):,} | 🧺 *AOV:* ₹{ftd['aov']:.0f}",
            f"📉 *Discount:* {ftd['dis_pct']:.0f}% (₹{ftd['dis_val']:.2f}L)\n",
            "🏪 *Brand Contribution*",
            *[f"{icon} *{b}:* ₹{d['val']:.2f}L ({d['pct']}%) · {d['dis']}% dis" 
              for b, d, icon in self.get_brand_data(is_mtd=False)],
            "\n🛵 *Channel Mix*",
            *[f"{icon} *{c}:* ₹{d['val']:.2f}L ({d['pct']}%) · {d['dis']}% dis" 
              for c, d, icon in self.get_channel_data(is_mtd=False)],
            "\n🌍 *Regional Performance*",
            *[f"{icon} *{r}:* ₹{d['val']:.2f}L | {d['txns']:,} Txn · {d['dis']}% dis" 
              for r, d, icon in self.get_region_data(is_mtd=False)],
            "\n⏰ *Session Performance*",
            *[f"🔹 *{s}:* ₹{d['val']:.2f}L ({d['pct']}%)" 
              for s, d in self.get_session_data(is_mtd=False)],
            f"\n💡 *Highest discount channel:* {ftd.get('max_dis_channel', 'N/A')} | *Region:* {ftd.get('max_dis_region', 'N/A')}"
        ]

        # ---------------------------------------------------
        # 2. MTD MESSAGE
        # ---------------------------------------------------
        mtd_msg = [
            "📊 *COCO _ MTD SALES REPORT*",
            f"📅 *{self.today.strftime('%b-%Y')}*\n",
            "💰 *MTD Business Overview*",
            f"💵 *Net Revenue:* ₹{mtd['net_rev']:.2f}L (Gross: ₹{mtd['gross_sales']:.2f}L)",
            f"🧾 *Transactions:* {mtd['txns']:,}",
            f"🧺 *AOV:* ₹{mtd['aov']:.0f}",
            f"📉 *Discount:* {mtd['dis_pct']:.0f}% (₹{mtd['dis_val']:.2f}L)\n",
            "🏪 *MTD Brand Contribution*",
            *[f"{icon} *{b}:* ₹{d['val']:.2f}L ({d['pct']}%) · {d['dis']}% dis" 
              for b, d, icon in self.get_brand_data(is_mtd=True)],
            "\n🛵 *MTD Channel Mix*",
            *[f"{icon} *{c}:* ₹{d['val']:.2f}L ({d['pct']}%) · {d['dis']}% dis" 
              for c, d, icon in self.get_channel_data(is_mtd=True)],
            "\n🌍 *MTD Regional Performance*",
            *[f"{icon} *{r}:* ₹{d['val']:.2f}L | {d['txns']:,} Txn · {d['dis']}% dis" 
              for r, d, icon in self.get_region_data(is_mtd=True)],
            "\n⏰ *MTD Session Performance*",
            *[f"🔹 *{s}:* ₹{d['val']:.2f}L ({d['pct']}%)" 
              for s, d in self.get_session_data(is_mtd=True)],
            f"\n💡 *Highest discount channel:* {mtd.get('max_dis_channel', 'N/A')} | *Region:* {mtd.get('max_dis_region', 'N/A')}"
        ]

        post_message("\n".join(ftd_msg))
        post_message("\n".join(mtd_msg))
        logger.info("✅ FTD and MTD Telegram reports sent successfully!")
        return True

    except Exception as e:
        logger.error(f"❌ Failed to send Telegram summary: {e}")
        return False


DSRDashboard.send_telegram_summary = send_telegram_summary


# ============================================================
# GOOGLE SHEETS DSR DASHBOARD
# ============================================================

GOOGLE_SHEET_ID = "1gryf29pAcBUQ9YN5igXklWvhbSASzL_3a7Cqdt9vrC0"
GOOGLE_SHEET_TAB = "Dashboard"


def _gs_col_letter(n):
    result = ""
    while n > 0:
        n, remainder = divmod(n - 1, 26)
        result = chr(65 + remainder) + result
    return result


def _gs_clean_dataframe(df):
    if df is None or df.empty:
        return []

    df = df.copy()

    for col in df.columns:
        if pd.api.types.is_datetime64_any_dtype(df[col]):
            df[col] = df[col].dt.strftime("%d-%b-%Y")

    df = df.replace([np.nan, np.inf, -np.inf], "")
    output = []

    for row in df.values.tolist():
        clean_row = []
        for value in row:
            if isinstance(value, np.integer):
                value = int(value)
            elif isinstance(value, np.floating):
                value = float(value)
            elif pd.isna(value):
                value = ""
            clean_row.append(value)
        output.append(clean_row)

    return output


def _gs_get_filters(worksheet, dashboard):
    values = worksheet.get("A2:G2")
    if not values:
        values = [[]]

    row = values[0]
    while len(row) < 7:
        row.append("")

    from_date = str(row[0]).strip()
    to_date = str(row[1]).strip()
    from_month = str(row[2]).strip()
    to_month = str(row[3]).strip()
    region = str(row[4]).strip() or "ALL"
    store_type = str(row[5]).strip() or "ALL"
    source = str(row[6]).strip() or "ALL"

    if from_date and to_date:
        mode = "DATE"
    elif from_month and to_month:
        mode = "MONTH"
    else:
        default_date = dashboard.today.strftime("%d-%b-%Y")
        from_date = default_date
        to_date = default_date
        mode = "DATE"

    return {
        "mode": mode,
        "from_date": from_date,
        "to_date": to_date,
        "from_month": from_month,
        "to_month": to_month,
        "region": region,
        "store_type": store_type,
        "source": source
    }


def _gs_apply_filters(dashboard, filters):
    df = dashboard.df.copy()

    for col in ["Region", "Store Type", "Source"]:
        if col in df.columns:
            df[col] = df[col].fillna("").astype(str).str.strip()

    if filters["mode"] == "DATE":
        from_date = pd.to_datetime(filters["from_date"], dayfirst=True, errors="coerce")
        to_date = pd.to_datetime(filters["to_date"], dayfirst=True, errors="coerce")

        if pd.isna(from_date) or pd.isna(to_date):
            raise ValueError("Invalid From Date / To Date in Dashboard.")

        from_date = from_date.normalize()
        to_date = to_date.normalize()

        df = df[
            (df["Date"].dt.normalize() >= from_date) &
            (df["Date"].dt.normalize() <= to_date)
        ].copy()
    else:
        from_month = pd.to_datetime(filters["from_month"], errors="coerce")
        to_month = pd.to_datetime(filters["to_month"], errors="coerce")

        if pd.isna(from_month) or pd.isna(to_month):
            raise ValueError("Invalid From Month / To Month in Dashboard.")

        from_period = from_month.to_period("M")
        to_period = to_month.to_period("M")

        if from_period > to_period:
            raise ValueError("From Month cannot be greater than To Month.")

        df_period = df["Date"].dt.to_period("M")
        df = df[(df_period >= from_period) & (df_period <= to_period)].copy()

    if filters["region"] and filters["region"].upper() != "ALL":
        df = df[df["Region"].str.lower() == filters["region"].strip().lower()].copy()

    if filters["store_type"] and filters["store_type"].upper() != "ALL":
        df = df[df["Store Type"].str.lower() == filters["store_type"].strip().lower()].copy()

    if filters["source"] and filters["source"].upper() != "ALL":
        df = df[df["Source"].str.lower() == filters["source"].strip().lower()].copy()

    return df


def _gs_metrics(data):
    if data is None or data.empty:
        return {
            "net_sales": 0, "discount": 0, "orders": 0, "gross_sales": 0,
            "quantity": 0, "dis_pct": 0, "aov": 0, "offline_pct": 0, "online_pct": 0
        }

    net_sales = data["Net Sales"].sum()
    discount = data["Discount"].sum()
    orders = data["Orders"].sum()

    gross_sales = (
        data["Gross Sales"].sum()
        if "Gross Sales" in data.columns
        else net_sales + discount
    )

    quantity = data["Quantity"].sum() if "Quantity" in data.columns else 0
    dis_pct = (discount / gross_sales * 100) if gross_sales > 0 else 0
    aov = (net_sales / orders) if orders > 0 else 0

    offline_sales = data[data["Source"].str.lower().eq("in store")]["Net Sales"].sum()
    offline_pct = (offline_sales / net_sales * 100) if net_sales > 0 else 0
    online_pct = (100 - offline_pct) if net_sales > 0 else 0

    return {
        "net_sales": net_sales, "discount": discount, "orders": orders,
        "gross_sales": gross_sales, "quantity": quantity, "dis_pct": dis_pct,
        "aov": aov, "offline_pct": offline_pct, "online_pct": online_pct
    }


def _gs_calculate_growth(current, previous):
    if previous == 0 or pd.isna(previous):
        return 0
    return ((current - previous) / previous) * 100


def _gs_period_comparison(dashboard, filtered_df, filters):
    rows = []

    if filters["mode"] == "DATE":
        from_date = pd.to_datetime(filters["from_date"], dayfirst=True).normalize()
        to_date = pd.to_datetime(filters["to_date"], dayfirst=True).normalize()
        period_days = (to_date - from_date).days + 1

        current = filtered_df[
            (filtered_df["Date"].dt.normalize() >= from_date) &
            (filtered_df["Date"].dt.normalize() <= to_date)
        ]

        previous_week_start = from_date - timedelta(days=7)
        previous_week_end = to_date - timedelta(days=7)

        previous_month_end = from_date - timedelta(days=1)
        previous_month_start = previous_month_end - timedelta(days=period_days - 1)

        previous_year_start = from_date - pd.DateOffset(years=1)
        previous_year_end = to_date - pd.DateOffset(years=1)

        previous_week = filtered_df[
            (filtered_df["Date"].dt.normalize() >= previous_week_start) &
            (filtered_df["Date"].dt.normalize() <= previous_week_end)
        ]

        previous_month = filtered_df[
            (filtered_df["Date"].dt.normalize() >= previous_month_start) &
            (filtered_df["Date"].dt.normalize() <= previous_month_end)
        ]

        previous_year = filtered_df[
            (filtered_df["Date"].dt.normalize() >= previous_year_start) &
            (filtered_df["Date"].dt.normalize() <= previous_year_end)
        ]

        datasets = {
            "Selected Period": current,
            "Previous Week": previous_week,
            "Previous Month": previous_month,
            "Previous Year": previous_year
        }

        metric_map = {
            "Net Sales": "net_sales", "Discount": "discount", "Orders": "orders",
            "Dis%": "dis_pct", "AOV": "aov", "Offline %": "offline_pct", "Online %": "online_pct"
        }

        metric_values = {label: _gs_metrics(data) for label, data in datasets.items()}

        for metric, key in metric_map.items():
            current_value = metric_values["Selected Period"][key]
            week_value = metric_values["Previous Week"][key]
            month_value = metric_values["Previous Month"][key]
            year_value = metric_values["Previous Year"][key]

            rows.append({
                "Metrics": metric,
                "Selected Period": current_value,
                "Previous Week": week_value,
                "Growth % (PW)": _gs_calculate_growth(current_value, week_value),
                "Previous Month": month_value,
                "Growth % (PM)": _gs_calculate_growth(current_value, month_value),
                "Previous Year": year_value,
                "Growth % (PY)": _gs_calculate_growth(current_value, year_value)
            })

        return pd.DataFrame(rows)

    from_month = pd.to_datetime(filters["from_month"]).to_period("M")
    to_month = pd.to_datetime(filters["to_month"]).to_period("M")
    months = pd.period_range(from_month, to_month, freq="M")

    month_rows = []
    for month in months:
        data = filtered_df[filtered_df["Date"].dt.to_period("M") == month]
        m = _gs_metrics(data)
        month_rows.append({
            "Month": month.strftime("%b-%Y"),
            "Net Sales": m["net_sales"],
            "Discount": m["discount"],
            "Orders": m["orders"],
            "Quantity": m["quantity"],
            "Dis%": m["dis_pct"],
            "AOV": m["aov"],
            "Offline %": m["offline_pct"],
            "Online %": m["online_pct"]
        })

    return pd.DataFrame(month_rows)


def _gs_dimension_summary(filtered_df, dimension, coco_only=True):
    data = filtered_df.copy()
    if coco_only and "Store Type" in data.columns:
        data = data[data["Store Type"].str.upper() == "COCO"]

    if data.empty:
        return pd.DataFrame(columns=[
            dimension, "Net Sales", "Orders", "Quantity", "Discount", "Dis%", "AOV"
        ])

    grouped = (
        data.groupby(dimension, dropna=False)
        .agg({
            "Net Sales": "sum",
            "Orders": "sum",
            "Quantity": "sum",
            "Discount": "sum",
            "Gross Sales": "sum"
        })
        .reset_index()
    )

    grouped["Dis%"] = np.where(
        grouped["Gross Sales"] > 0,
        grouped["Discount"] / grouped["Gross Sales"] * 100, 0
    )
    grouped["AOV"] = np.where(
        grouped["Orders"] > 0,
        grouped["Net Sales"] / grouped["Orders"], 0
    )

    grouped.drop(columns=["Gross Sales"], inplace=True)
    grouped.sort_values("Net Sales", ascending=False, inplace=True)

    return grouped


def _gs_bucket_analysis(filtered_df, bucket_col):
    data = filtered_df.copy()
    if "Store Type" in data.columns:
        data = data[data["Store Type"].str.upper() == "COCO"]

    if data.empty:
        return pd.DataFrame()

    total_sales = data["Net Sales"].sum()
    instore_total = data[data["Source"].str.lower() == "in store"]["Net Sales"].sum()
    swiggy_total = data[data["Source"].str.lower() == "swiggy"]["Net Sales"].sum()
    zomato_total = data[data["Source"].str.lower() == "zomato"]["Net Sales"].sum()
    ownly_total = data[data["Source"].str.lower() == "ownly"]["Net Sales"].sum()

    rows = []
    for bucket in sorted(data[bucket_col].dropna().astype(str).unique()):
        sub = data[data[bucket_col].astype(str) == bucket]

        overall = sub["Net Sales"].sum()
        instore = sub[sub["Source"].str.lower() == "in store"]["Net Sales"].sum()
        swiggy = sub[sub["Source"].str.lower() == "swiggy"]["Net Sales"].sum()
        zomato = sub[sub["Source"].str.lower() == "zomato"]["Net Sales"].sum()
        ownly = sub[sub["Source"].str.lower() == "ownly"]["Net Sales"].sum()

        rows.append({
            bucket_col: bucket,
            "Overall Contrib%": (overall / total_sales * 100) if total_sales > 0 else 0,
            "In Store Contrib%": (instore / instore_total * 100) if instore_total > 0 else 0,
            "Swiggy Contrib%": (swiggy / swiggy_total * 100) if swiggy_total > 0 else 0,
            "Zomato Contrib%": (zomato / zomato_total * 100) if zomato_total > 0 else 0,
            "Ownly Contrib%": (ownly / ownly_total * 100) if ownly_total > 0 else 0
        })

    return pd.DataFrame(rows)


def _gs_day_level(filtered_df):
    data = filtered_df.copy()
    data = data[data["Store Type"].str.upper() == "COCO"]

    if data.empty:
        return pd.DataFrame()

    dates = sorted(data["Date"].dt.date.unique())
    rows = []

    for date_value in dates:
        sub = data[data["Date"].dt.date == date_value]
        m = _gs_metrics(sub)

        rows.append({
            "Date": date_value.strftime("%d-%b-%Y"),
            "Net Sales": m["net_sales"],
            "Discount": m["discount"],
            "Orders": m["orders"],
            "Quantity": m["quantity"],
            "Dis%": m["dis_pct"],
            "AOV": m["aov"]
        })

    return pd.DataFrame(rows)


def _gs_top_bottom_stores(filtered_df):
    data = filtered_df.copy()
    data = data[data["Store Type"].str.upper() == "COCO"]

    if data.empty:
        return pd.DataFrame(), pd.DataFrame()

    grouped = []
    for branch, branch_df in data.groupby("Branch"):
        m = _gs_metrics(branch_df)

        grouped.append({
            "Branch": branch,
            "Net Sales": m["net_sales"],
            "Orders": m["orders"],
            "Quantity": m["quantity"],
            "Dis%": m["dis_pct"],
            "Offline%": m["offline_pct"],
            "Online%": m["online_pct"]
        })

    summary = pd.DataFrame(grouped).sort_values("Net Sales", ascending=False)
    top10 = summary.head(10).copy()
    bottom10 = summary.tail(10).sort_values("Net Sales", ascending=True).copy()

    return top10, bottom10


def _gs_write_section(worksheet, title, dataframe, start_row):
    worksheet.update(f"A{start_row}", [[title]])

    if dataframe is None or dataframe.empty:
        worksheet.update(f"A{start_row + 1}", [["No data available"]])
        return start_row + 3

    headers = list(dataframe.columns)
    values = _gs_clean_dataframe(dataframe)
    output = [headers] + values

    end_row = start_row + len(output) - 1
    end_col = _gs_col_letter(len(headers))

    worksheet.update(f"A{start_row + 1}:{end_col}{end_row}", output)

    worksheet.format(
        f"A{start_row + 1}:{end_col}{start_row + 1}",
        {"textFormat": {"bold": True}}
    )

    worksheet.format(
        f"A{start_row}",
        {"textFormat": {"bold": True, "fontSize": 12}}
    )

    return end_row + 3


def update_google_sheet(self):
    if not gspread or not Credentials:
        logger.error("❌ gspread or google-auth not installed. Skipping Google Sheet update.")
        return False

    try:
        logger.info("Starting Google Sheets Dashboard update...")

        credentials_json = os.getenv("GOOGLE_CREDENTIALS")
        if not credentials_json:
            logger.error("❌ GOOGLE_CREDENTIALS not found.")
            return False

        scopes = [
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive"
        ]

        creds_dict = json.loads(credentials_json)
        creds = Credentials.from_service_account_info(creds_dict, scopes=scopes)
        client = gspread.authorize(creds)

        spreadsheet = client.open_by_key(GOOGLE_SHEET_ID)
        worksheet = spreadsheet.worksheet(GOOGLE_SHEET_TAB)

        logger.info("✓ Connected to Google Sheet Dashboard")

        filters = _gs_get_filters(worksheet, self)
        logger.info(f"Dashboard filters: {filters}")

        filtered_df = _gs_apply_filters(self, filters)
        logger.info(f"✓ Filtered rows: {len(filtered_df):,}")

        worksheet.batch_clear(["A5:Z1000"])

        filter_display = [[
            "Mode", filters["mode"],
            "From Date", filters["from_date"],
            "To Date", filters["to_date"],
            "Region", filters["region"],
            "Store Type", filters["store_type"],
            "Source", filters["source"]
        ]]

        worksheet.update("A4", filter_display)
        worksheet.update("A5", [["📊 Daily Sales Report (DSR) Dashboard"]])

        worksheet.format("A5", {"textFormat": {"bold": True, "fontSize": 16}})

        current_row = 7

        period_analysis = _gs_period_comparison(self, filtered_df, filters)
        current_row = _gs_write_section(worksheet, "1. Overall Sales Summary", period_analysis, current_row)

        coco_df = filtered_df[filtered_df["Store Type"].str.upper() == "COCO"].copy()
        coco_metrics = _gs_metrics(coco_df)

        coco_summary = pd.DataFrame([
            {"Metric": "Net Sales", "Value": coco_metrics["net_sales"]},
            {"Metric": "Discount", "Value": coco_metrics["discount"]},
            {"Metric": "Orders", "Value": coco_metrics["orders"]},
            {"Metric": "Quantity", "Value": coco_metrics["quantity"]},
            {"Metric": "Dis%", "Value": coco_metrics["dis_pct"]},
            {"Metric": "AOV", "Value": coco_metrics["aov"]},
            {"Metric": "Offline%", "Value": coco_metrics["offline_pct"]},
            {"Metric": "Online%", "Value": coco_metrics["online_pct"]}
        ])

        current_row = _gs_write_section(worksheet, "2. COCO Sales Summary", coco_summary, current_row)

        brand_summary = _gs_dimension_summary(filtered_df, "Brand Name", coco_only=True)
        current_row = _gs_write_section(worksheet, "3. Brand Sales Summary (COCO)", brand_summary, current_row)

        region_summary = _gs_dimension_summary(filtered_df, "Region", coco_only=True)
        current_row = _gs_write_section(worksheet, "4. Region Sales Summary (COCO)", region_summary, current_row)

        source_summary = _gs_dimension_summary(filtered_df, "Source", coco_only=True)
        current_row = _gs_write_section(worksheet, "5. Source Sales Summary (COCO)", source_summary, current_row)

        session_summary = _gs_dimension_summary(filtered_df, "Session", coco_only=True)
        current_row = _gs_write_section(worksheet, "6. Session Sales Summary (COCO)", session_summary, current_row)

        discount_bucket = _gs_bucket_analysis(filtered_df, "Discount Bucket")
        current_row = _gs_write_section(worksheet, "7. Discount Bucket Breakdown (COCO)", discount_bucket, current_row)

        aov_bucket = _gs_bucket_analysis(filtered_df, "AOV Bucket")
        current_row = _gs_write_section(worksheet, "8. AOV Bucket Breakdown (COCO)", aov_bucket, current_row)

        day_level = _gs_day_level(filtered_df)
        current_row = _gs_write_section(worksheet, "9. Day Level Performance (COCO)", day_level, current_row)

        top10, bottom10 = _gs_top_bottom_stores(filtered_df)
        current_row = _gs_write_section(worksheet, "10. Top 10 Branches (COCO)", top10, current_row)
        current_row = _gs_write_section(worksheet, "11. Bottom 10 Stores (COCO)", bottom10, current_row)

        try:
            worksheet.freeze(rows=4)
            worksheet.columns_auto_resize(0, min(15, worksheet.col_count))
        except Exception as format_error:
            logger.warning(f"⚠️ Sheet formatting warning: {format_error}")

        logger.info("✅ Google Sheets DSR Dashboard updated successfully!")
        return True

    except Exception as e:
        logger.exception(f"❌ Google Sheets Dashboard update failed: {e}")
        return False


DSRDashboard.update_google_sheet = update_google_sheet


# ============================================================
# MAIN EXECUTION
# ============================================================

def main():
    logger.info("Starting DSR Dashboard execution...")

    dashboard = DSRDashboard()

    # 1. EMAIL
    logger.info("Generating dashboard email...")
    dashboard.send_dashboard_email()

    # 2. TELEGRAM
    logger.info("Sending Telegram summary...")
    dashboard.send_telegram_summary()

    # 3. GOOGLE SHEET
    logger.info("Updating Google Sheets...")
    dashboard.update_google_sheet()

    logger.info("DSR Dashboard execution completed.")


if __name__ == "__main__":
    main()
