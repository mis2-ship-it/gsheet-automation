"""
Daily Sales Report (DSR) Dashboard
Comprehensive multi-level analysis with email delivery
"""

import gzip
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.image import MIMEImage
import os
import io
from pathlib import Path
import logging
from typing import Tuple, Dict, List
import matplotlib.pyplot as plt
import seaborn as sns
from matplotlib.patches import Rectangle
import warnings

warnings.filterwarnings('ignore')

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Configuration
EMAIL_CONFIG = {
    'sender_email': os.getenv('SENDER_EMAIL', 'your-email@gmail.com'),
    'sender_password': os.getenv('EMAIL_PASSWORD', 'your-app-password'),
    'recipients': os.getenv('DSR_RECIPIENTS', 'recipient@example.com').split(','),
    'smtp_server': 'smtp.gmail.com',
    'smtp_port': 587
}

DATA_FILE = 'historical_data/historical_sales.csv.gz'

class DSRDashboard:
    """Daily Sales Report Dashboard Generator"""
    
    def __init__(self, data_path=DATA_FILE):
        """Initialize dashboard with data"""
        self.data_path = data_path
        self.df = None
        self.today = None
        self.load_data()
        
    def load_data(self):
        """Load and prepare sales data"""
        try:
            logger.info("Loading sales data...")
            with gzip.open(self.data_path, 'rt') as f:
                self.df = pd.read_csv(f)
            
            # Parse date column
            date_col = [col for col in self.df.columns if col.lower() in ['date', 'sales_date']][0]
            self.df['Date'] = pd.to_datetime(self.df[date_col])
            self.today = self.df['Date'].max().date()
            
            logger.info(f"✓ Data loaded successfully. Latest date: {self.today}")
            logger.info(f"  Total records: {len(self.df)}")
            logger.info(f"  Columns: {', '.join(self.df.columns)}")
            
        except Exception as e:
            logger.error(f"Error loading data: {e}")
            raise
    
    def get_date_range(self, range_type: str, reference_date=None) -> Tuple[datetime, datetime]:
        """Get date ranges for various comparisons"""
        if reference_date is None:
            reference_date = pd.Timestamp(self.today)
        else:
            reference_date = pd.Timestamp(reference_date)
        
        if range_type == 'today':
            return reference_date.date(), reference_date.date()
        
        elif range_type == 'yesterday':
            yesterday = reference_date - timedelta(days=1)
            return yesterday.date(), yesterday.date()
        
        elif range_type == 'last_week':
            last_week = reference_date - timedelta(days=7)
            return last_week.date(), last_week.date()
        
        elif range_type == 'last_month_same_day':
            last_month = reference_date - pd.DateOffset(months=1)
            return last_month.date(), last_month.date()
        
        elif range_type == 'last_year_same_day':
            last_year = reference_date - pd.DateOffset(years=1)
            return last_year.date(), last_year.date()
        
        elif range_type == 'mtd':  # Month-to-date
            month_start = reference_date.replace(day=1)
            return month_start.date(), reference_date.date()
        
        elif range_type == 'lmtd':  # Last month till date
            last_month = reference_date - pd.DateOffset(months=1)
            month_start = last_month.replace(day=1)
            return month_start.date(), last_month.date()
        
        elif range_type == 'lytd':  # Last year till date
            last_year = reference_date - pd.DateOffset(years=1)
            month_start = last_year.replace(day=1)
            return month_start.date(), last_year.date()
        
        elif range_type == 'last_week_range':
            last_week_end = reference_date - timedelta(days=1)
            last_week_start = last_week_end - timedelta(days=6)
            return last_week_start.date(), last_week_end.date()
        
        elif range_type == 'last_month_range':
            last_month = reference_date - pd.DateOffset(months=1)
            month_start = last_month.replace(day=1)
            month_end = (month_start + pd.DateOffset(months=1)) - timedelta(days=1)
            return month_start.date(), month_end.date()
        
        elif range_type == 'last_year_month':
            last_year = reference_date - pd.DateOffset(years=1)
            month_start = last_year.replace(day=1)
            month_end = (month_start + pd.DateOffset(months=1)) - timedelta(days=1)
            return month_start.date(), month_end.date()
    
    def filter_by_date(self, start_date, end_date, store_type=None):
        """Filter dataframe by date range and optional store type"""
        mask = (self.df['Date'].dt.date >= start_date) & (self.df['Date'].dt.date <= end_date)
        filtered = self.df[mask].copy()
        
        if store_type:
            filtered = filtered[filtered['Store Type'] == store_type]
        
        return filtered
    
    def calculate_metrics(self, data: pd.DataFrame) -> Dict:
        """Calculate all metrics from data"""
        if data.empty:
            return {
                'net_sales': 0, 'orders': 0, 'dis_pct': 0, 'aov': 0,
                'discount': 0, 'taxes': 0, 'gross_sales': 0,
                'offline_pct': 0, 'online_pct': 0
            }
        
        net_sales = data['Net Sales'].sum()
        orders = data['Orders'].sum()
        discount = data['Discount'].sum()
        gross_sales = data['Gross Sales'].sum()
        
        # Dis % - weighted average
        dis_pct = (discount / gross_sales * 100) if gross_sales > 0 else 0
        
        # AOV
        aov = (net_sales / orders) if orders > 0 else 0
        
        # Offline vs Online
        offline_sales = data[data['Source'] == 'In Store']['Net Sales'].sum()
        online_sales = data[data['Source'] != 'In Store']['Net Sales'].sum()
        total_sales = offline_sales + online_sales
        
        offline_pct = (offline_sales / total_sales * 100) if total_sales > 0 else 0
        online_pct = (online_sales / total_sales * 100) if total_sales > 0 else 0
        
        return {
            'net_sales': net_sales,
            'orders': orders,
            'dis_pct': dis_pct,
            'aov': aov,
            'discount': discount,
            'offline_pct': offline_pct,
            'online_pct': online_pct
        }
    
    def calculate_growth(self, current: float, previous: float) -> float:
        """Calculate growth percentage"""
        if previous == 0:
            return 0
        return ((current - previous) / previous) * 100
    
    def get_growth_html_color(self, growth: float) -> str:
        """Return HTML with color coding for growth"""
        if growth >= 0:
            color = '#d4edda'  # Light green
            symbol = '↑'
        else:
            color = '#f8d7da'  # Light red
            symbol = '↓'
        
        return f'<span style="background-color: {color}; padding: 2px 6px; border-radius: 3px;">{symbol}{abs(growth):.2f}%</span>'
    
    # ==================== KPI CARDS ====================
    
    def get_kpi_cards_data(self, store_type=None) -> Dict:
        """Get KPI cards data for top 5 metrics"""
        # Today
        today_data = self.filter_by_date(*self.get_date_range('today'), store_type)
        today_metrics = self.calculate_metrics(today_data)
        
        # Last month same day
        lmsd_range = self.get_date_range('last_month_same_day')
        lmsd_data = self.filter_by_date(lmsd_range[0], lmsd_range[1], store_type)
        lmsd_metrics = self.calculate_metrics(lmsd_data)
        
        # Last year same day
        lysd_range = self.get_date_range('last_year_same_day')
        lysd_data = self.filter_by_date(lysd_range[0], lysd_range[1], store_type)
        lysd_metrics = self.calculate_metrics(lysd_data)
        
        # MoM (Month-to-date vs Last month till date)
        mtd_range = self.get_date_range('mtd')
        mtd_data = self.filter_by_date(mtd_range[0], mtd_range[1], store_type)
        mtd_metrics = self.calculate_metrics(mtd_data)
        
        lmtd_range = self.get_date_range('lmtd')
        lmtd_data = self.filter_by_date(lmtd_range[0], lmtd_range[1], store_type)
        lmtd_metrics = self.calculate_metrics(lmtd_data)
        
        mom_growth = self.calculate_growth(mtd_metrics['net_sales'], lmtd_metrics['net_sales'])
        
        # YoY (Last year till date)
        lytd_range = self.get_date_range('lytd')
        lytd_data = self.filter_by_date(lytd_range[0], lytd_range[1], store_type)
        lytd_metrics = self.calculate_metrics(lytd_data)
        
        yoy_growth = self.calculate_growth(mtd_metrics['net_sales'], lytd_metrics['net_sales'])
        
        return {
            'net_sales': today_metrics['net_sales'],
            'net_sales_lacs': today_metrics['net_sales'] / 100000,  # Convert to lacs
            'orders': today_metrics['orders'],
            'dis_pct': today_metrics['dis_pct'],
            'aov': today_metrics['aov'],
            'mom_growth': mom_growth,
            'yoy_growth': yoy_growth
        }
    
    # ==================== OVERALL SUMMARY ====================
    
    def get_overall_summary(self, store_type=None) -> pd.DataFrame:
        """Get overall summary table"""
        metrics_list = []
        
        # Today
        today_data = self.filter_by_date(*self.get_date_range('today'), store_type)
        today_metrics = self.calculate_metrics(today_data)
        
        # Last Week (same day of week)
        lw_range = self.get_date_range('last_week')
        lw_data = self.filter_by_date(lw_range[0], lw_range[1], store_type)
        lw_metrics = self.calculate_metrics(lw_data)
        lw_growth_sales = self.calculate_growth(today_metrics['net_sales'], lw_metrics['net_sales'])
        
        # Last Month (same day)
        lm_range = self.get_date_range('last_month_same_day')
        lm_data = self.filter_by_date(lm_range[0], lm_range[1], store_type)
        lm_metrics = self.calculate_metrics(lm_data)
        lm_growth_sales = self.calculate_growth(today_metrics['net_sales'], lm_metrics['net_sales'])
        
        # MTD
        mtd_range = self.get_date_range('mtd')
        mtd_data = self.filter_by_date(mtd_range[0], mtd_range[1], store_type)
        mtd_metrics = self.calculate_metrics(mtd_data)
        
        # LMTD
        lmtd_range = self.get_date_range('lmtd')
        lmtd_data = self.filter_by_date(lmtd_range[0], lmtd_range[1], store_type)
        lmtd_metrics = self.calculate_metrics(lmtd_data)
        mtd_lmtd_growth = self.calculate_growth(mtd_metrics['net_sales'], lmtd_metrics['net_sales'])
        
        # Last Year
        ly_range = self.get_date_range('last_year_same_day')
        ly_data = self.filter_by_date(ly_range[0], ly_range[1], store_type)
        ly_metrics = self.calculate_metrics(ly_data)
        ly_growth_sales = self.calculate_growth(today_metrics['net_sales'], ly_metrics['net_sales'])
        
        # Build summary rows
        summary_data = {
            'Net Sales': {
                'Today': f"₹{today_metrics['net_sales']:,.0f}",
                'Last Week': f"₹{lw_metrics['net_sales']:,.0f}",
                'Growth%': f"{lw_growth_sales:.2f}%",
                'Last Month': f"₹{lm_metrics['net_sales']:,.0f}",
                'Growth%.1': f"{lm_growth_sales:.2f}%",
                'MTD': f"₹{mtd_metrics['net_sales']:,.0f}",
                'LMTD': f"₹{lmtd_metrics['net_sales']:,.0f}",
                'Growth%.2': f"{mtd_lmtd_growth:.2f}%",
                'Last Year': f"₹{ly_metrics['net_sales']:,.0f}",
                'Growth%.3': f"{ly_growth_sales:.2f}%"
            },
            'Orders': {
                'Today': int(today_metrics['orders']),
                'Last Week': int(lw_metrics['orders']),
                'Growth%': f"{self.calculate_growth(today_metrics['orders'], lw_metrics['orders']):.2f}%",
                'Last Month': int(lm_metrics['orders']),
                'Growth%.1': f"{self.calculate_growth(today_metrics['orders'], lm_metrics['orders']):.2f}%",
                'MTD': int(mtd_metrics['orders']),
                'LMTD': int(lmtd_metrics['orders']),
                'Growth%.2': f"{self.calculate_growth(mtd_metrics['orders'], lmtd_metrics['orders']):.2f}%",
                'Last Year': int(ly_metrics['orders']),
                'Growth%.3': f"{self.calculate_growth(today_metrics['orders'], ly_metrics['orders']):.2f}%"
            },
            'Discount': {
                'Today': f"₹{today_metrics['discount']:,.0f}",
                'Last Week': f"₹{lw_metrics['discount']:,.0f}",
                'Growth%': f"{self.calculate_growth(today_metrics['discount'], lw_metrics['discount']):.2f}%",
                'Last Month': f"₹{lm_metrics['discount']:,.0f}",
                'Growth%.1': f"{self.calculate_growth(today_metrics['discount'], lm_metrics['discount']):.2f}%",
                'MTD': f"₹{mtd_metrics['discount']:,.0f}",
                'LMTD': f"₹{lmtd_metrics['discount']:,.0f}",
                'Growth%.2': f"{self.calculate_growth(mtd_metrics['discount'], lmtd_metrics['discount']):.2f}%",
                'Last Year': f"₹{ly_metrics['discount']:,.0f}",
                'Growth%.3': f"{self.calculate_growth(today_metrics['discount'], ly_metrics['discount']):.2f}%"
            },
            'Dis%': {
                'Today': f"{today_metrics['dis_pct']:.2f}%",
                'Last Week': f"{lw_metrics['dis_pct']:.2f}%",
                'Growth%': f"{self.calculate_growth(today_metrics['dis_pct'], lw_metrics['dis_pct']):.2f}%",
                'Last Month': f"{lm_metrics['dis_pct']:.2f}%",
                'Growth%.1': f"{self.calculate_growth(today_metrics['dis_pct'], lm_metrics['dis_pct']):.2f}%",
                'MTD': f"{mtd_metrics['dis_pct']:.2f}%",
                'LMTD': f"{lmtd_metrics['dis_pct']:.2f}%",
                'Growth%.2': f"{self.calculate_growth(mtd_metrics['dis_pct'], lmtd_metrics['dis_pct']):.2f}%",
                'Last Year': f"{ly_metrics['dis_pct']:.2f}%",
                'Growth%.3': f"{self.calculate_growth(today_metrics['dis_pct'], ly_metrics['dis_pct']):.2f}%"
            },
            'AOV': {
                'Today': f"₹{today_metrics['aov']:,.0f}",
                'Last Week': f"₹{lw_metrics['aov']:,.0f}",
                'Growth%': f"{self.calculate_growth(today_metrics['aov'], lw_metrics['aov']):.2f}%",
                'Last Month': f"₹{lm_metrics['aov']:,.0f}",
                'Growth%.1': f"{self.calculate_growth(today_metrics['aov'], lm_metrics['aov']):.2f}%",
                'MTD': f"₹{mtd_metrics['aov']:,.0f}",
                'LMTD': f"₹{lmtd_metrics['aov']:,.0f}",
                'Growth%.2': f"{self.calculate_growth(mtd_metrics['aov'], lmtd_metrics['aov']):.2f}%",
                'Last Year': f"₹{ly_metrics['aov']:,.0f}",
                'Growth%.3': f"{self.calculate_growth(today_metrics['aov'], ly_metrics['aov']):.2f}%"
            },
            'Offline %': {
                'Today': f"{today_metrics['offline_pct']:.2f}%",
                'Last Week': f"{lw_metrics['offline_pct']:.2f}%",
                'Growth%': f"{self.calculate_growth(today_metrics['offline_pct'], lw_metrics['offline_pct']):.2f}%",
                'Last Month': f"{lm_metrics['offline_pct']:.2f}%",
                'Growth%.1': f"{self.calculate_growth(today_metrics['offline_pct'], lm_metrics['offline_pct']):.2f}%",
                'MTD': f"{mtd_metrics['offline_pct']:.2f}%",
                'LMTD': f"{lmtd_metrics['offline_pct']:.2f}%",
                'Growth%.2': f"{self.calculate_growth(mtd_metrics['offline_pct'], lmtd_metrics['offline_pct']):.2f}%",
                'Last Year': f"{ly_metrics['offline_pct']:.2f}%",
                'Growth%.3': f"{self.calculate_growth(today_metrics['offline_pct'], ly_metrics['offline_pct']):.2f}%"
            },
            'Online %': {
                'Today': f"{today_metrics['online_pct']:.2f}%",
                'Last Week': f"{lw_metrics['online_pct']:.2f}%",
                'Growth%': f"{self.calculate_growth(today_metrics['online_pct'], lw_metrics['online_pct']):.2f}%",
                'Last Month': f"{lm_metrics['online_pct']:.2f}%",
                'Growth%.1': f"{self.calculate_growth(today_metrics['online_pct'], lm_metrics['online_pct']):.2f}%",
                'MTD': f"{mtd_metrics['online_pct']:.2f}%",
                'LMTD': f"{lmtd_metrics['online_pct']:.2f}%",
                'Growth%.2': f"{self.calculate_growth(mtd_metrics['online_pct'], lmtd_metrics['online_pct']):.2f}%",
                'Last Year': f"{ly_metrics['online_pct']:.2f}%",
                'Growth%.3': f"{self.calculate_growth(today_metrics['online_pct'], ly_metrics['online_pct']):.2f}%"
            }
        }
        
        return pd.DataFrame(summary_data).T
    
    # ==================== DIMENSION-WISE SUMMARIES ====================
    
    def get_dimension_summary(self, dimension: str, store_type=None) -> pd.DataFrame:
        """Get summary by dimension (Brand, Region, Source, Session)"""
        
        # Filter to COCO only
        data = self.df[self.df['Store Type'] == 'COCO'].copy()
        
        # Today
        today_data = data[data['Date'].dt.date == self.today]
        today_summary = today_data.groupby(dimension)[['Net Sales', 'Orders', 'Discount']].sum()
        
        # Last Week (same day)
        lw_date = (pd.Timestamp(self.today) - timedelta(days=7)).date()
        lw_data = data[data['Date'].dt.date == lw_date]
        lw_summary = lw_data.groupby(dimension)[['Net Sales', 'Orders', 'Discount']].sum()
        
        # Last Month (same day)
        lm_date = (pd.Timestamp(self.today) - pd.DateOffset(months=1)).date()
        lm_data = data[data['Date'].dt.date == lm_date]
        lm_summary = lm_data.groupby(dimension)[['Net Sales', 'Orders', 'Discount']].sum()
        
        # MTD
        mtd_start = pd.Timestamp(self.today).replace(day=1).date()
        mtd_data = data[(data['Date'].dt.date >= mtd_start) & (data['Date'].dt.date <= self.today)]
        mtd_summary = mtd_data.groupby(dimension)[['Net Sales', 'Orders', 'Discount']].sum()
        
        # LMTD
        lmtd_month = pd.Timestamp(self.today) - pd.DateOffset(months=1)
        lmtd_start = lmtd_month.replace(day=1).date()
        lmtd_end = lmtd_month.date()
        lmtd_data = data[(data['Date'].dt.date >= lmtd_start) & (data['Date'].dt.date <= lmtd_end)]
        lmtd_summary = lmtd_data.groupby(dimension)[['Net Sales', 'Orders', 'Discount']].sum()
        
        # Last Year (same day)
        ly_date = (pd.Timestamp(self.today) - pd.DateOffset(years=1)).date()
        ly_data = data[data['Date'].dt.date == ly_date]
        ly_summary = ly_data.groupby(dimension)[['Net Sales', 'Orders', 'Discount']].sum()
        
        # Combine and calculate growth
        result_data = []
        all_keys = set(today_summary.index) | set(lw_summary.index) | set(lm_summary.index)
        
        for key in sorted(all_keys):
            row = {
                'Name': key,
                'Today': today_summary.loc[key, 'Net Sales'] if key in today_summary.index else 0,
                'Last Week': lw_summary.loc[key, 'Net Sales'] if key in lw_summary.index else 0,
                'Last Month': lm_summary.loc[key, 'Net Sales'] if key in lm_summary.index else 0,
                'MTD': mtd_summary.loc[key, 'Net Sales'] if key in mtd_summary.index else 0,
                'LMTD': lmtd_summary.loc[key, 'Net Sales'] if key in lmtd_summary.index else 0,
                'Last Year': ly_summary.loc[key, 'Net Sales'] if key in ly_summary.index else 0,
            }
            
            row['Growth% LW'] = self.calculate_growth(row['Today'], row['Last Week'])
            row['Growth% LM'] = self.calculate_growth(row['Today'], row['Last Month'])
            row['Growth% MTD'] = self.calculate_growth(row['MTD'], row['LMTD'])
            row['Growth% LY'] = self.calculate_growth(row['Today'], row['Last Year'])
            
            result_data.append(row)
        
        return pd.DataFrame(result_data)
    
    # ==================== BUCKET ANALYSIS ====================
    
    def get_bucket_analysis(self, bucket_type: str, store_type='COCO') -> pd.DataFrame:
        """Get discount or AOV bucket analysis"""
        
        data = self.df[self.df['Store Type'] == store_type].copy()
        bucket_col = f"{bucket_type} Bucket"
        
        # FTD (Today)
        ftd_data = data[data['Date'].dt.date == self.today]
        ftd_overall = ftd_data.groupby(bucket_col)['Net Sales'].sum()
        ftd_instore = ftd_data[ftd_data['Source'] == 'In Store'].groupby(bucket_col)['Net Sales'].sum()
        ftd_swiggy = ftd_data[ftd_data['Source'] == 'Swiggy'].groupby(bucket_col)['Net Sales'].sum()
        ftd_zomato = ftd_data[ftd_data['Source'] == 'Zomato'].groupby(bucket_col)['Net Sales'].sum()
        
        # MTD
        mtd_start = pd.Timestamp(self.today).replace(day=1).date()
        mtd_data = data[(data['Date'].dt.date >= mtd_start) & (data['Date'].dt.date <= self.today)]
        mtd_overall = mtd_data.groupby(bucket_col)['Net Sales'].sum()
        mtd_instore = mtd_data[mtd_data['Source'] == 'In Store'].groupby(bucket_col)['Net Sales'].sum()
        mtd_swiggy = mtd_data[mtd_data['Source'] == 'Swiggy'].groupby(bucket_col)['Net Sales'].sum()
        mtd_zomato = mtd_data[mtd_data['Source'] == 'Zomato'].groupby(bucket_col)['Net Sales'].sum()
        
        # Combine
        result = pd.DataFrame({
            'Bucket': ftd_overall.index,
            'FTD Overall': ftd_overall.values,
            'FTD In Store': [ftd_instore.get(b, 0) for b in ftd_overall.index],
            'FTD Swiggy': [ftd_swiggy.get(b, 0) for b in ftd_overall.index],
            'FTD Zomato': [ftd_zomato.get(b, 0) for b in ftd_overall.index],
            'MTD Overall': [mtd_overall.get(b, 0) for b in ftd_overall.index],
            'MTD In Store': [mtd_instore.get(b, 0) for b in ftd_overall.index],
            'MTD Swiggy': [mtd_swiggy.get(b, 0) for b in ftd_overall.index],
            'MTD Zomato': [mtd_zomato.get(b, 0) for b in ftd_overall.index],
        })
        
        return result.set_index('Bucket')
    
    # ==================== TOP/BOTTOM STORES ====================
    
    def get_top_bottom_stores(self, store_type='COCO', limit=10) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """Get top and bottom stores by net sales"""
        
        data = self.df[self.df['Store Type'] == store_type].copy()
        today_data = data[data['Date'].dt.date == self.today]
        
        branch_summary = today_data.groupby('Branch').agg({
            'Net Sales': 'sum',
            'Dis %': 'mean',
            'Orders': 'sum',
            'Source': lambda x: (x == 'In Store').sum() / len(x) * 100 if len(x) > 0 else 0
        }).reset_index()
        
        branch_summary.columns = ['Branch', 'Net Sales', 'Dis%', 'Orders', 'Offline%']
        branch_summary['Online%'] = 100 - branch_summary['Offline%']
        branch_summary = branch_summary.sort_values('Net Sales', ascending=False)
        
        top = branch_summary.head(limit)
        bottom = branch_summary.tail(limit).sort_values('Net Sales')
        
        return top, bottom
    
    # ==================== DAY LEVEL PERFORMANCE ====================
    
    def get_day_level_performance(self, store_type='COCO') -> pd.DataFrame:
        """Get day level performance for current month"""
        
        data = self.df[self.df['Store Type'] == store_type].copy()
        mtd_start = pd.Timestamp(self.today).replace(day=1).date()
        mtd_data = data[(data['Date'].dt.date >= mtd_start) & (data['Date'].dt.date <= self.today)]
        
        daily_perf = mtd_data.groupby(mtd_data['Date'].dt.date).agg({
            'Net Sales': 'sum',
            'Orders': 'sum',
            'Discount': 'sum',
            'Dis %': 'mean'
        }).reset_index()
        
        daily_perf.columns = ['Date', 'Net Sales', 'Orders', 'Discount', 'Dis%']
        daily_perf['AOV'] = daily_perf['Net Sales'] / daily_perf['Orders']
        
        return daily_perf
    
    # ==================== HTML GENERATION ====================
    
    def generate_html_dashboard(self, overall_summary: pd.DataFrame) -> str:
        """Generate HTML dashboard"""
        
        html = """
        <!DOCTYPE html>
        <html>
        <head>
            <meta charset="UTF-8">
            <title>Daily Sales Report Dashboard</title>
            <style>
                * { margin: 0; padding: 0; box-sizing: border-box; }
                body { 
                    font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
                    background-color: #f5f5f5;
                    padding: 20px;
                    line-height: 1.6;
                    color: #333;
                }
                .container { max-width: 1400px; margin: 0 auto; }
                
                /* Header */
                .header {
                    background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
                    color: white;
                    padding: 30px;
                    border-radius: 10px;
                    margin-bottom: 30px;
                    text-align: center;
                }
                .header h1 { font-size: 2.5em; margin-bottom: 10px; }
                .header p { font-size: 1.1em; opacity: 0.9; }
                
                /* KPI Cards */
                .kpi-container { display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 20px; margin-bottom: 30px; }
                .kpi-card {
                    background: white;
                    border-radius: 10px;
                    padding: 20px;
                    box-shadow: 0 2px 10px rgba(0,0,0,0.1);
                    border-left: 5px solid #667eea;
                }
                .kpi-label { color: #666; font-size: 0.9em; margin-bottom: 10px; }
                .kpi-value { font-size: 2em; font-weight: bold; color: #667eea; }
                .kpi-subtitle { color: #999; font-size: 0.85em; margin-top: 10px; }
                
                /* Tables */
                .section {
                    background: white;
                    border-radius: 10px;
                    padding: 25px;
                    margin-bottom: 25px;
                    box-shadow: 0 2px 10px rgba(0,0,0,0.1);
                }
                .section-title {
                    font-size: 1.5em;
                    font-weight: bold;
                    margin-bottom: 20px;
                    color: #333;
                    border-bottom: 3px solid #667eea;
                    padding-bottom: 10px;
                }
                
                table {
                    width: 100%;
                    border-collapse: collapse;
                    font-size: 0.95em;
                }
                table th {
                    background-color: #f0f0f0;
                    padding: 12px;
                    text-align: left;
                    font-weight: bold;
                    color: #333;
                    border-bottom: 2px solid #ddd;
                }
                table td {
                    padding: 10px 12px;
                    border-bottom: 1px solid #eee;
                }
                table tr:hover { background-color: #f9f9f9; }
                table tr:nth-child(even) { background-color: #fafafa; }
                
                /* Growth Colors */
                .growth-positive { background-color: #d4edda !important; color: #155724; }
                .growth-negative { background-color: #f8d7da !important; color: #721c24; }
                
                /* Store Type Toggle */
                .toggle-container {
                    margin-bottom: 30px;
                    text-align: center;
                }
                .toggle-button {
                    padding: 10px 20px;
                    margin: 0 10px;
                    border: 2px solid #667eea;
                    background: white;
                    color: #667eea;
                    border-radius: 5px;
                    cursor: pointer;
                    font-size: 1em;
                    font-weight: bold;
                    transition: all 0.3s ease;
                }
                .toggle-button.active {
                    background: #667eea;
                    color: white;
                }
                
                /* Footer */
                .footer {
                    text-align: center;
                    color: #999;
                    font-size: 0.85em;
                    margin-top: 40px;
                    padding-top: 20px;
                    border-top: 1px solid #ddd;
                }
                
                /* Overall + COCO Tabs */
                .tab-content { display: none; }
                .tab-content.active { display: block; }
                
                .metrics-row {
                    display: grid;
                    grid-template-columns: repeat(auto-fit, minmax(300px, 1fr));
                    gap: 15px;
                    margin-bottom: 20px;
                }
                .metric-card {
                    background: #f9f9f9;
                    padding: 15px;
                    border-radius: 5px;
                    border-left: 4px solid #667eea;
                }
                .metric-card .label { font-size: 0.9em; color: #666; }
                .metric-card .value { font-size: 1.3em; font-weight: bold; color: #333; margin: 5px 0; }
            </style>
        </head>
        <body>
            <div class="container">
                <div class="header">
                    <h1>📊 Daily Sales Report (DSR) Dashboard</h1>
                    <p>"""
        
        html += f"{pd.Timestamp(self.today).strftime('%A, %B %d, %Y')}"
        html += """</p>
                </div>
                
                <div class="toggle-container">
                    <button class="toggle-button active" onclick="showTab('overall')">Overall</button>
                    <button class="toggle-button" onclick="showTab('coco')">COCO Stores Only</button>
                </div>
                
                <!-- OVERALL TAB -->
                <div id="overall" class="tab-content active">
                    <div class="section">
                        <div class="section-title">📈 KPI Cards - Overall</div>
                        <div class="kpi-container">
        """
        
        # Add KPI cards for overall
        kpi_overall = self.get_kpi_cards_data(store_type=None)
        html += self._generate_kpi_html(kpi_overall)
        
        html += """
                        </div>
                    </div>
                    
                    <div class="section">
                        <div class="section-title">📊 Overall Summary</div>
        """
        html += self._generate_table_html(overall_summary)
        html += """
                    </div>
                </div>
                
                <!-- COCO TAB -->
                <div id="coco" class="tab-content">
                    <div class="section">
                        <div class="section-title">📈 KPI Cards - COCO Stores</div>
                        <div class="kpi-container">
        """
        
        # Add KPI cards for COCO
        kpi_coco = self.get_kpi_cards_data(store_type='COCO')
        html += self._generate_kpi_html(kpi_coco)
        
        html += """
                        </div>
                    </div>
                    
                    <div class="section">
                        <div class="section-title">📊 COCO Sales Summary</div>
        """
        coco_summary = self.get_overall_summary(store_type='COCO')
        html += self._generate_table_html(coco_summary)
        
        # Brand Summary
        html += """
                    </div>
                    
                    <div class="section">
                        <div class="section-title">🏷️ Brand Sales Summary (COCO)</div>
        """
        brand_summary = self.get_dimension_summary('Brand Name', store_type='COCO')
        html += self._generate_table_html(brand_summary)
        
        # Region Summary
        html += """
                    </div>
                    
                    <div class="section">
                        <div class="section-title">📍 Region Sales Summary (COCO)</div>
        """
        region_summary = self.get_dimension_summary('Region', store_type='COCO')
        html += self._generate_table_html(region_summary)
        
        # Source Summary
        html += """
                    </div>
                    
                    <div class="section">
                        <div class="section-title">🛒 Source Sales Summary (COCO)</div>
        """
        source_summary = self.get_dimension_summary('Source', store_type='COCO')
        html += self._generate_table_html(source_summary)
        
        # Session Summary
        html += """
                    </div>
                    
                    <div class="section">
                        <div class="section-title">⏰ Session Sales Summary (COCO)</div>
        """
        session_summary = self.get_dimension_summary('Session', store_type='COCO')
        html += self._generate_table_html(session_summary)
        
        # Discount Bucket
        html += """
                    </div>
                    
                    <div class="section">
                        <div class="section-title">🏷️ Discount Bucket Analysis (COCO)</div>
        """
        discount_bucket = self.get_bucket_analysis('Discount', store_type='COCO')
        html += self._generate_table_html(discount_bucket)
        
        # AOV Bucket
        html += """
                    </div>
                    
                    <div class="section">
                        <div class="section-title">💰 AOV Bucket Analysis (COCO)</div>
        """
        aov_bucket = self.get_bucket_analysis('AOV', store_type='COCO')
        html += self._generate_table_html(aov_bucket)
        
        # Day Level Performance
        html += """
                    </div>
                    
                    <div class="section">
                        <div class="section-title">📅 Day Level Performance (COCO)</div>
        """
        day_perf = self.get_day_level_performance(store_type='COCO')
        html += self._generate_table_html(day_perf)
        
        # Top and Bottom Stores
        html += """
                    </div>
                    
                    <div class="section">
                        <div class="section-title">🏆 Top 10 Stores (COCO)</div>
        """
        top_stores, _ = self.get_top_bottom_stores(store_type='COCO', limit=10)
        html += self._generate_table_html(top_stores)
        
        html += """
                    </div>
                    
                    <div class="section">
                        <div class="section-title">📉 Bottom 10 Stores (COCO)</div>
        """
        _, bottom_stores = self.get_top_bottom_stores(store_type='COCO', limit=10)
        html += self._generate_table_html(bottom_stores)
        
        html += """
                    </div>
                </div>
                
                <div class="footer">
                    <p>Generated on """ + datetime.now().strftime('%Y-%m-%d %H:%M:%S') + """</p>
                    <p>This is an automated report. Please do not reply.</p>
                </div>
            </div>
            
            <script>
                function showTab(tabName) {
                    // Hide all tabs
                    var tabs = document.getElementsByClassName('tab-content');
                    for (var i = 0; i < tabs.length; i++) {
                        tabs[i].classList.remove('active');
                    }
                    
                    // Remove active class from buttons
                    var buttons = document.getElementsByClassName('toggle-button');
                    for (var i = 0; i < buttons.length; i++) {
                        buttons[i].classList.remove('active');
                    }
                    
                    // Show selected tab
                    document.getElementById(tabName).classList.add('active');
                    
                    // Add active class to button
                    event.target.classList.add('active');
                }
            </script>
        </body>
        </html>
        """
        
        return html
    
    def _generate_kpi_html(self, kpi_data: Dict) -> str:
        """Generate KPI cards HTML"""
        html = ""
        
        kpis = [
            ('Net Sales', f"₹{kpi_data['net_sales_lacs']:.2f}L", 'Lacs'),
            ('Orders', f"{int(kpi_data['orders'])}", 'Count'),
            ('Dis %', f"{kpi_data['dis_pct']:.2f}%", 'Percentage'),
            ('AOV', f"₹{kpi_data['aov']:,.0f}", 'Average'),
            ('MoM%', f"{kpi_data['mom_growth']:.2f}%", 'Growth'),
            ('YoY%', f"{kpi_data['yoy_growth']:.2f}%", 'Growth'),
        ]
        
        for label, value, subtitle in kpis:
            html += f"""
            <div class="kpi-card">
                <div class="kpi-label">{label}</div>
                <div class="kpi-value">{value}</div>
                <div class="kpi-subtitle">{subtitle}</div>
            </div>
            """
        
        return html
    
    def _generate_table_html(self, df: pd.DataFrame) -> str:
        """Generate table HTML from DataFrame"""
        html = "<table><thead><tr>"
        
        # Headers
        for col in df.columns:
            html += f"<th>{col}</th>"
        html += "</tr></thead><tbody>"
        
        # Rows
        for _, row in df.iterrows():
            html += "<tr>"
            for val in row:
                # Color code growth columns
                if isinstance(val, str) and '%' in val:
                    try:
                        growth = float(val.replace('%', ''))
                        if growth < 0:
                            html += f'<td class="growth-negative">{val}</td>'
                        else:
                            html += f'<td class="growth-positive">{val}</td>'
                    except:
                        html += f"<td>{val}</td>"
                else:
                    html += f"<td>{val}</td>"
            html += "</tr>"
        
        html += "</tbody></table>"
        return html
    
    # ==================== EMAIL GENERATION ====================
    
    def send_dashboard_email(self):
        """Generate and send dashboard email"""
        try:
            logger.info("Generating dashboard HTML...")
            overall_summary = self.get_overall_summary()
            html_content = self.generate_html_dashboard(overall_summary)
            
            # Create email
            msg = MIMEMultipart('alternative')
            msg['Subject'] = f"Daily Sales Report - {pd.Timestamp(self.today).strftime('%A, %B %d, %Y')}"
            msg['From'] = EMAIL_CONFIG['sender_email']
            msg['To'] = ', '.join(EMAIL_CONFIG['recipients'])
            
            # Attach HTML
            msg.attach(MIMEText(html_content, 'html'))
            
            # Send email
            logger.info("Sending email...")
            with smtplib.SMTP(EMAIL_CONFIG['smtp_server'], EMAIL_CONFIG['smtp_port']) as server:
                server.starttls()
                server.login(EMAIL_CONFIG['sender_email'], EMAIL_CONFIG['sender_password'])
                server.send_message(msg)
            
            logger.info(f"✅ Dashboard sent successfully to {EMAIL_CONFIG['recipients']}")
            return True
            
        except Exception as e:
            logger.error(f"❌ Error sending email: {e}")
            return False
    
    def generate_excel_dashboard(self, output_path: str = 'dsr_dashboard.xlsx'):
        """Generate Excel dashboard with multiple sheets"""
        try:
            logger.info("Generating Excel dashboard...")
            
            with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
                # KPI Overview
                kpi_coco = self.get_kpi_cards_data(store_type='COCO')
                kpi_df = pd.DataFrame({
                    'Metric': ['Net Sales (Lacs)', 'Orders', 'Dis %', 'AOV', 'MoM%', 'YoY%'],
                    'Value': [
                        kpi_coco['net_sales_lacs'],
                        kpi_coco['orders'],
                        kpi_coco['dis_pct'],
                        kpi_coco['aov'],
                        kpi_coco['mom_growth'],
                        kpi_coco['yoy_growth']
                    ]
                })
                kpi_df.to_excel(writer, sheet_name='KPI', index=False)
                
                # Overall Summary
                self.get_overall_summary().to_excel(writer, sheet_name='Overall Summary')
                
                # COCO Summary
                self.get_overall_summary(store_type='COCO').to_excel(writer, sheet_name='COCO Summary')
                
                # Brand Analysis
                self.get_dimension_summary('Brand Name').to_excel(writer, sheet_name='Brand Analysis')
                
                # Region Analysis
                self.get_dimension_summary('Region').to_excel(writer, sheet_name='Region Analysis')
                
                # Source Analysis
                self.get_dimension_summary('Source').to_excel(writer, sheet_name='Source Analysis')
                
                # Session Analysis
                self.get_dimension_summary('Session').to_excel(writer, sheet_name='Session Analysis')
                
                # Discount Bucket
                self.get_bucket_analysis('Discount').to_excel(writer, sheet_name='Discount Bucket')
                
                # AOV Bucket
                self.get_bucket_analysis('AOV').to_excel(writer, sheet_name='AOV Bucket')
                
                # Day Level Performance
                self.get_day_level_performance().to_excel(writer, sheet_name='Day Performance', index=False)
                
                # Top Stores
                top, _ = self.get_top_bottom_stores()
                top.to_excel(writer, sheet_name='Top Stores', index=False)
                
                # Bottom Stores
                _, bottom = self.get_top_bottom_stores()
                bottom.to_excel(writer, sheet_name='Bottom Stores', index=False)
            
            logger.info(f"✅ Excel dashboard saved to {output_path}")
            return output_path
            
        except Exception as e:
            logger.error(f"❌ Error generating Excel: {e}")
            return None

def main():
    """Main execution function"""
    logger.info("=" * 60)
    logger.info("DAILY SALES REPORT (DSR) DASHBOARD")
    logger.info("=" * 60)
    
    try:
        # Initialize dashboard
        dashboard = DSRDashboard()
        
        # Generate Excel
        excel_file = dashboard.generate_excel_dashboard()
        
        # Send email with HTML
        dashboard.send_dashboard_email()
        
        logger.info("=" * 60)
        logger.info("✅ DSR Dashboard process completed successfully!")
        logger.info("=" * 60)
        
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        raise

if __name__ == "__main__":
    main()
