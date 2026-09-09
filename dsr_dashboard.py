"""
Daily Sales Report (DSR) Dashboard
Fixed: String formatting type errors, forced target date handling, Dis%/Offline%/Online% formatting, 
and full percentage contribution columns for bucket breakdowns.
"""

import os
import glob
import gzip
import logging
import warnings
from typing import Tuple
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

warnings.filterwarnings('ignore')

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

EMAIL_CONFIG = {
    'sender_email': os.getenv('SENDER_EMAIL', 'your-email@gmail.com'),
    'sender_password': os.getenv('EMAIL_PASSWORD', 'your-app-password'),
    'email_to': os.getenv('EMAIL_TO', 'mis2@frozenbottle.in'),
    'email_cc': os.getenv('EMAIL_CC', 'mis2@frozenbottle.in'),
    'smtp_server': 'smtp.gmail.com',
    'smtp_port': 587
}

HISTORICAL_DATA_FILE = 'historical_data/historical_sales.csv.gz'
MONTHLY_DATA_DIR = 'monthly_data'

class DSRDashboard:
    def __init__(self):
        self.df = None
        self.today = None
        self.load_data()
        
    def load_data(self):
        try:
            logger.info("Loading sales data...")
            dataframes = []

            if os.path.exists(HISTORICAL_DATA_FILE):
                with gzip.open(HISTORICAL_DATA_FILE, 'rt') as f:
                    dataframes.append(pd.read_csv(f))

            monthly_files = glob.glob(os.path.join(MONTHLY_DATA_DIR, '**', '*.csv'), recursive=True)
            for m_file in monthly_files:
                dataframes.append(pd.read_csv(m_file))

            if not dataframes:
                raise FileNotFoundError("No sales data files found!")

            self.df = pd.concat(dataframes, ignore_index=True)

            date_col = [col for col in self.df.columns if col.lower() in ['date', 'sales_date']][0]
            self.df['Date'] = pd.to_datetime(self.df[date_col])
            
            for col in ['Session', 'Source', 'Brand Name', 'Store Type', 'Discount Bucket', 'AOV Bucket']:
                if col in self.df.columns:
                    self.df[col] = self.df[col].astype(str).str.strip()

            dedup_cols = [c for c in ['Date', 'Branch', 'Source', 'Session', 'Brand Name', 'Store Type'] if c in self.df.columns]
            if dedup_cols:
                self.df.drop_duplicates(subset=dedup_cols, keep='last', inplace=True)

            # Explicitly force report date to yesterday (8th Sep 2026 when running on 9th Sep)
            self.today = datetime(2026, 9, 8).date()
            
            for col in ['Net Sales', 'Discount', 'Taxes', 'Gross Sales', 'Quantity', 'Orders']:
                if col in self.df.columns:
                    self.df[col] = pd.to_numeric(self.df[col], errors='coerce').fillna(0)
                    
            logger.info(f"✓ Data loaded successfully. Report Target Date: {self.today}")
        except Exception as e:
            logger.error(f"Error loading data: {e}")
            raise

    def calculate_growth(self, current: float, previous: float) -> float:
        if previous == 0 or pd.isna(previous):
            return 0.0
        return ((current - previous) / previous) * 100

    def format_growth_html(self, val: float) -> str:
        if pd.isna(val):
            return "0.00%"
        color = "#d4edda" if val >= 0 else "#f8d7da"
        text_color = "#155724" if val >= 0 else "#721c24"
        symbol = "↑" if val >= 0 else "↓"
        return f'<span style="background-color: {color}; color: {text_color}; padding: 3px 6px; border-radius: 4px; font-weight: bold;">{symbol} {abs(val):.2f}%</span>'

    def get_metrics_dict(self, data: pd.DataFrame) -> dict:
        if data.empty:
            return {'net_sales': 0, 'discount': 0, 'orders': 0, 'dis_pct': 0, 'aov': 0, 'offline_pct': 0, 'online_pct': 0}
        
        net_sales = data['Net Sales'].sum()
        discount = data['Discount'].sum()
        orders = data['Orders'].sum()
        gross_sales = data['Gross Sales'].sum() if 'Gross Sales' in data.columns else (net_sales + discount)
        
        dis_pct = (discount / gross_sales * 100) if gross_sales > 0 else 0
        aov = (net_sales / orders) if orders > 0 else 0
        
        offline_sales = data[data['Source'].str.lower() == 'in store']['Net Sales'].sum()
        total_sales = data['Net Sales'].sum()
        
        offline_pct = (offline_sales / total_sales * 100) if total_sales > 0 else 0
        online_pct = 100.0 - offline_pct if total_sales > 0 else 0
        
        return {
            'net_sales': net_sales,
            'discount': discount,
            'orders': orders,
            'dis_pct': dis_pct,
            'aov': aov,
            'offline_pct': offline_pct,
            'online_pct': online_pct
        }

    def get_kpi_cards_data(self) -> dict:
        ftd_data = self.df[self.df['Date'].dt.date == self.today]
        ftd = self.get_metrics_dict(ftd_data)
        
        mtd_start = self.today.replace(day=1)
        mtd_data = self.df[(self.df['Date'].dt.date >= mtd_start) & (self.df['Date'].dt.date <= self.today)]
        mtd = self.get_metrics_dict(mtd_data)
        
        lmtd_end = self.today - pd.DateOffset(months=1)
        lmtd_start = lmtd_end.replace(day=1)
        lmtd_data = self.df[(self.df['Date'].dt.date >= lmtd_start.date()) & (self.df['Date'].dt.date <= lmtd_end.date())]
        lmtd = self.get_metrics_dict(lmtd_data)
        
        lytd_end = self.today - pd.DateOffset(years=1)
        lytd_start = lytd_end.replace(day=1)
        lytd_data = self.df[(self.df['Date'].dt.date >= lytd_start.date()) & (self.df['Date'].dt.date <= lytd_end.date())]
        lytd = self.get_metrics_dict(lytd_data)
        
        mom = self.calculate_growth(mtd['net_sales'], lmtd['net_sales'])
        yoy = self.calculate_growth(mtd['net_sales'], lytd['net_sales'])
        
        return {
            'net_sales_lacs': ftd['net_sales'] / 100000.0,
            'orders': ftd['orders'],
            'dis_pct': ftd['dis_pct'],
            'aov': ftd['aov'],
            'mom_growth': mom,
            'yoy_growth': yoy
        }

    def get_summary_table(self, store_type=None) -> pd.DataFrame:
        df_filtered = self.df if store_type is None else self.df[self.df['Store Type'] == store_type]
        
        target_day = self.today
        last_week_day = target_day - timedelta(days=7) # Target 1st Sept when Yesterday is 8th Sept
        
        mtd_start = target_day.replace(day=1)
        
        last_month_same_day = (target_day - pd.DateOffset(months=1)).date()
        lmtd_end = last_month_same_day
        lmtd_start = lmtd_end.replace(day=1)
        
        last_year_same_day = (target_day - pd.DateOffset(years=1)).date()
        ly_mtd_end = last_year_same_day
        ly_mtd_start = ly_mtd_end.replace(day=1)
        
        yest_m = self.get_metrics_dict(df_filtered[df_filtered['Date'].dt.date == target_day])
        lw_m = self.get_metrics_dict(df_filtered[df_filtered['Date'].dt.date == last_week_day])
        lm_m = self.get_metrics_dict(df_filtered[df_filtered['Date'].dt.date == last_month_same_day])
        
        mtd_m = self.get_metrics_dict(df_filtered[(df_filtered['Date'].dt.date >= mtd_start) & (df_filtered['Date'].dt.date <= target_day)])
        lmtd_m = self.get_metrics_dict(df_filtered[(df_filtered['Date'].dt.date >= lmtd_start) & (df_filtered['Date'].dt.date <= lmtd_end)])
        ly_mtd_m = self.get_metrics_dict(df_filtered[(df_filtered['Date'].dt.date >= ly_mtd_start) & (df_filtered['Date'].dt.date <= ly_mtd_end)])
        
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

    def get_dimension_summary(self, dimension: str) -> pd.DataFrame:
        data = self.df[self.df['Store Type'] == 'COCO'].copy()
        
        target_day = self.today
        last_week_day = target_day - timedelta(days=7)
        last_month_same_day = (target_day - pd.DateOffset(months=1)).date()
        
        mtd_start = target_day.replace(day=1)
        lmtd_end = last_month_same_day
        lmtd_start = lmtd_end.replace(day=1)
        
        ly_mtd_end = (target_day - pd.DateOffset(years=1)).date()
        ly_mtd_start = ly_mtd_end.replace(day=1)
        
        def group_sales(df_slice):
            if df_slice.empty:
                return pd.Series(dtype=float)
            return df_slice.groupby(dimension)['Net Sales'].sum()
        
        yest_s = group_sales(data[data['Date'].dt.date == target_day])
        lw_s = group_sales(data[data['Date'].dt.date == last_week_day])
        lm_s = group_sales(data[data['Date'].dt.date == last_month_same_day])
        
        mtd_s = group_sales(data[(data['Date'].dt.date >= mtd_start) & (data['Date'].dt.date <= target_day)])
        lmtd_s = group_sales(data[(data['Date'].dt.date >= lmtd_start) & (data['Date'].dt.date <= lmtd_end)])
        ly_s = group_sales(data[(data['Date'].dt.date >= ly_mtd_start) & (data['Date'].dt.date <= ly_mtd_end)])
        
        if dimension == 'Session':
            session_order = ['Breakfast', 'Lunch', 'Snacks', 'Dinner', 'Post Dinner', 'Late Night', 'Closing']
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

    def get_bucket_analysis(self, bucket_col: str) -> pd.DataFrame:
        data = self.df[self.df['Store Type'] == 'COCO'].copy()
        
        ftd_data = data[data['Date'].dt.date == self.today]
        mtd_start = self.today.replace(day=1)
        mtd_data = data[(data['Date'].dt.date >= mtd_start) & (data['Date'].dt.date <= self.today)]
        
        ftd_total = ftd_data['Net Sales'].sum()
        mtd_total = mtd_data['Net Sales'].sum()
        
        buckets = sorted(list(data[bucket_col].dropna().unique()))
        rows = []
        
        for b in buckets:
            f_b = ftd_data[ftd_data[bucket_col] == b]
            m_b = mtd_data[mtd_data[bucket_col] == b]
            
            f_overall = f_b['Net Sales'].sum()
            f_instore = f_b[f_b['Source'].str.lower() == 'in store']['Net Sales'].sum()
            f_swiggy = f_b[f_b['Source'].str.lower() == 'swiggy']['Net Sales'].sum()
            f_zomato = f_b[f_b['Source'].str.lower() == 'zomato']['Net Sales'].sum()
            
            m_overall = m_b['Net Sales'].sum()
            m_instore = m_b[m_b['Source'].str.lower() == 'in store']['Net Sales'].sum()
            m_swiggy = m_b[m_b['Source'].str.lower() == 'swiggy']['Net Sales'].sum()
            m_zomato = m_b[m_b['Source'].str.lower() == 'zomato']['Net Sales'].sum()
            
            rows.append({
                bucket_col: b,
                'FTD Overall Contrib%': (f_overall / ftd_total * 100) if ftd_total > 0 else 0,
                'FTD In Store Contrib%': (f_instore / ftd_total * 100) if ftd_total > 0 else 0,
                'FTD Swiggy Contrib%': (f_swiggy / ftd_total * 100) if ftd_total > 0 else 0,
                'FTD Zomato Contrib%': (f_zomato / ftd_total * 100) if ftd_total > 0 else 0,
                'MTD Overall Contrib%': (m_overall / mtd_total * 100) if mtd_total > 0 else 0,
                'MTD In Store Contrib%': (m_instore / mtd_total * 100) if mtd_total > 0 else 0,
                'MTD Swiggy Contrib%': (m_swiggy / mtd_total * 100) if mtd_total > 0 else 0,
                'MTD Zomato Contrib%': (m_zomato / mtd_total * 100) if mtd_total > 0 else 0,
            })
            
        return pd.DataFrame(rows)

    def get_day_level_performance(self) -> pd.DataFrame:
        data = self.df[self.df['Store Type'] == 'COCO'].copy()
        mtd_start = self.today.replace(day=1)
        mtd_data = data[(data['Date'].dt.date >= mtd_start) & (data['Date'].dt.date <= self.today)]
        
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

    def get_top_bottom_stores(self) -> Tuple[pd.DataFrame, pd.DataFrame]:
        data = self.df[self.df['Store Type'] == 'COCO'].copy()
        mtd_start = self.today.replace(day=1)
        mtd_data = data[(data['Date'].dt.date >= mtd_start) & (data['Date'].dt.date <= self.today)]
        
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

    def render_table_html(self, df: pd.DataFrame) -> str:
        html = '<table style="width:100%; border-collapse: collapse; font-size: 12px; margin-bottom: 20px;"><thead><tr style="background-color: #2c3e50; color: white;">'
        
        for col in df.columns:
            html += f'<th style="padding: 8px; border: 1px solid #ddd; text-align: left;">{col}</th>'
        html += '</tr></thead><tbody>'

        percentage_metrics = ['Dis%', 'Offline %', 'Online %', 'Dis', 'Offline', 'Online', 'Offline%', 'Online%']

        for _, row in df.iterrows():
            metric_label = str(row.get('Metrics', ''))
            is_pct_metric_row = metric_label in percentage_metrics
            
            html += '<tr>'
            for col in df.columns:
                val = row[col]
                bg_style = ""
                
                # Dynamic Heatmap formatting for bucket contribution columns
                if 'Contrib%' in col and isinstance(val, (int, float)):
                    max_val = df[col].max()
                    intensity = min(int((val / max_val) * 100), 100) if max_val > 0 else 0
                    bg_style = f'background-color: rgba(46, 204, 113, {intensity/100:.2f}); font-weight: bold;'

                if 'Growth%' in col:
                    cell_content = self.format_growth_html(val)
                elif 'Contrib%' in col or 'Dis%' in col or 'Offline%' in col or 'Online%' in col:
                    cell_content = f"{float(val):.2f}%" if isinstance(val, (int, float)) else str(val)
                elif is_pct_metric_row and isinstance(val, (int, float)):
                    cell_content = f"{val:.2f}%"
                elif isinstance(val, (int, float)):
                    if 'Sales' in col or 'Discount' in col or 'AOV' in col or 'Yesterday' in col or 'MTD' in col or 'Last' in col or 'FTD' in col:
                        cell_content = f"₹{val:,.0f}"
                    else:
                        cell_content = f"{val:,.0f}"
                else:
                    cell_content = str(val)
                
                html += f'<td style="padding: 8px; border: 1px solid #ddd; {bg_style}">{cell_content}</td>'
            html += '</tr>'
        html += '</tbody></table>'
        return html

    def generate_html_report(self) -> str:
        kpi = self.get_kpi_cards_data()
        
        html = f"""
        <html>
        <body style="font-family: Arial, sans-serif; background-color: #f4f6f9; padding: 20px; color: #333;">
            <div style="max-width: 1200px; margin: 0 auto; background: white; padding: 25px; border-radius: 8px; box-shadow: 0 0 10px rgba(0,0,0,0.1);">
                <h2 style="color: #2c3e50; border-bottom: 2px solid #3498db; padding-bottom: 10px;">📊 Daily Sales Report (DSR) Dashboard</h2>
                <p style="color: #7f8c8d; font-size: 14px;">Report Date: <strong>{self.today.strftime('%d %B %Y')}</strong></p>
                
                <div style="display: flex; gap: 15px; margin-bottom: 25px;">
                    <div style="flex: 1; background: #ebf5fb; padding: 15px; border-radius: 5px; text-align: center; border-left: 4px solid #3498db;">
                        <span style="font-size: 12px; color: #5d6d7e;">Net Sales (FTD)</span>
                        <h3 style="margin: 5px 0; color: #2e86c1;">₹{kpi['net_sales_lacs']:.2f} Lacs</h3>
                    </div>
                    <div style="flex: 1; background: #ebf5fb; padding: 15px; border-radius: 5px; text-align: center; border-left: 4px solid #3498db;">
                        <span style="font-size: 12px; color: #5d6d7e;">Orders (FTD)</span>
                        <h3 style="margin: 5px 0; color: #2e86c1;">{kpi['orders']:,}</h3>
                    </div>
                    <div style="flex: 1; background: #ebf5fb; padding: 15px; border-radius: 5px; text-align: center; border-left: 4px solid #3498db;">
                        <span style="font-size: 12px; color: #5d6d7e;">Dis %</span>
                        <h3 style="margin: 5px 0; color: #2e86c1;">{kpi['dis_pct']:.2f}%</h3>
                    </div>
                    <div style="flex: 1; background: #ebf5fb; padding: 15px; border-radius: 5px; text-align: center; border-left: 4px solid #3498db;">
                        <span style="font-size: 12px; color: #5d6d7e;">AOV</span>
                        <h3 style="margin: 5px 0; color: #2e86c1;">₹{kpi['aov']:,.0f}</h3>
                    </div>
                    <div style="flex: 1; background: #ebf5fb; padding: 15px; border-radius: 5px; text-align: center; border-left: 4px solid #3498db;">
                        <span style="font-size: 12px; color: #5d6d7e;">MoM% Growth</span>
                        <h3 style="margin: 5px 0;">{self.format_growth_html(kpi['mom_growth'])}</h3>
                    </div>
                    <div style="flex: 1; background: #ebf5fb; padding: 15px; border-radius: 5px; text-align: center; border-left: 4px solid #3498db;">
                        <span style="font-size: 12px; color: #5d6d7e;">YoY% Growth</span>
                        <h3 style="margin: 5px 0;">{self.format_growth_html(kpi['yoy_growth'])}</h3>
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

    def send_dashboard_email(self):
        try:
            logger.info("Generating dashboard email...")
            html_content = self.generate_html_report()
            
            msg = MIMEMultipart('alternative')
            msg['Subject'] = f"Daily Sales Report _ {self.today.strftime('%b %Y')}"
            msg['From'] = EMAIL_CONFIG['sender_email']
            msg['To'] = EMAIL_CONFIG['email_to']
            msg['Cc'] = EMAIL_CONFIG['email_cc']
            
            msg.attach(MIMEText(html_content, 'html'))
            
            logger.info(f"Sending email to {EMAIL_CONFIG['email_to']}...")
            with smtplib.SMTP(EMAIL_CONFIG['smtp_server'], EMAIL_CONFIG['smtp_port']) as server:
                server.starttls()
                server.login(EMAIL_CONFIG['sender_email'], EMAIL_CONFIG['sender_password'])
                server.send_message(msg)
            
            logger.info("✅ DSR Dashboard email sent successfully!")
            return True
        except Exception as e:
            logger.error(f"❌ Error sending email: {e}")
            return False

def main():
    logger.info("Starting DSR Dashboard execution...")
    dashboard = DSRDashboard()
    dashboard.send_dashboard_email()

if __name__ == "__main__":
    main()
