import glob
import os
import gc
import pandas as pd
import numpy as np
from datetime import datetime
from io import BytesIO
import threading
from flask import Flask

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.ext import (
    ApplicationBuilder,
    CommandHandler,
    CallbackQueryHandler,
    MessageHandler,
    ContextTypes,
    filters,
)

import openpyxl
from openpyxl.styles import Font, PatternFill

from pptx import Presentation
from pptx.util import Inches, Pt
from pptx.enum.chart import XL_CHART_TYPE, XL_LEGEND_POSITION
from pptx.chart.data import CategoryChartData
from pptx.dml.color import RGBColor

# =========================================================
# 1. FLASK HEALTH CHECK SERVER
# =========================================================
flask_app = Flask(__name__)

@flask_app.route('/')
def health_check():
    return "Telegram Bot is running live!", 200

def run_flask():
    port = int(os.environ.get("PORT", 10000))
    flask_app.run(host="0.0.0.0", port=port)

# =========================================================
# 2. ULTRA-LOW MEMORY DATA ENGINE WITH BUCKETING
# =========================================================
DB_CACHE_FILE = "cached_dataset.parquet"

def optimize_and_cache_data():
    if os.path.exists(DB_CACHE_FILE):
        print("Found Parquet cache! Loading directly...")
        return pd.read_parquet(DB_CACHE_FILE)

    all_csvs = glob.glob("**/*.csv", recursive=True) + glob.glob("/home/RaviMallappa/**/*.csv", recursive=True)
    all_csvs = sorted(list(set([f for f in all_csvs if not os.path.basename(f).startswith('.')])))

    if not all_csvs:
        raise FileNotFoundError("No CSV files found in directory!")

    print(f"Processing {len(all_csvs)} CSV files with low-RAM chunking...")
    target_cols = ['Date', 'Brand Name', 'Brand', 'Branch', 'Store', 'Store Type', 'Region', 'Source', 'Session', 'Net Sales', 'Orders', 'Discount', 'Gross Sales']

    dfs = []
    for file_path in all_csvs:
        try:
            sample_df = pd.read_csv(file_path, nrows=1)
            valid_cols = [c for c in target_cols if c in sample_df.columns]
            del sample_df

            temp_df = pd.read_csv(file_path, usecols=valid_cols, low_memory=True)
            dfs.append(temp_df)
        except Exception as e:
            print(f"Skipping {file_path}: {e}")

    df = pd.concat(dfs, ignore_index=True)
    del dfs
    gc.collect()

    df['Date'] = pd.to_datetime(df['Date'], errors='coerce')
    df = df.dropna(subset=['Date'])

    df['YearMonth'] = df['Date'].dt.strftime('%Y-%m')
    df['MonthLabel'] = df['Date'].dt.strftime('%b %Y')

    df['Brand Name'] = df['Brand Name'] if 'Brand Name' in df.columns else df.get('Brand', 'Unknown')
    df['Branch'] = df['Branch'] if 'Branch' in df.columns else df.get('Store', 'Unknown')
    df['Source'] = df.get('Source', 'Unknown').astype(str).fillna('Unknown')

    for col in ['Net Sales', 'Gross Sales', 'Discount']:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0.0).astype('float32')
        else:
            df[col] = np.float32(0.0)

    df['Orders'] = pd.to_numeric(df.get('Orders', 0), errors='coerce').fillna(0).astype('int32')

    # Calculate Order-level AOV & Discount % for Bucketing
    df['Calc_AOV'] = np.where(df['Orders'] > 0, df['Net Sales'] / df['Orders'], 0.0)
    df['Calc_Disc_Pct'] = np.where(df['Gross Sales'] > 0, (df['Discount'] / df['Gross Sales']) * 100, 0.0)

    # AOV Buckets
    aov_bins = [-np.inf, 200, 400, 600, 800, np.inf]
    aov_labels = ['< ₹200', '₹200 - ₹400', '₹400 - ₹600', '₹600 - ₹800', '> ₹800']
    df['AOV Bucket'] = pd.cut(df['Calc_AOV'], bins=aov_bins, labels=aov_labels)

    # Discount Buckets
    disc_bins = [-np.inf, 5, 15, 25, 35, np.inf]
    disc_labels = ['0 - 5%', '5 - 15%', '15 - 25%', '25 - 35%', '> 35%']
    df['Discount Bucket'] = pd.cut(df['Calc_Disc_Pct'], bins=disc_bins, labels=disc_labels)

    cat_cols = ['Brand Name', 'Branch', 'Store Type', 'Region', 'Source', 'Session', 'YearMonth', 'MonthLabel', 'AOV Bucket', 'Discount Bucket']
    for c in cat_cols:
        if c in df.columns:
            df[c] = df[c].astype(str).fillna('Unknown').astype('category')

    df.to_parquet(DB_CACHE_FILE, compression='snappy')
    gc.collect()

    print(f"Dataset successfully compiled! RAM footprint: {df.memory_usage().sum() / 1024**2:.2f} MB")
    return df

GLOBAL_DF = optimize_and_cache_data()

DIM_COL_MAP = {
    'Brand': 'Brand Name',
    'Region': 'Region',
    'Source': 'Source',
    'Session': 'Session',
    'Store': 'Branch'
}

def get_unique_options(col_name):
    if col_name in GLOBAL_DF.columns:
        return sorted(GLOBAL_DF[col_name].dropna().unique().tolist())
    return []

# =========================================================
# 3. PIVOT & DATA CALCULATIONS
# =========================================================
def generate_pivoted_report(filters_dict, primary_dim, analysis_dims, timeframe):
    df = GLOBAL_DF.copy()

    if filters_dict.get('Store Type') and filters_dict['Store Type'] != 'ALL':
        df = df[df['Store Type'] == filters_dict['Store Type']]

    for key, col in DIM_COL_MAP.items():
        selected = filters_dict.get(key, set())
        if selected and 'ALL' not in selected and col in df.columns:
            df = df[df[col].isin(list(selected))]

    available_months = sorted(df['YearMonth'].dropna().unique())
    if not available_months:
        return None, None

    if timeframe == "Current Month":
        target_months = available_months[-1:]
    elif timeframe == "Last Month":
        target_months = available_months[-2:-1] if len(available_months) >= 2 else available_months[-1:]
    elif timeframe == "Last 2 Months":
        target_months = available_months[-2:]
    elif timeframe == "Quarterly":
        target_months = available_months[-3:]
    elif timeframe == "Half-Yearly":
        target_months = available_months[-6:]
    elif timeframe == "Yearly":
        target_months = available_months[-12:]
    else:
        target_months = available_months[-3:]

    df_eval = df[df['YearMonth'].isin(target_months)].copy()
    if df_eval.empty:
        return None, None

    group_cols = [DIM_COL_MAP[dim] for dim in analysis_dims if dim in DIM_COL_MAP and DIM_COL_MAP[dim] in df_eval.columns]
    if not group_cols:
        group_cols = [DIM_COL_MAP.get(primary_dim, 'Brand Name')]

    pivot_sales = pd.pivot_table(df_eval, index=group_cols, columns='YearMonth', values='Net Sales', aggfunc='sum', fill_value=0, observed=False)
    pivot_orders = pd.pivot_table(df_eval, index=group_cols, columns='YearMonth', values='Orders', aggfunc='sum', fill_value=0, observed=False)
    pivot_discount = pd.pivot_table(df_eval, index=group_cols, columns='YearMonth', values='Discount', aggfunc='sum', fill_value=0, observed=False)
    pivot_gross = pd.pivot_table(df_eval, index=group_cols, columns='YearMonth', values='Gross Sales', aggfunc='sum', fill_value=0, observed=False)

    summary_df = pivot_sales.copy()
    summary_df['Total Net Sales'] = pivot_sales.sum(axis=1)
    summary_df['Total Orders'] = pivot_orders.sum(axis=1)
    summary_df['AOV'] = np.where(summary_df['Total Orders'] > 0, summary_df['Total Net Sales'] / summary_df['Total Orders'], 0.0)

    tot_gross = pivot_gross.sum(axis=1)
    tot_disc = pivot_discount.sum(axis=1)
    summary_df['Discount %'] = np.where(tot_gross > 0, (tot_disc / tot_gross) * 100, 0.0)

    return summary_df, df_eval

# =========================================================
# 4. EXCEL EXPORTER (WITH BUCKET SUMMARIES)
# =========================================================
def build_excel_export(pivot_sales, df_raw, title):
    output = BytesIO()
    wb = openpyxl.Workbook()

    header_fill = PatternFill(start_color="1F4E79", end_color="1F4E79", fill_type="solid")
    font_header = Font(name="Calibri", size=11, bold=True, color="FFFFFF")

    month_cols = [c for c in pivot_sales.columns if c not in ['Total Net Sales', 'Total Orders', 'AOV', 'Discount %']]
    active_months = [m for m in month_cols if pivot_sales[m].sum() > 0]
    active_months = sorted(active_months)

    clean_pivot = pivot_sales.copy()
    if len(active_months) >= 2:
        first_m, last_m = active_months[0], active_months[-1]
        clean_pivot['MoM Growth %'] = np.where(
            clean_pivot[first_m] > 0,
            ((clean_pivot[last_m] - clean_pivot[first_m]) / clean_pivot[first_m]) * 100,
            0.0
        )
        final_cols = active_months + ['Total Net Sales', 'Total Orders', 'AOV', 'Discount %', 'MoM Growth %']
    else:
        final_cols = active_months + ['Total Net Sales', 'Total Orders', 'AOV', 'Discount %']

    clean_pivot = clean_pivot[final_cols]
    clean_pivot = clean_pivot[clean_pivot['Total Net Sales'] > 0]

    def write_sheet(ws, sheet_title, data_df):
        ws.append([sheet_title])
        ws.append([])

        headers = list(data_df.index.names) + list(data_df.columns)
        ws.append(headers)

        for c_idx in range(1, len(headers) + 1):
            cell = ws.cell(row=3, column=c_idx)
            cell.fill = header_fill
            cell.font = font_header

        for row in data_df.reset_index().values:
            formatted_row = []
            for item in row:
                if isinstance(item, float):
                    formatted_row.append(round(item, 2))
                else:
                    formatted_row.append(item)
            ws.append(formatted_row)

    ws1 = wb.active
    ws1.title = "Executive Summary"
    write_sheet(ws1, f"Monthly Breakdown - {title}", clean_pivot)

    # Standard Dimension Summaries
    summary_dims = {
        "Brand Summary": "Brand Name",
        "Store Summary": "Branch",
        "Source Summary": "Source",
        "Session Summary": "Session"
    }

    for sheet_name, col_name in summary_dims.items():
        if col_name in df_raw.columns:
            ws = wb.create_sheet(title=sheet_name)
            
            piv_s = pd.pivot_table(df_raw, index=col_name, columns='YearMonth', values='Net Sales', aggfunc='sum', fill_value=0, observed=True)
            piv_o = pd.pivot_table(df_raw, index=col_name, columns='YearMonth', values='Orders', aggfunc='sum', fill_value=0, observed=True)
            piv_d = pd.pivot_table(df_raw, index=col_name, columns='YearMonth', values='Discount', aggfunc='sum', fill_value=0, observed=True)
            piv_g = pd.pivot_table(df_raw, index=col_name, columns='YearMonth', values='Gross Sales', aggfunc='sum', fill_value=0, observed=True)

            dim_df = piv_s[active_months].copy()
            dim_df['Total Net Sales'] = piv_s.sum(axis=1)
            dim_df['Total Orders'] = piv_o.sum(axis=1)
            dim_df['AOV'] = np.where(dim_df['Total Orders'] > 0, dim_df['Total Net Sales'] / dim_df['Total Orders'], 0.0)
            
            tot_gross = piv_g.sum(axis=1)
            tot_disc = piv_d.sum(axis=1)
            dim_df['Discount %'] = np.where(tot_gross > 0, (tot_disc / tot_gross) * 100, 0.0)

            if len(active_months) >= 2:
                first_m, last_m = active_months[0], active_months[-1]
                dim_df['MoM Growth %'] = np.where(
                    dim_df[first_m] > 0,
                    ((dim_df[last_m] - dim_df[first_m]) / dim_df[first_m]) * 100,
                    0.0
                )

            dim_df = dim_df[dim_df['Total Net Sales'] > 0].sort_values(by='Total Net Sales', ascending=False)
            dim_df = dim_df.loc[~dim_df.index.isin(['Unknown', 'nan'])]

            write_sheet(ws, f"{sheet_name} Matrix", dim_df)

    # Bucket Summaries (AOV & Discount Buckets in Rows, Months in Columns)
    bucket_dims = {
        "AOV Bucket Summary": "AOV Bucket",
        "Discount Bucket Summary": "Discount Bucket"
    }

    for sheet_name, col_name in bucket_dims.items():
        if col_name in df_raw.columns:
            ws = wb.create_sheet(title=sheet_name)
            piv_b = pd.pivot_table(df_raw, index=col_name, columns='YearMonth', values='Net Sales', aggfunc='sum', fill_value=0, observed=True)
            b_df = piv_b[active_months].copy()
            b_df['Total Net Sales'] = piv_b.sum(axis=1)
            b_df = b_df[b_df['Total Net Sales'] > 0]
            write_sheet(ws, f"{sheet_name} (Sales in ₹)", b_df)

    wb.save(output)
    output.seek(0)
    return output

# =========================================================
# 5. PPTX EXPORTER WITH INSIGHTS & BUCKET SLIDES
# =========================================================
def build_pptx_export(pivot_sales, df_raw, title):
    prs = Presentation()
    prs.slide_width = Inches(13.33)
    prs.slide_height = Inches(7.5)
    blank_layout = prs.slide_layouts[6]

    # Helper function to add structured insight boxes
    def add_insight_box(slide, heading, bullet_lines):
        tb = slide.shapes.add_textbox(Inches(8.8), Inches(1.5), Inches(4.2), Inches(5.0))
        tf = tb.text_frame
        tf.word_wrap = True
        
        p = tf.paragraphs[0]
        p.text = f"💡 Executive Insights: {heading}"
        p.font.size = Pt(14)
        p.font.bold = True
        p.font.color.rgb = RGBColor(31, 78, 121)

        for line in bullet_lines:
            p_bullet = tf.add_paragraph()
            p_bullet.text = f"• {line}"
            p_bullet.font.size = Pt(11)
            p_bullet.space_before = Pt(8)

    # Slide 1: Title Slide
    slide1 = prs.slides.add_slide(blank_layout)
    tb = slide1.shapes.add_textbox(Inches(1), Inches(2.5), Inches(11.33), Inches(2))
    p = tb.text_frame.paragraphs[0]
    p.text = f"FROZEN BOTTLE ANALYTICS"
    p.font.size = Pt(40)
    p.font.bold = True
    p.font.color.rgb = RGBColor(31, 78, 121)

    p2 = tb.text_frame.add_paragraph()
    p2.text = f"Executive Dashboard: {title}\nGenerated on: {datetime.now().strftime('%b %d, %Y')}"
    p2.font.size = Pt(18)

    # Slide 2: Brand Performance (Bar Chart)
    slide2 = prs.slides.add_slide(blank_layout)
    top_performers = df_raw.groupby('Brand Name', observed=True)['Net Sales'].sum().sort_values(ascending=False).head(5)
    
    chart_data = CategoryChartData()
    chart_data.categories = [str(x) for x in top_performers.index]
    chart_data.add_series('Net Sales (₹ Lacs)', [round(float(v) / 100000.0, 2) for v in top_performers.values])

    chart_shape = slide2.shapes.add_chart(XL_CHART_TYPE.COLUMN_CLUSTERED, Inches(0.8), Inches(1.5), Inches(7.5), Inches(5), chart_data)
    chart = chart_shape.chart
    
    plots = chart.plots[0]
    plots.has_data_labels = True
    plots.data_labels.font.size = Pt(10)
    plots.data_labels.font.bold = True

    top_brand = top_performers.index[0] if not top_performers.empty else "N/A"
    top_brand_rev = round(top_performers.iloc[0] / 100000.0, 2) if not top_performers.empty else 0
    tot_rev = round(df_raw['Net Sales'].sum() / 100000.0, 2)

    add_insight_box(slide2, "Brand Performance", [
        f"Top Performing Brand: {top_brand} generating ₹{top_brand_rev:.2f} Lacs.",
        f"Overall portfolio revenue for selected period is ₹{tot_rev:.2f} Lacs.",
        f"Brand concentration: Top brand contributes {round((top_brand_rev/tot_rev)*100, 1) if tot_rev > 0 else 0}% of net sales."
    ])

    # Slide 3: Monthly Breakdown (Line Chart)
    slide3 = prs.slides.add_slide(blank_layout)
    m_trend = (
        df_raw.groupby(['YearMonth', 'MonthLabel'], observed=True)['Net Sales']
        .sum()
        .reset_index()
        .sort_values('YearMonth')
    )

    chart_data_line = CategoryChartData()
    chart_data_line.categories = [str(x) for x in m_trend['MonthLabel']]
    chart_data_line.add_series('Monthly Revenue (₹ Lacs)', [round(float(v) / 100000.0, 2) for v in m_trend['Net Sales']])

    chart_shape_line = slide3.shapes.add_chart(XL_CHART_TYPE.LINE, Inches(0.8), Inches(1.5), Inches(7.5), Inches(5), chart_data_line)
    chart_line = chart_shape_line.chart
    plots_line = chart_line.plots[0]
    plots_line.has_data_labels = True
    plots_line.data_labels.font.size = Pt(10)

    growth_str = "Stable trajectory across active months."
    if len(m_trend) >= 2:
        m1 = m_trend.iloc[0]['Net Sales']
        m2 = m_trend.iloc[-1]['Net Sales']
        pct_chg = round(((m2 - m1) / m1) * 100, 1) if m1 > 0 else 0
        growth_str = f"Net Sales changed by {pct_chg}% from {m_trend.iloc[0]['MonthLabel']} to {m_trend.iloc[-1]['MonthLabel']}."

    add_insight_box(slide3, "Monthly Dynamics", [
        f"Monthly revenue trend analyzed over {len(m_trend)} active months.",
        growth_str,
        f"Peak sales month recorded at ₹{round(m_trend['Net Sales'].max()/100000.0, 2)} Lacs."
    ])

    # Slide 4: Pie Chart (Source Share) with Legend and Category Names
    slide4 = prs.slides.add_slide(blank_layout)
    src_dist = df_raw.groupby('Source', observed=True)['Net Sales'].sum()
    src_dist = src_dist[src_dist > 0]

    chart_data_pie = CategoryChartData()
    chart_data_pie.categories = [str(x) for x in src_dist.index]
    chart_data_pie.add_series('Source Share', [round(float(v) / 100000.0, 2) for v in src_dist.values])

    chart_shape_pie = slide4.shapes.add_chart(XL_CHART_TYPE.PIE, Inches(0.8), Inches(1.5), Inches(7.5), Inches(5), chart_data_pie)
    chart_pie = chart_shape_pie.chart
    chart_pie.has_legend = True
    chart_pie.legend.position = XL_LEGEND_POSITION.RIGHT
    chart_pie.legend.font.size = Pt(10)

    plots_pie = chart_pie.plots[0]
    plots_pie.has_data_labels = True
    plots_pie.data_labels.show_category_name = True
    plots_pie.data_labels.show_value = True
    plots_pie.data_labels.font.size = Pt(9)

    top_src = src_dist.idxmax() if not src_dist.empty else "N/A"
    top_src_pct = round((src_dist.max() / src_dist.sum()) * 100, 1) if not src_dist.empty else 0

    add_insight_box(slide4, "Source Share", [
        f"Dominant Channel: {top_src} accounts for {top_src_pct}% of total sales.",
        f"Total active fulfillment channels: {len(src_dist)}.",
        "Clear channel segmentation visible between Online Aggregators and Offline In-Store sales."
    ])

    # Slide 5: AOV Bucket Analysis (Revenue in Lacs vs AOV Buckets)
    slide5 = prs.slides.add_slide(blank_layout)
    aov_dist = df_raw.groupby('AOV Bucket', observed=True)['Net Sales'].sum()
    
    chart_data_aov = CategoryChartData()
    chart_data_aov.categories = [str(x) for x in aov_dist.index]
    chart_data_aov.add_series('Revenue (₹ Lacs)', [round(float(v) / 100000.0, 2) for v in aov_dist.values])

    chart_shape_aov = slide5.shapes.add_chart(XL_CHART_TYPE.COLUMN_CLUSTERED, Inches(0.8), Inches(1.5), Inches(7.5), Inches(5), chart_data_aov)
    chart_aov = chart_shape_aov.chart
    chart_aov.plots[0].has_data_labels = True
    chart_aov.plots[0].data_labels.font.size = Pt(10)

    top_aov_bucket = aov_dist.idxmax() if not aov_dist.empty else "N/A"
    add_insight_box(slide5, "AOV Bucket Distribution", [
        f"Primary Revenue Driver: Orders in the '{top_aov_bucket}' AOV bucket.",
        "Higher AOV buckets reflect premium product combos and larger order sizes.",
        "Strategic focus recommended on upselling to push transactions into higher buckets."
    ])

    # Slide 6: Discount Bucket Analysis (Revenue in Lacs vs Discount Buckets)
    slide6 = prs.slides.add_slide(blank_layout)
    disc_dist = df_raw.groupby('Discount Bucket', observed=True)['Net Sales'].sum()

    chart_data_disc = CategoryChartData()
    chart_data_disc.categories = [str(x) for x in disc_dist.index]
    chart_data_disc.add_series('Revenue (₹ Lacs)', [round(float(v) / 100000.0, 2) for v in disc_dist.values])

    chart_shape_disc = slide6.shapes.add_chart(XL_CHART_TYPE.COLUMN_CLUSTERED, Inches(0.8), Inches(1.5), Inches(7.5), Inches(5), chart_data_disc)
    chart_disc = chart_shape_disc.chart
    chart_disc.plots[0].has_data_labels = True
    chart_disc.plots[0].data_labels.font.size = Pt(10)

    top_disc_bucket = disc_dist.idxmax() if not disc_dist.empty else "N/A"
    add_insight_box(slide6, "Discount Bucket Distribution", [
        f"Largest Revenue Bucket: '{top_disc_bucket}' discount range.",
        "Monitors margin health by evaluating revenue generated at steep discount levels.",
        "Targeted discount optimization can help improve overall gross margins."
    ])

    output = BytesIO()
    prs.save(output)
    output.seek(0)
    return output

# =========================================================
# 6. TELEGRAM BOT CONTROLLER
# =========================================================
ITEMS_PER_PAGE = 16

async def start_greeting(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data.clear()
    context.user_data['filters'] = {}
    context.user_data['analysis_models'] = set()

    keyboard = [
        [InlineKeyboardButton("🏷️ Brand Performance", callback_data="p_Brand")],
        [InlineKeyboardButton("🏬 Store Performance", callback_data="p_Store")],
        [InlineKeyboardButton("🛒 Source Performance", callback_data="p_Source")],
        [InlineKeyboardButton("🗺️ Region Performance", callback_data="p_Region")],
        [InlineKeyboardButton("⏰ Session Performance", callback_data="p_Session")],
    ]
    text = "👋 **Welcome to Executive Performance Analytics Engine**\nSelect your **Primary Dimension**:"

    if update.message:
        await update.message.reply_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="Markdown")
    elif update.callback_query:
        await update.callback_query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="Markdown")

async def text_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await start_greeting(update, context)

async def callback_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    data = query.data

    if 'filters' not in context.user_data:
        context.user_data['filters'] = {}
    if 'analysis_models' not in context.user_data:
        context.user_data['analysis_models'] = set()

    if data.startswith("p_"):
        prim = data.split("_")[1]
        context.user_data['primary_dim'] = prim

        keyboard = [
            [InlineKeyboardButton("🏬 COCO", callback_data="st_COCO")],
            [InlineKeyboardButton("🤝 FOFO", callback_data="st_FOFO")],
            [InlineKeyboardButton("🌐 Overall / ALL", callback_data="st_ALL")],
        ]
        await query.edit_message_text("🏢 **Step 2: Select Store Type Filter**", reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="Markdown")

    elif data.startswith("st_"):
        stype = data.split("_")[1]
        context.user_data['filters']['Store Type'] = stype

        keyboard = [
            [InlineKeyboardButton("📅 Current Month", callback_data="tf_Current Month")],
            [InlineKeyboardButton("📅 Last Month", callback_data="tf_Last Month")],
            [InlineKeyboardButton("📅 Last 2 Months", callback_data="tf_Last 2 Months")],
            [InlineKeyboardButton("📅 Quarterly", callback_data="tf_Quarterly")],
            [InlineKeyboardButton("📅 Half-Yearly", callback_data="tf_Half-Yearly")],
            [InlineKeyboardButton("📅 Yearly", callback_data="tf_Yearly")],
        ]
        await query.edit_message_text("⏳ **Step 3: Select Timeframe**", reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="Markdown")

    elif data.startswith("tf_"):
        tf = data.split("_")[1]
        context.user_data['timeframe'] = tf
        await render_analysis_models_menu(query, context)

    elif data.startswith("tm_"):
        model = data.replace("tm_", "")
        models = context.user_data.get('analysis_models', set())

        if model == "ALL":
            if len(models) == 5:
                models.clear()
            else:
                models = {"Brand", "Region", "Source", "Session", "Store"}
        else:
            if model in models:
                models.remove(model)
            else:
                models.add(model)

        context.user_data['analysis_models'] = models
        await render_analysis_models_menu(query, context)

    elif data == "confirm_models":
        models = context.user_data.get('analysis_models', set())
        if not models:
            models = {context.user_data.get('primary_dim', 'Brand')}
            context.user_data['analysis_models'] = models

        context.user_data['filter_queue'] = list(models)
        await prompt_next_entity_filter(query, context)

    elif data.startswith("te_"):
        parts = data.split("_")
        dim_code = parts[1]
        idx_str = parts[2]
        page = int(parts[3]) if len(parts) > 3 else 0

        dim_map = {'B': 'Brand', 'R': 'Region', 'S': 'Source', 'E': 'Session', 'T': 'Store'}
        dim = dim_map[dim_code]

        selected = context.user_data['filters'].get(dim, set())
        if not isinstance(selected, set):
            selected = set()

        if idx_str == "ALL":
            selected = {"ALL"}
        else:
            all_opts = get_unique_options(DIM_COL_MAP[dim])
            val = all_opts[int(idx_str)]

            selected.discard("ALL")
            if val in selected:
                selected.remove(val)
            else:
                selected.add(val)

        context.user_data['filters'][dim] = selected
        await render_entity_filter_menu(query, context, dim, page)

    elif data.startswith("pg_"):
        parts = data.split("_")
        dim_code = parts[1]
        target_page = int(parts[2])

        dim_map = {'B': 'Brand', 'R': 'Region', 'S': 'Source', 'E': 'Session', 'T': 'Store'}
        dim = dim_map[dim_code]

        await render_entity_filter_menu(query, context, dim, target_page)

    elif data == "noop":
        return

    elif data.startswith("ce_"):
        dim_code = data.replace("ce_", "")
        dim_map = {'B': 'Brand', 'R': 'Region', 'S': 'Source', 'E': 'Session', 'T': 'Store'}
        dim = dim_map[dim_code]

        if not context.user_data['filters'].get(dim):
            context.user_data['filters'][dim] = {"ALL"}

        await prompt_next_entity_filter(query, context)

    elif data == "dl_excel":
        await query.answer("Building Multi-Tab Excel Dashboard with Bucketing...")
        pivot_sales, df_raw = generate_pivoted_report(
            context.user_data.get('filters', {}),
            context.user_data.get('primary_dim', 'Brand'),
            list(context.user_data.get('analysis_models', ['Brand'])),
            context.user_data.get('timeframe', 'Current Month')
        )
        excel_file = build_excel_export(pivot_sales, df_raw, context.user_data.get('primary_dim', 'Brand'))
        await context.bot.send_document(
            chat_id=query.message.chat_id,
            document=excel_file,
            filename=f"FrozenBottle_Analytics_{context.user_data.get('primary_dim', 'Brand')}.xlsx",
            caption="📊 **Excel Executive Dashboard attached (includes AOV & Discount Bucketing).**"
        )

    elif data == "dl_pptx":
        await query.answer("Generating PowerPoint Presentation with Insights & Bucket Slides...")
        pivot_sales, df_raw = generate_pivoted_report(
            context.user_data.get('filters', {}),
            context.user_data.get('primary_dim', 'Brand'),
            list(context.user_data.get('analysis_models', ['Brand'])),
            context.user_data.get('timeframe', 'Current Month')
        )
        pptx_file = build_pptx_export(pivot_sales, df_raw, context.user_data.get('primary_dim', 'Brand'))
        await context.bot.send_document(
            chat_id=query.message.chat_id,
            document=pptx_file,
            filename=f"FrozenBottle_Analytics_{context.user_data.get('primary_dim', 'Brand')}.pptx",
            caption="📊 **PowerPoint Analytics Presentation with Native Charts & Insights attached.**"
        )

    elif data == "start_over":
        await start_greeting(update, context)

async def render_analysis_models_menu(query, context):
    selected_models = context.user_data.get('analysis_models', set())
    all_options = ["Brand", "Region", "Source", "Session", "Store"]

    keyboard = []
    all_mark = "✅" if len(selected_models) == 5 else "🟩"
    keyboard.append([InlineKeyboardButton(f"{all_mark} Select All Analysis Models", callback_data="tm_ALL")])

    for opt in all_options:
        mark = "✅" if opt in selected_models else "⬜"
        keyboard.append([InlineKeyboardButton(f"{mark} {opt} Model", callback_data=f"tm_{opt}")])

    keyboard.append([InlineKeyboardButton("➡️ Confirm Models & Proceed", callback_data="confirm_models")])

    text = "🔬 **Step 4: Select Analysis Models (Multi-Select)**\nPick the models you want to combine for breakdown:"
    await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="Markdown")

async def prompt_next_entity_filter(query, context):
    if 'filters' not in context.user_data:
        context.user_data['filters'] = {}
    if 'filter_queue' not in context.user_data:
        context.user_data['filter_queue'] = []

    queue = context.user_data.get('filter_queue', [])
    if queue:
        next_dim = queue.pop(0)
        context.user_data['filter_queue'] = queue
        await render_entity_filter_menu(query, context, next_dim, page=0)
    else:
        await generate_final_summary_view(query, context)

async def render_entity_filter_menu(query, context, dim, page=0):
    code_map = {'Brand': 'B', 'Region': 'R', 'Source': 'S', 'Session': 'E', 'Store': 'T'}
    d_code = code_map[dim]

    filters = context.user_data.setdefault('filters', {})
    selected = filters.get(dim, set())
    if not isinstance(selected, set):
        selected = set()

    all_options = get_unique_options(DIM_COL_MAP[dim])
    total_items = len(all_options)
    total_pages = max(1, (total_items + ITEMS_PER_PAGE - 1) // ITEMS_PER_PAGE)

    start_idx = page * ITEMS_PER_PAGE
    end_idx = start_idx + ITEMS_PER_PAGE
    page_options = all_options[start_idx:end_idx]

    keyboard = []
    is_all = "ALL" in selected or not selected
    all_mark = "✅" if is_all else "🟩"
    keyboard.append([InlineKeyboardButton(f"{all_mark} All {dim}s (Overall)", callback_data=f"te_{d_code}_ALL_{page}")])

    row = []
    for offset, opt in enumerate(page_options):
        actual_idx = start_idx + offset
        mark = "✅" if opt in selected else "⬜"
        row.append(InlineKeyboardButton(f"{mark} {opt}", callback_data=f"te_{d_code}_{actual_idx}_{page}"))
        if len(row) == 2:
            keyboard.append(row)
            row = []
    if row:
        keyboard.append(row)

    nav_row = []
    if page > 0:
        nav_row.append(InlineKeyboardButton("◀️ Prev", callback_data=f"pg_{d_code}_{page - 1}"))
    nav_row.append(InlineKeyboardButton(f"📄 {page + 1}/{total_pages}", callback_data="noop"))
    if page < total_pages - 1:
        nav_row.append(InlineKeyboardButton("Next ▶️", callback_data=f"pg_{d_code}_{page + 1}"))
    keyboard.append(nav_row)

    keyboard.append([InlineKeyboardButton(f"➡️ Confirm {dim} Selection", callback_data=f"ce_{d_code}")])

    text = f"🎯 **Filter Option: Select {dim} List** (Page {page + 1} of {total_pages})\nPick items, navigate pages, or choose **Overall**:"
    await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="Markdown")

async def generate_final_summary_view(query, context):
    await query.edit_message_text("🔄 *Calculating performance metrics...*", parse_mode="Markdown")

    pivot_sales, df_raw = generate_pivoted_report(
        context.user_data['filters'],
        context.user_data['primary_dim'],
        list(context.user_data['analysis_models']),
        context.user_data['timeframe']
    )

    if df_raw is None or df_raw.empty:
        await query.edit_message_text("⚠️ No data available for selected criteria. Tap /start to try again.")
        return

    # Source & Discount Analysis calculations
    src_grouped = df_raw.groupby('Source', observed=True).agg({'Net Sales': 'sum', 'Discount': 'sum', 'Gross Sales': 'sum'})
    tot_net = src_grouped['Net Sales'].sum()
    
    in_store_sales = src_grouped.loc[src_grouped.index.astype(str).str.lower().isin(['in store', 'instore', 'takeaway', 'dine in']), 'Net Sales'].sum()
    offline_pct = round((in_store_sales / tot_net) * 100, 1) if tot_net > 0 else 0.0
    online_pct = round(100.0 - offline_pct, 1)

    src_grouped['Disc_Pct'] = np.where(src_grouped['Gross Sales'] > 0, (src_grouped['Discount'] / src_grouped['Gross Sales']) * 100, 0.0)
    max_disc_src = src_grouped['Disc_Pct'].idxmax() if not src_grouped.empty else "N/A"
    max_disc_val = round(src_grouped['Disc_Pct'].max(), 1) if not src_grouped.empty else 0.0

    summary_dims = [
        ("BRAND PERFORMANCE REPORT", "Brand Name", "🏷️ Brand:"),
        ("SOURCE PERFORMANCE REPORT", "Source", "🛒 Source:"),
        ("BRANCH PERFORMANCE REPORT", "Branch", "🏦 Branch:")
    ]

    cards = []

    for title, col, label_prefix in summary_dims:
        if col in df_raw.columns:
            top_item = df_raw.groupby(col, observed=True).agg({
                'Net Sales': 'sum', 
                'Orders': 'sum', 
                'Discount': 'sum', 
                'Gross Sales': 'sum'
            }).sort_values(by='Net Sales', ascending=False).iloc[0]
    
            item_name = top_item.name
            net_rev = top_item['Net Sales'] / 100000.0
            tot_orders = int(top_item['Orders'])
            avg_aov = round(top_item['Net Sales'] / tot_orders, 0) if tot_orders > 0 else 0
            disc_pct = round((top_item['Discount'] / top_item['Gross Sales']) * 100, 1) if top_item['Gross Sales'] > 0 else 0.0
    
            card_str = f"📊 **{title}**\n"
            card_str += f"{label_prefix} **{item_name}**\n"
            card_str += f"🏬 Store Type: **{context.user_data['filters'].get('Store Type', 'ALL')}**\n"
            card_str += f"📅 Period: **{context.user_data['timeframe']}**\n\n"
            card_str += f"💰 Net Revenue: **₹{net_rev:.2f}L**\n"
            card_str += f"📄 Total Orders: **{tot_orders:,}**\n"
            card_str += f"🧺 Avg AOV: **₹{int(avg_aov)}**\n"
            card_str += f"📉 Avg Discount: **{disc_pct}%**\n"
            card_str += f"🏪 Offline Share: **{offline_pct}%** | 🌐 Online Share: **{online_pct}%**\n"
            card_str += f"⚠️ Highest Discount Channel: **{max_disc_src} ({max_disc_val}%)**\n\n"
            card_str += "📈 **Monthly Breakdown**\n"
    
            item_df = df_raw[df_raw[col] == item_name]
            m_breakdown = (
                item_df.groupby(['YearMonth', 'MonthLabel'], observed=True)
                .agg({'Net Sales': 'sum', 'Orders': 'sum', 'Discount': 'sum', 'Gross Sales': 'sum'})
                .reset_index()
            )
    
            m_breakdown = m_breakdown[m_breakdown['Orders'] > 0].sort_values('YearMonth')
    
            for _, m_row in m_breakdown.iterrows():
                m_name = m_row['MonthLabel']
                m_rev = m_row['Net Sales'] / 100000.0
                m_disc = round((m_row['Discount'] / m_row['Gross Sales']) * 100, 1) if m_row['Gross Sales'] > 0 else 0.0
                card_str += f"🔹 **{m_name}**: ₹{m_rev:.2f}L ({int(m_row['Orders']):,} orders | Dis: {m_disc}%)\n"
    
            cards.append(card_str)
    
    full_response = "\n\n---\n\n".join(cards[:2])
    full_response += "\n\n📥 **Download complete breakdown reports below:**"

    keyboard = [
        [InlineKeyboardButton("📊 Download Excel Dashboard", callback_data="dl_excel")],
        [InlineKeyboardButton("📈 Download PPTX Visual Presentation", callback_data="dl_pptx")],
        [InlineKeyboardButton("🔄 Start New Query", callback_data="start_over")]
    ]
    await query.edit_message_text(full_response, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="Markdown")

# =========================================================
# 7. MAIN LAUNCHER
# =========================================================
if __name__ == '__main__':
    threading.Thread(target=run_flask, daemon=True).start()

    token = os.environ.get("ANALYTICS_BOT_TOKEN")
    if not token:
        raise ValueError("ANALYTICS_BOT_TOKEN environment variable is missing!")

    app = ApplicationBuilder().token(token).build()

    app.add_handler(CommandHandler("start", start_greeting))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, text_handler))
    app.add_handler(CallbackQueryHandler(callback_handler))

    print("🤖 Telegram Bot Polling Started...")
    app.run_polling(drop_pending_updates=True)
