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
from reportlab.lib.pagesizes import letter, landscape
from reportlab.lib import colors
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle, Image
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle

# =========================================================
# 1. FLASK HEALTH CHECK SERVER (For Render Free Tier)
# =========================================================
flask_app = Flask(__name__)

@flask_app.route('/')
def health_check():
    return "Bot is running live!"

def run_flask():
    port = int(os.environ.get("PORT", 10000))
    flask_app.run(host="0.0.0.0", port=port)

threading.Thread(target=run_flask, daemon=True).start()

# =========================================================
# 2. ULTRA-LOW MEMORY DATA ENGINE
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
    df['DayOfWeek'] = df['Date'].dt.day_name()
    df['WeekNumber'] = df['Date'].dt.isocalendar().week.astype(str)

    df['Brand Name'] = df['Brand Name'] if 'Brand Name' in df.columns else df.get('Brand', 'Unknown')
    df['Branch'] = df['Branch'] if 'Branch' in df.columns else df.get('Store', 'Unknown')

    for col in ['Net Sales', 'Gross Sales', 'Discount']:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0.0).astype('float32')
        else:
            df[col] = np.float32(0.0)

    df['Orders'] = pd.to_numeric(df.get('Orders', 0), errors='coerce').fillna(0).astype('int32')

    cat_cols = ['Brand Name', 'Branch', 'Store Type', 'Region', 'Source', 'Session', 'YearMonth', 'MonthLabel', 'DayOfWeek', 'WeekNumber']
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
# 3. PIVOT & MONTHLY BREAKDOWN CALCULATIONS
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

    # Create distinct monthly columns for Net Sales breakdown
    pivot_sales = pd.pivot_table(
        df_eval,
        index=group_cols,
        columns='YearMonth',
        values='Net Sales',
        aggfunc='sum',
        fill_value=0,
        observed=False
    )

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
# 4. EXCEL & PDF EXPORTERS (WITH DAY/WEEK ANALYSIS)
# =========================================================
def build_excel_export(pivot_sales, df_raw, title):
    output = BytesIO()
    wb = openpyxl.Workbook()

    header_fill = PatternFill(start_color="1F4E79", end_color="1F4E79", fill_type="solid")
    font_header = Font(name="Calibri", size=11, bold=True, color="FFFFFF")

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
    write_sheet(ws1, f"Monthly Breakdown - {title}", pivot_sales)

    # Day of Week Performance
    if 'DayOfWeek' in df_raw.columns:
        day_order = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
        day_grp = df_raw.groupby('DayOfWeek', observed=False).agg({'Net Sales': 'sum', 'Orders': 'sum'}).reindex(day_order).fillna(0)
        day_grp['AOV'] = np.where(day_grp['Orders'] > 0, day_grp['Net Sales'] / day_grp['Orders'], 0.0)
        ws_day = wb.create_sheet(title="Day of Week Performance")
        write_sheet(ws_day, "Day of Week Sales Performance", day_grp)

    # Weekly Performance
    if 'WeekNumber' in df_raw.columns:
        week_grp = df_raw.groupby('WeekNumber', observed=False).agg({'Net Sales': 'sum', 'Orders': 'sum'}).sort_index()
        week_grp['AOV'] = np.where(week_grp['Orders'] > 0, week_grp['Net Sales'] / week_grp['Orders'], 0.0)
        ws_week = wb.create_sheet(title="Weekly Performance")
        write_sheet(ws_week, "Weekly Sales Performance", week_grp)

    summary_dims = {
        "Brand Summary": "Brand Name",
        "Store Summary": "Branch",
        "Source Summary": "Source",
        "Session Summary": "Session"
    }

    for sheet_name, col_name in summary_dims.items():
        if col_name in df_raw.columns:
            ws = wb.create_sheet(title=sheet_name)
            grp = df_raw.groupby(col_name, observed=False).agg({'Net Sales': 'sum', 'Orders': 'sum', 'Discount': 'sum', 'Gross Sales': 'sum'}).reset_index()
            grp['Discount %'] = np.where(grp['Gross Sales'] > 0, (grp['Discount'] / grp['Gross Sales']) * 100, 0.0)
            grp['AOV'] = np.where(grp['Orders'] > 0, grp['Net Sales'] / grp['Orders'], 0.0)

            res_df = grp.set_index(col_name)[['Net Sales', 'Orders', 'AOV', 'Discount %']].sort_values(by='Net Sales', ascending=False)
            write_sheet(ws, f"{sheet_name} Matrix", res_df)

    wb.save(output)
    output.seek(0)
    return output

def build_pdf_export(pivot_sales, title):
    output = BytesIO()
    doc = SimpleDocTemplate(output, pagesize=landscape(letter), rightMargin=25, leftMargin=25, topMargin=25, bottomMargin=25)
    story = []
    styles = getSampleStyleSheet()

    title_style = ParagraphStyle('DocTitle', parent=styles['Heading1'], fontName='Helvetica-Bold', fontSize=18, leading=22, textColor=colors.HexColor('#1F4E79'))
    subtitle_style = ParagraphStyle('DocSubTitle', parent=styles['Normal'], fontName='Helvetica', fontSize=10, leading=12, textColor=colors.HexColor('#555555'))
    cell_style = ParagraphStyle('TableCell', parent=styles['Normal'], fontName='Helvetica', fontSize=8, leading=10)
    header_cell_style = ParagraphStyle('HeaderCell', parent=styles['Normal'], fontName='Helvetica-Bold', fontSize=8, leading=10, textColor=colors.white)

    logo_path = "frozen_bottle_logo.png"
    if os.path.exists(logo_path):
        logo_img = Image(logo_path, width=100, height=45)
    else:
        logo_img = Paragraph("<b>FROZEN BOTTLE</b>", title_style)

    header_text = [
        Paragraph(f"<b>Executive Analytics Report: {title}</b>", title_style),
        Paragraph(f"Generated: {datetime.now().strftime('%b %d, %Y | %I:%M %p')} | Confidential Management Report", subtitle_style)
    ]

    header_table = Table([[logo_img, header_text]], colWidths=[120, 640])
    header_table.setStyle(TableStyle([('VALIGN', (0, 0), (-1, -1), 'MIDDLE')]))
    story.append(header_table)
    story.append(Spacer(1, 15))

    headers = list(pivot_sales.index.names) + list(pivot_sales.columns)
    table_data = [[Paragraph(f"<b>{h}</b>", header_cell_style) for h in headers]]

    for idx, row in pivot_sales.reset_index().head(25).iterrows():
        formatted_row = []
        for item in row[:len(pivot_sales.index.names)]:
            formatted_row.append(Paragraph(str(item), cell_style))
        for col_name, val in zip(pivot_sales.columns, row[len(pivot_sales.index.names):]):
            if isinstance(val, float):
                if 'Sales' in str(col_name) or 'AOV' in str(col_name) or str(col_name).startswith('20'):
                    txt = f"₹{val/100000:,.2f} Lacs"
                elif '%' in str(col_name):
                    txt = f"{val:+.1f}%"
                else:
                    txt = f"{val:,.1f}"
            else:
                txt = str(val)
            formatted_row.append(Paragraph(txt, cell_style))
        table_data.append(formatted_row)

    total_width = 760
    dim_cols_count = len(pivot_sales.index.names)
    metric_cols_count = len(pivot_sales.columns)
    dim_width = 240 / dim_cols_count if dim_cols_count > 0 else 120
    metric_width = (total_width - (dim_width * dim_cols_count)) / metric_cols_count if metric_cols_count > 0 else 70
    col_widths = [dim_width] * dim_cols_count + [metric_width] * metric_cols_count

    data_table = Table(table_data, colWidths=col_widths, repeatRows=1)
    data_table.setStyle(TableStyle([
        ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor("#1F4E79")),
        ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
        ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
        ('GRID', (0, 0), (-1, -1), 0.5, colors.HexColor("#D3D3D3")),
        ('ROWBACKGROUNDS', (0, 1), (-1, -1), [colors.white, colors.HexColor("#F8F9FA")]),
        ('TOPPADDING', (0, 0), (-1, -1), 4),
        ('BOTTOMPADDING', (0, 0), (-1, -1), 4),
    ]))

    story.append(data_table)
    doc.build(story)
    output.seek(0)
    return output

# =========================================================
# 5. TELEGRAM BOT CONTROLLER WITH PAGINATION
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
        await query.answer("Building Multi-Tab Excel Dashboard...")
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
            caption="📊 **Excel Executive Dashboard attached.**"
        )

    elif data == "dl_pdf":
        await query.answer("Building Branded PDF Summary...")
        pivot_sales, df_raw = generate_pivoted_report(
            context.user_data.get('filters', {}),
            context.user_data.get('primary_dim', 'Brand'),
            list(context.user_data.get('analysis_models', ['Brand'])),
            context.user_data.get('timeframe', 'Current Month')
        )
        pdf_file = build_pdf_export(pivot_sales, context.user_data.get('primary_dim', 'Brand'))
        await context.bot.send_document(
            chat_id=query.message.chat_id,
            document=pdf_file,
            filename=f"FrozenBottle_Analytics_{context.user_data.get('primary_dim', 'Brand')}.pdf",
            caption="📄 **Branded PDF Executive Summary attached.**"
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
    await query.edit_message_text("🔄 *Calculating monthly analytics matrix...*", parse_mode="Markdown")

    pivot_sales, df_raw = generate_pivoted_report(
        context.user_data['filters'],
        context.user_data['primary_dim'],
        list(context.user_data['analysis_models']),
        context.user_data['timeframe']
    )

    if pivot_sales is None or pivot_sales.empty:
        await query.edit_message_text("⚠️ No data available for selected criteria. Tap /start to try again.")
        return

    summary_text = f"📊 **Executive Sales Summary (₹ in Lacs)**\n"
    summary_text += f"Timeframe: `{context.user_data['timeframe']}` | Models: `{', '.join(context.user_data['analysis_models'])}`\n\n"
    summary_text += "```\n"

    summary_text += f"{'Dimension Breakdown':<24} | Total Net Sales\n"
    summary_text += "-" * 42 + "\n"

    for idx, row in pivot_sales.head(8).iterrows():
        if isinstance(idx, tuple):
            clean_parts = [str(i) for i in idx if str(i) != 'Unknown']
            label = " > ".join(clean_parts[-2:]) if len(clean_parts) > 2 else " > ".join(clean_parts)
            label = label[:24]
        else:
            label = str(idx)[:24]

        # Convert Net Sales directly into Lacs
        total_lacs = row['Total Net Sales'] / 100000.0
        summary_text += f"{label:<24} | ₹{total_lacs:>6.2f} Lacs\n"

    summary_text += "```\n"
    summary_text += "📥 **Download complete breakdown reports below:**"

    keyboard = [
        [InlineKeyboardButton("📊 Download Excel Dashboard", callback_data="dl_excel")],
        [InlineKeyboardButton("📄 Download PDF Executive Summary", callback_data="dl_pdf")],
        [InlineKeyboardButton("🔄 Start New Query", callback_data="start_over")]
    ]
    await query.edit_message_text(summary_text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="Markdown")

# =========================================================
# 6. MAIN LAUNCHER
# =========================================================
if __name__ == "__main__":
    bot_token = os.getenv("TELEGRAM_BOT_TOKEN")

    app = ApplicationBuilder().token(bot_token).build()

    app.add_handler(CommandHandler("start", start_greeting))
    app.add_handler(MessageHandler(filters.TEXT & (~filters.COMMAND), start_greeting))
    app.add_handler(CallbackQueryHandler(callback_handler))

    print("🤖 Analytics Bot running with Low-RAM Engine...")
    app.run_polling(poll_interval=2.0, timeout=30)
