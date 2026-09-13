import glob, os, gc, threading, logging
import pandas as pd
import numpy as np
from datetime import datetime
from io import BytesIO
from flask import Flask

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.ext import ApplicationBuilder, CommandHandler, CallbackQueryHandler, MessageHandler, ContextTypes, filters
import openpyxl
from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
from openpyxl.utils import get_column_letter

from pptx import Presentation
from pptx.util import Inches, Pt
from pptx.dml.color import RGBColor

# Logging setup
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Flask Web Server
flask_app = Flask(__name__)
@flask_app.route('/')
@flask_app.route('/health')
def health(): 
    return "Analytics Bot Online", 200

def run_flask():
    port = int(os.environ.get("PORT", 10000))
    flask_app.run(host="0.0.0.0", port=port, debug=False, use_reloader=False)

# Parquet Data Loader & Memory Optimization
DB_CACHE_FILE = "cached_dataset.parquet"

def optimize_and_cache_data():
    if os.path.exists(DB_CACHE_FILE):
        return pd.read_parquet(DB_CACHE_FILE)

    all_csvs = sorted(list(set(glob.glob("**/*.csv", recursive=True) + glob.glob("/home/RaviMallappa/**/*.csv", recursive=True))))
    if not all_csvs: 
        raise FileNotFoundError("No historical CSV files found!")

    target_cols = ['Date', 'Brand Name', 'Brand', 'Branch', 'Store', 'Store Type', 'Region', 'Source', 'Session', 'Net Sales', 'Orders', 'Discount', 'Gross Sales']
    
    parquet_parts = []
    for f in all_csvs:
        try:
            s_df = pd.read_csv(f, nrows=1)
            v_cols = [c for c in target_cols if c in s_df.columns]
            df_part = pd.read_csv(f, usecols=v_cols, low_memory=True)

            df_part['Date'] = pd.to_datetime(df_part['Date'], errors='coerce')
            df_part = df_part.dropna(subset=['Date'])
            if df_part.empty:
                continue

            df_part['Brand Name'] = df_part.get('Brand Name', df_part.get('Brand', 'Unknown')).astype(str).astype('category')
            df_part['Branch'] = df_part.get('Branch', df_part.get('Store', 'Unknown')).astype(str).astype('category')
            df_part['Source'] = df_part.get('Source', 'Unknown').astype(str).astype('category')
            if 'Store Type' in df_part: df_part['Store Type'] = df_part['Store Type'].astype(str).astype('category')
            if 'Region' in df_part: df_part['Region'] = df_part['Region'].astype(str).astype('category')
            if 'Session' in df_part: df_part['Session'] = df_part['Session'].astype(str).astype('category')

            for num_col in ['Net Sales', 'Gross Sales', 'Discount']:
                df_part[num_col] = pd.to_numeric(df_part.get(num_col, 0), errors='coerce').fillna(0.0).astype('float32')
            df_part['Orders'] = pd.to_numeric(df_part.get('Orders', 0), errors='coerce').fillna(0).astype('int32')

            parquet_parts.append(df_part)
            del df_part
            gc.collect()
        except Exception:
            continue

    if not parquet_parts:
        raise ValueError("No valid data could be processed from CSVs.")

    df = pd.concat(parquet_parts, ignore_index=True)
    del parquet_parts
    gc.collect()

    df['YearMonth'] = df['Date'].dt.strftime('%Y-%m').astype('category')
    df['MonthLabel'] = df['Date'].dt.strftime('%b %Y').astype('category')

    df['Calc_AOV'] = np.where(df['Orders'] > 0, df['Net Sales'] / df['Orders'], 0.0).astype('float32')
    df['Calc_Disc_Pct'] = np.where(df['Gross Sales'] > 0, (df['Discount'] / df['Gross Sales']) * 100, 0.0).astype('float32')

    df['AOV Bucket'] = pd.cut(df['Calc_AOV'], bins=[-np.inf, 200, 400, 600, 800, np.inf], labels=['< ₹200', '₹200 - ₹400', '₹400 - ₹600', '₹600 - ₹800', '> ₹800'])
    df['Discount Bucket'] = pd.cut(df['Calc_Disc_Pct'], bins=[-np.inf, 5, 15, 25, 35, np.inf], labels=['0 - 5%', '5 - 15%', '15 - 25%', '25 - 35%', '> 35%'])

    df.to_parquet(DB_CACHE_FILE, compression='snappy')
    return df

GLOBAL_DF = optimize_and_cache_data()
DIM_COL_MAP = {
    'Brand': 'Brand Name',
    'Region': 'Region',
    'Source': 'Source',
    'Session': 'Session',
    'Store': 'Branch',
    'AOV Bucket': 'AOV Bucket',
    'Discount Bucket': 'Discount Bucket'
}

def generate_pivot(df_filtered, dim_col, months):
    df_eval = df_filtered[df_filtered['YearMonth'].isin(months)].copy()
    if df_eval.empty or dim_col not in df_eval.columns:
        return pd.DataFrame()
        
    piv = pd.pivot_table(df_eval, index=dim_col, columns='YearMonth', values='Net Sales', aggfunc='sum', fill_value=0)
    
    if len(piv.columns) >= 2:
        c1, c2 = piv.columns[-2], piv.columns[-1]
        piv['MoM Growth %'] = np.where(piv[c1] > 0, ((piv[c2] - piv[c1]) / piv[c1]) * 100, 0.0)
    piv['Total Sales'] = piv[[c for c in piv.columns if c != 'MoM Growth %']].sum(axis=1)
    
    return piv.sort_values(by='Total Sales', ascending=False)

def get_filtered_data(filters_dict, timeframe):
    df = GLOBAL_DF.copy()
    if filters_dict.get('Store Type') and filters_dict['Store Type'] != 'ALL':
        df = df[df['Store Type'] == filters_dict['Store Type']]
    for k, col in DIM_COL_MAP.items():
        sel = filters_dict.get(k, set())
        if sel and 'ALL' not in sel and col in df.columns:
            df = df[df[col].isin(list(sel))]
    
    avail = sorted(df['YearMonth'].dropna().unique())
    tf_map = {"Current Month": 1, "Last Month": 2, "Last 2 Months": 2, "Quarterly": 3, "Half-Yearly": 6, "Yearly": 12}
    months = avail[-tf_map.get(timeframe, 3):]
    
    return df, df[df['YearMonth'].isin(months)].copy(), months

def build_telegram_summary(piv, primary_dim, timeframe):
    if piv.empty:
        return "No data available for the selected parameters."
    lines = [f"📊 **Performance Summary ({primary_dim} | {timeframe})**\n"]
    lines.append("`" + f"{primary_dim[:12]:<12} | " + " | ".join([str(c) for c in piv.columns[:-2]]) + " | Total`")
    lines.append("`" + "-"*40 + "`")
    
    for idx, row in piv.head(8).iterrows():
        val_str = " | ".join([f"₹{int(v/1000)}k" for v in row[:-2]])
        tot_str = f"₹{int(row['Total Sales']/1000)}k"
        lines.append(f"`{str(idx)[:12]:<12} | {val_str} | {tot_str}`")
        
    tot_sales = piv['Total Sales'].sum()
    lines.append("\n" + f"💰 **Total Period Net Sales:** ₹{tot_sales:,.2f}")
    return "\n".join(lines)

def build_multi_sheet_excel(df_filtered, months):
    out = BytesIO()
    wb = openpyxl.Workbook()
    wb.remove(wb.active)  # Remove default sheet

    sections = [
        ("Brand Summary", "Brand Name"),
        ("Store Summary", "Branch"),
        ("Source Summary", "Source"),
        ("Session Summary", "Session")
    ]

    header_fill = PatternFill(start_color="1F4E78", end_color="1F4E78", fill_type="solid")
    header_font = Font(color="FFFFFF", bold=True)
    thin = Side(border_style="thin", color="CCCCCC")
    border = Border(left=thin, right=thin, top=thin, bottom=thin)

    # 1. Generate Summary Tabs
    for sheet_title, dim_col in sections:
        piv = generate_pivot(df_filtered, dim_col, months)
        ws = wb.create_sheet(title=sheet_title)
        
        ws.append([f"{sheet_title} Report"])
        ws.cell(1, 1).font = Font(size=14, bold=True, color="1F4E78")
        ws.append([])

        if piv.empty:
            ws.append(["No data available"])
            continue

        headers = [dim_col] + list(piv.columns)
        ws.append(headers)

        for col_idx, h in enumerate(headers, 1):
            cell = ws.cell(3, col_idx)
            cell.fill = header_fill
            cell.font = header_font
            cell.alignment = Alignment(horizontal="center")

        for r in piv.reset_index().values:
            row_vals = [round(val, 2) if isinstance(val, (float, int)) else val for val in r]
            ws.append(row_vals)

        # Summary Row
        sum_row = ["Total Summary"]
        for c in piv.columns:
            sum_row.append("-" if c == 'MoM Growth %' else round(piv[c].sum(), 2))
        ws.append(sum_row)

        # Border & Formatting
        for row in ws.iter_rows(min_row=4, max_row=ws.max_row, min_col=1, max_col=len(headers)):
            for cell in row:
                cell.border = border
                if isinstance(cell.value, (int, float)):
                    cell.number_format = '#,##0.00'

        for col in ws.columns:
            max_len = max(len(str(cell.value or '')) for cell in col)
            ws.column_dimensions[get_column_letter(col[0].column)].width = max(max_len + 3, 12)

    # 2. Raw Data Tab
    ws_raw = wb.create_sheet(title="Raw Data")
    ws_raw.append(list(df_filtered.columns))
    for r in df_filtered.head(5000).values:
        ws_raw.append([str(x) if isinstance(x, pd.Timestamp) else x for x in r])

    wb.save(out)
    out.seek(0)
    return out

def build_pptx(piv, primary_dim, timeframe):
    prs = Presentation()
    prs.slide_width, prs.slide_height = Inches(13.33), Inches(7.5)
    blank_layout = prs.slide_layouts[6]
    
    # Slide 1: Cover
    s1 = prs.slides.add_slide(blank_layout)
    bg1 = s1.shapes.add_shape(1, 0, 0, Inches(13.33), Inches(7.5))
    bg1.fill.solid()
    bg1.fill.fore_color.rgb = RGBColor(31, 78, 120)
    
    tb = s1.shapes.add_textbox(Inches(1), Inches(2.5), Inches(11.33), Inches(2))
    p = tb.text_frame.paragraphs[0]
    p.text = f"{primary_dim} Executive Overview"
    p.font.size = Pt(40)
    p.font.bold = True
    p.font.color.rgb = RGBColor(255, 255, 255)
    
    p2 = tb.text_frame.add_paragraph()
    p2.text = f"Timeframe: {timeframe} | Generated on {datetime.now().strftime('%Y-%m-%d')}"
    p2.font.size = Pt(20)
    p2.font.color.rgb = RGBColor(200, 220, 240)

    # Slide 2: Data Matrix
    s2 = prs.slides.add_slide(blank_layout)
    tb2 = s2.shapes.add_textbox(Inches(0.8), Inches(0.5), Inches(11), Inches(0.8))
    p3 = tb2.text_frame.paragraphs[0]
    p3.text = f"Performance Breakup by {primary_dim}"
    p3.font.size = Pt(28)
    p3.font.bold = True
    p3.font.color.rgb = RGBColor(31, 78, 120)

    rows = min(10, len(piv) + 1)
    cols = len(piv.columns) + 1
    table_shape = s2.shapes.add_table(rows, cols, Inches(0.8), Inches(1.5), Inches(11.73), Inches(4.8))
    table = table_shape.table

    headers = [piv.index.name or primary_dim] + list(piv.columns)
    for col_idx, h in enumerate(headers):
        cell = table.cell(0, col_idx)
        cell.text = str(h)
        cell.fill.solid()
        cell.fill.fore_color.rgb = RGBColor(31, 78, 120)
        p = cell.text_frame.paragraphs[0]
        p.font.color.rgb = RGBColor(255, 255, 255)
        p.font.bold = True
        p.font.size = Pt(13)

    for row_idx, (dim_val, row_data) in enumerate(piv.head(rows - 1).iterrows(), start=1):
        cell_dim = table.cell(row_idx, 0)
        cell_dim.text = str(dim_val)
        cell_dim.text_frame.paragraphs[0].font.size = Pt(11)
        for col_idx, val in enumerate(row_data, start=1):
            cell = table.cell(row_idx, col_idx)
            cell.text = f"{val:,.2f}" if isinstance(val, (float, int)) else str(val)
            cell.text_frame.paragraphs[0].font.size = Pt(11)

    out = BytesIO()
    prs.save(out)
    out.seek(0)
    return out

# UI Builders
def get_main_menu():
    kb = [
        [InlineKeyboardButton("🏷️ Brand", callback_data="p_Brand"), InlineKeyboardButton("🏬 Store", callback_data="p_Store")],
        [InlineKeyboardButton("🗺️ Region", callback_data="p_Region"), InlineKeyboardButton("🌐 Source", callback_data="p_Source")],
        [InlineKeyboardButton("🕒 Session", callback_data="p_Session"), InlineKeyboardButton("💰 AOV Bucket", callback_data="p_AOV Bucket")],
        [InlineKeyboardButton("🏷️ Discount Bucket", callback_data="p_Discount Bucket")]
    ]
    return InlineKeyboardMarkup(kb)

def get_store_type_menu():
    kb = [
        [InlineKeyboardButton("🌐 ALL Types", callback_data="st_ALL")],
        [InlineKeyboardButton("🏬 FOFO", callback_data="st_FOFO"), InlineKeyboardButton("🏢 COCO", callback_data="st_COCO")],
        [InlineKeyboardButton("🤝 Partner", callback_data="st_Partner")]
    ]
    return InlineKeyboardMarkup(kb)

def get_timeframe_menu():
    kb = [
        [InlineKeyboardButton("📅 Current Month", callback_data="tf_Current Month"), InlineKeyboardButton("📅 Last Month", callback_data="tf_Last Month")],
        [InlineKeyboardButton("📊 Last 2 Months", callback_data="tf_Last 2 Months"), InlineKeyboardButton("📈 Quarterly", callback_data="tf_Quarterly")],
        [InlineKeyboardButton("📉 Half-Yearly", callback_data="tf_Half-Yearly"), InlineKeyboardButton("📅 Yearly", callback_data="tf_Yearly")]
    ]
    return InlineKeyboardMarkup(kb)

def get_filter_menu(dim_name, selected_set):
    col = DIM_COL_MAP[dim_name]
    opts = sorted(GLOBAL_DF[col].dropna().unique().tolist())
    kb = []
    
    all_mark = "✅ " if "ALL" in selected_set or not selected_set else ""
    kb.append([InlineKeyboardButton(f"{all_mark}ALL Options", callback_data=f"fl_{dim_name}_ALL")])
    
    row = []
    for opt in opts[:10]:
        mark = "✅ " if opt in selected_set else ""
        row.append(InlineKeyboardButton(f"{mark}{opt}", callback_data=f"fl_{dim_name}_{opt}"))
        if len(row) == 2:
            kb.append(row)
            row = []
    if row:
        kb.append(row)
        
    kb.append([InlineKeyboardButton("➡️ Continue to Timeframe", callback_data="step_timeframe")])
    return InlineKeyboardMarkup(kb)

# Telegram Handlers
async def start(u: Update, c: ContextTypes.DEFAULT_TYPE):
    c.user_data.clear()
    c.user_data['filters'] = {}
    msg = u.message or u.callback_query.message
    await msg.reply_text("📊 **Historical Analytics Engine**\nSelect Primary Dimension:", reply_markup=get_main_menu(), parse_mode="Markdown")

async def handle_all_messages(u: Update, c: ContextTypes.DEFAULT_TYPE):
    await start(u, c)

async def handle_callback(u: Update, c: ContextTypes.DEFAULT_TYPE):
    q = u.callback_query
    await q.answer()
    data = q.data
    
    if data.startswith("p_"):
        c.user_data['prim'] = data.split("_")[1]
        await q.edit_message_text("🏬 **Select Store Type:**", reply_markup=get_store_type_menu(), parse_mode="Markdown")
        
    elif data.startswith("st_"):
        c.user_data['filters']['Store Type'] = data.split("_")[1]
        dims = [d for d in ['Brand', 'Region', 'Source', 'Session'] if d != c.user_data.get('prim')]
        first_dim = dims[0]
        await q.edit_message_text(f"🔍 **Filter by {first_dim}:**", reply_markup=get_filter_menu(first_dim, set()), parse_mode="Markdown")

    elif data.startswith("fl_"):
        _, dim, val = data.split("_", 2)
        if dim not in c.user_data['filters']:
            c.user_data['filters'][dim] = set()
            
        if val == "ALL":
            c.user_data['filters'][dim] = {"ALL"}
        else:
            c.user_data['filters'][dim].discard("ALL")
            if val in c.user_data['filters'][dim]:
                c.user_data['filters'][dim].remove(val)
            else:
                c.user_data['filters'][dim].add(val)
                
        await q.edit_message_reply_markup(reply_markup=get_filter_menu(dim, c.user_data['filters'][dim]))

    elif data == "step_timeframe":
        await q.edit_message_text("📅 **Select Timeframe:**", reply_markup=get_timeframe_menu(), parse_mode="Markdown")

    elif data.startswith("tf_"):
        tf = data.split("_")[1]
        c.user_data['timeframe'] = tf
        
        df_all, df_eval, months = get_filtered_data(c.user_data['filters'], tf)
        prim_col = DIM_COL_MAP[c.user_data['prim']]
        piv = generate_pivot(df_all, prim_col, months)
        
        c.user_data['piv'] = piv
        c.user_data['df_eval'] = df_eval
        c.user_data['months'] = months
        
        summary_text = build_telegram_summary(piv, c.user_data['prim'], tf)
        kb = [[InlineKeyboardButton("📄 Export Excel", callback_data="dl_xls"), InlineKeyboardButton("📊 Export PPT", callback_data="dl_ppt")]]
        
        await q.edit_message_text(f"{summary_text}\n\nChoose export format:", reply_markup=InlineKeyboardMarkup(kb), parse_mode="Markdown")
        
    elif data == "dl_xls":
        doc = build_multi_sheet_excel(c.user_data['df_eval'], c.user_data['months'])
        await c.bot.send_document(q.message.chat_id, doc, filename=f"Analytics_Full_Report.xlsx")
        
    elif data == "dl_ppt":
        doc = build_pptx(c.user_data['piv'], c.user_data['prim'], c.user_data['timeframe'])
        await c.bot.send_document(q.message.chat_id, doc, filename=f"Analytics_{c.user_data['prim']}.pptx")

async def error_handler(u: object, c: ContextTypes.DEFAULT_TYPE):
    if "Conflict" in str(c.error):
        return
    logger.error("Error encountered:", exc_info=c.error)

if __name__ == '__main__':
    threading.Thread(target=run_flask, daemon=True).start()

    token = os.environ.get("ANALYTICS_BOT_TOKEN") or os.environ.get("TELEGRAM_BOT_TOKEN")
    if not token:
        raise KeyError("Bot token missing in Environment Variables!")

    app = ApplicationBuilder().token(token).build()
    
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CallbackQueryHandler(handle_callback))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_all_messages))
    app.add_error_handler(error_handler)

    print("📊 Analytics Bot Running...")
    
    app.run_polling(
        drop_pending_updates=True,
        allowed_updates=Update.ALL_TYPES,
        poll_interval=1.0,
        timeout=30
    )
