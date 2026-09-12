import glob, os, gc, threading
import pandas as pd
import numpy as np
from datetime import datetime
from io import BytesIO
from flask import Flask

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.ext import ApplicationBuilder, CommandHandler, CallbackQueryHandler, ContextTypes
import openpyxl
from pptx import Presentation
from pptx.util import Inches

# 1. FLASK BINDING FOR RENDER PORT SCANNER
flask_app = Flask(__name__)

@flask_app.route('/')
@flask_app.route('/health')
def health(): 
    return "Analytics Bot Online", 200

def run_flask():
    # Force port to 10000 if PORT env variable is not set by Render
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
DIM_COL_MAP = {'Brand': 'Brand Name', 'Region': 'Region', 'Source': 'Source', 'Session': 'Session', 'Store': 'Branch'}

# Report Builders
def generate_pivoted_report(filters_dict, primary_dim, timeframe):
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
    
    df_eval = df[df['YearMonth'].isin(months)].copy()
    piv = pd.pivot_table(df_eval, index=DIM_COL_MAP.get(primary_dim, 'Brand Name'), columns='YearMonth', values='Net Sales', aggfunc='sum', fill_value=0)
    return piv, df_eval

def build_excel(piv, df_raw):
    out = BytesIO()
    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = "Analytics Overview"
    ws.append(["Analytics Matrix"])
    ws.append([])
    ws.append(list(piv.index.names) + list(piv.columns))
    for r in piv.reset_index().values:
        ws.append([round(x, 2) if isinstance(x, float) else x for x in r])
    wb.save(out)
    out.seek(0)
    return out

def build_pptx(piv, df_raw):
    prs = Presentation()
    prs.slide_width, prs.slide_height = Inches(13.33), Inches(7.5)
    s = prs.slides.add_slide(prs.slide_layouts[6])
    tb = s.shapes.add_textbox(Inches(1), Inches(2), Inches(11), Inches(2))
    tb.text_frame.paragraphs[0].text = "Executive Performance Report"
    out = BytesIO()
    prs.save(out)
    out.seek(0)
    return out

# Telegram Handlers
async def start(u: Update, c: ContextTypes.DEFAULT_TYPE):
    kb = [[InlineKeyboardButton("🏷️ Brand", callback_data="p_Brand"), InlineKeyboardButton("🏬 Store", callback_data="p_Store")]]
    await (u.message or u.callback_query.message).reply_text("📊 **Historical Analytics Engine**\nSelect Primary Dimension:", reply_markup=InlineKeyboardMarkup(kb), parse_mode="Markdown")

async def handle_callback(u: Update, c: ContextTypes.DEFAULT_TYPE):
    q = u.callback_query
    await q.answer()
    data = q.data
    
    if data.startswith("p_"):
        c.user_data['prim'] = data.split("_")[1]
        c.user_data['filters'] = {}
        piv, df_raw = generate_pivoted_report(c.user_data['filters'], c.user_data['prim'], "Current Month")
        c.user_data['piv'], c.user_data['df_raw'] = piv, df_raw
        kb = [[InlineKeyboardButton("📄 Export Excel", callback_data="dl_xls"), InlineKeyboardButton("📊 Export PPT", callback_data="dl_ppt")]]
        await q.edit_message_text(f"Summary generated for **{c.user_data['prim']}**. Choose export:", reply_markup=InlineKeyboardMarkup(kb), parse_mode="Markdown")
        
    elif data == "dl_xls":
        doc = build_excel(c.user_data['piv'], c.user_data['df_raw'])
        await c.bot.send_document(q.message.chat_id, doc, filename="Analytics_Report.xlsx")
        
    elif data == "dl_ppt":
        doc = build_pptx(c.user_data['piv'], c.user_data['df_raw'])
        await c.bot.send_document(q.message.chat_id, doc, filename="Analytics_Presentation.pptx")

if __name__ == '__main__':
    # Start Web Server Thread
    threading.Thread(target=run_flask, daemon=True).start()

    # Safely get token (supports ANALYTICS_BOT_TOKEN or TELEGRAM_BOT_TOKEN)
    token = os.environ.get("ANALYTICS_BOT_TOKEN") or os.environ.get("TELEGRAM_BOT_TOKEN")
    
    if not token:
        raise KeyError(
            "Bot token missing! Please set 'ANALYTICS_BOT_TOKEN' or 'TELEGRAM_BOT_TOKEN' "
            "in your Render Environment Variables."
        )

    app = ApplicationBuilder().token(token).build()
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CallbackQueryHandler(handle_callback))
    print("📊 Analytics Bot Running...")
    app.run_polling(drop_pending_updates=True)
