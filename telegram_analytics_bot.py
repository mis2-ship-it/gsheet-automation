import glob, os, gc, threading
import pandas as pd
import numpy as np
from datetime import datetime
from io import BytesIO
from flask import Flask

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.ext import ApplicationBuilder, CommandHandler, CallbackQueryHandler, MessageHandler, ContextTypes, filters
import openpyxl
from openpyxl.styles import Font, PatternFill
from pptx import Presentation
from pptx.util import Inches, Pt
from pptx.enum.chart import XL_CHART_TYPE, XL_LEGEND_POSITION
from pptx.chart.data import CategoryChartData
from pptx.dml.color import RGBColor

# Flask Web Server
flask_app = Flask(__name__)
@flask_app.route('/')
def health(): return "Analytics Bot Online", 200

def run_flask():
    flask_app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 10000)))

# Parquet Data Loader & Bucketing
DB_CACHE_FILE = "cached_dataset.parquet"

def optimize_and_cache_data():
    if os.path.exists(DB_CACHE_FILE):
        return pd.read_parquet(DB_CACHE_FILE)

    all_csvs = sorted(list(set(glob.glob("**/*.csv", recursive=True) + glob.glob("/home/RaviMallappa/**/*.csv", recursive=True))))
    if not all_csvs: raise FileNotFoundError("No historical CSV files found!")

    target_cols = ['Date', 'Brand Name', 'Brand', 'Branch', 'Store', 'Store Type', 'Region', 'Source', 'Session', 'Net Sales', 'Orders', 'Discount', 'Gross Sales']
    dfs = []
    for f in all_csvs:
        try:
            s_df = pd.read_csv(f, nrows=1)
            v_cols = [c for c in target_cols if c in s_df.columns]
            dfs.append(pd.read_csv(f, usecols=v_cols, low_memory=True))
        except: pass

    df = pd.concat(dfs, ignore_index=True)
    del dfs
    gc.collect()

    df['Date'] = pd.to_datetime(df['Date'], errors='coerce')
    df = df.dropna(subset=['Date'])
    df['YearMonth'] = df['Date'].dt.strftime('%Y-%m')
    df['MonthLabel'] = df['Date'].dt.strftime('%b %Y')
    df['Brand Name'] = df.get('Brand Name', df.get('Brand', 'Unknown'))
    df['Branch'] = df.get('Branch', df.get('Store', 'Unknown'))
    df['Source'] = df.get('Source', 'Unknown').astype(str)

    for c in ['Net Sales', 'Gross Sales', 'Discount']:
        df[c] = pd.to_numeric(df.get(c, 0), errors='coerce').fillna(0.0).astype('float32')
    df['Orders'] = pd.to_numeric(df.get('Orders', 0), errors='coerce').fillna(0).astype('int32')

    df['Calc_AOV'] = np.where(df['Orders'] > 0, df['Net Sales'] / df['Orders'], 0.0)
    df['Calc_Disc_Pct'] = np.where(df['Gross Sales'] > 0, (df['Discount'] / df['Gross Sales']) * 100, 0.0)

    df['AOV Bucket'] = pd.cut(df['Calc_AOV'], bins=[-np.inf, 200, 400, 600, 800, np.inf], labels=['< ₹200', '₹200 - ₹400', '₹400 - ₹600', '₹600 - ₹800', '> ₹800'])
    df['Discount Bucket'] = pd.cut(df['Calc_Disc_Pct'], bins=[-np.inf, 5, 15, 25, 35, np.inf], labels=['0 - 5%', '5 - 15%', '15 - 25%', '25 - 35%', '> 35%'])

    df.to_parquet(DB_CACHE_FILE, compression='snappy')
    return df

GLOBAL_DF = optimize_and_cache_data()
DIM_COL_MAP = {'Brand': 'Brand Name', 'Region': 'Region', 'Source': 'Source', 'Session': 'Session', 'Store': 'Branch'}

# Pivot & Exporters
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

# Telegram Callbacks
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
    threading.Thread(target=run_flask, daemon=True).start()
    app = ApplicationBuilder().token(os.environ["ANALYTICS_BOT_TOKEN"]).build()
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CallbackQueryHandler(handle_callback))
    print("📊 Analytics Bot Running...")
    app.run_polling(drop_pending_updates=True)
