import glob, os, gc, threading, logging, re, secrets, hashlib, smtplib, asyncio, io
import pandas as pd
import numpy as np
import requests
from datetime import datetime, timedelta
from io import BytesIO
from email.message import EmailMessage
from flask import Flask

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.ext import (
    ApplicationBuilder, CommandHandler, CallbackQueryHandler, 
    MessageHandler, ContextTypes, filters
)
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

# ---------------------------------------------------------
# AUTHORIZED USERS DIRECTORY
# ---------------------------------------------------------
AUTHORIZED_USERS = {
    "mis2@frozenbottle.in": {"role": "admin", "allowed_stores": "ALL"},
    "mis3@frozenbottle.in": {"role": "admin", "allowed_stores": "ALL"},
    "faraz@frozenbottle.in": {"role": "admin", "allowed_stores": "ALL"},
    "vivek@frozenbottle.in": {"role": "admin", "allowed_stores": "ALL"},

    "am.chennai@frozenbottle.in": {
        "role": "area_manager",
        "allowed_stores": ["Guduvanchery", "Mudichur", "OMR", "Pallikaranai", "Thoraipakkam", "Urapakkam CK", "Velachery"]
    },
    "am1.chennai@frozenbottle.in": {
        "role": "area_manager",
        "allowed_stores": ["Annanagar", "Besant Nagar", "Express Avenue Mall", "Mogappair", "Nanganallur CK", "Race Course Road", "Valsarvakkam", "Vellore"]
    },
    "am2.pune@frozenbottle.in": {
        "role": "area_manager",
        "allowed_stores": ["Baner Road - Pune", "Hinjewadi", "Hinjewadi Phase 3", "Koregaon Park - Pune", "Sinhagad", "Wagholi - CF CK"]
    },
    "am4.chennai@frozenbottle.in": {
        "role": "area_manager",
        "allowed_stores": ["Alwarpet", "Erode", "Iyyappanthangal - CK", "Kolathur", "Nungambakkam - CK", "Perambur - CK", "Zamin Pallavaram"]
    },
    "areamanager.kerala@frozenbottle.in": {
        "role": "area_manager",
        "allowed_stores": ["Kakkanad", "Ravipuram", "Thiruvalla"]
    },
    "areamanager.mumbai3@frozenbottle.in": {
        "role": "area_manager",
        "allowed_stores": ["Badlapur", "Byculla", "Kalyan", "Khar", "Lokhandwala", "Malad - CF - CK", "Prabhadevi", "Thakur Village"]
    },
    "areamanager.mumbai@frozenbottle.in": {
        "role": "area_manager",
        "allowed_stores": ["Dahisar", "Kamothe", "Manpada - CF CK", "Marol - CF CK", "Mira Road", "Mulund", "Powai- CF - CK", "SEAWOOD", "Sher- E-Punjab", "Virar"]
    },
    "areamanager1@frozenbottle.in": {
        "role": "area_manager",
        "allowed_stores": ["AECS Layout", "Gunjur", "ITPL", "Kempfort", "Manipal", "Miraya Rose", "Shivamogga", "Tata Sherwood", "Tumkur", "Whitefield", "Yemalur"]
    },
    "areamanager5@frozenbottle.in": {
        "role": "area_manager",
        "allowed_stores": ["Ananth Nagar", "BTM Layout", "Banashankari", "HSR Layout", "Harlur Road", "JP Nagar", "Kadubisanahalli - CF CK", "Koramangala", "Meenakshi Mall", "Sarjapur Road"]
    },
    "areamanager6@frozenbottle.in": {
        "role": "area_manager",
        "allowed_stores": ["Basaveshwarnagar", "Bel Road", "Channasandra", "Frazer Town", "Indiranagar - CK", "Kammanhalli", "Kanakapura", "Kolar- Highway Star", "Nagavara", "Yelahanka"]
    },
    "ops@lubov.in": {
        "role": "area_manager",
        "allowed_stores": ["Lubov Store"]
    },
    "bangaloreterritorymanager1@frozenbottle.in": {
        "role": "territory_manager",
        "allowed_stores": ["AECS Layout", "Ananth Nagar", "BTM Layout", "Banashankari", "Basaveshwarnagar", "Bel Road", "Channasandra", "Frazer Town", "Gunjur", "HSR Layout", "Harlur Road", "ITPL", "Indiranagar - CK", "JP Nagar", "Kadubisanahalli - CF CK", "Kammanhalli", "Kanakapura", "Kempfort", "Kolar- Highway Star", "Koramangala", "Lubov Store", "Manipal", "Meenakshi Mall", "Miraya Rose", "Nagavara", "Sarjapur Road", "Shivamogga", "Tata Sherwood", "Tumkur", "Whitefield", "Yelahanka", "Yemalur"]
    },
    "bangaloreterritorymanager@frozenbottle.in": {
        "role": "territory_manager",
        "allowed_stores": ["Kakkanad", "Ravipuram", "Thiruvalla"]
    },
    "manohar@frozenbottle.in": {
        "role": "territory_manager",
        "allowed_stores": ["Badlapur", "Baner Road - Pune", "Byculla", "Dahisar", "Hinjewadi", "Hinjewadi Phase 3", "Kalyan", "Kamothe", "Khar", "Koregaon Park - Pune", "Lokhandwala", "Malad - CF - CK", "Manpada - CF CK", "Marol - CF CK", "Mira Road", "Mulund", "Powai- CF - CK", "Prabhadevi", "SEAWOOD", "Sher- E-Punjab", "Sinhagad", "Thakur Village", "Virar", "Wagholi - CF CK"]
    },
    "tm.chennai@frozenbottle.in": {
        "role": "territory_manager",
        "allowed_stores": ["Alwarpet", "Annanagar", "Besant Nagar", "Erode", "Express Avenue Mall", "Guduvanchery", "Iyyappanthangal - CK", "Kolathur", "Mogappair", "Mudichur", "Nanganallur CK", "Nungambakkam - CK", "OMR", "Pallikaranai", "Perambur - CK", "Race Course Road", "Thoraipakkam", "Urapakkam CK", "Valsarvakkam", "Velachery", "Vellore", "Zamin Pallavaram"]
    }
}

USER_PASSWORDS = {}
SESSION_CACHE = {}

def hash_pass(pwd: str) -> str:
    return hashlib.sha256(pwd.encode()).hexdigest()

def generate_random_password(length=8):
    alphabet = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789"
    return "".join(secrets.choice(alphabet) for _ in range(length))

def send_access_email(user_email: str, passcode: str) -> bool:
    smtp_server = os.environ.get("SMTP_SERVER", "smtp.gmail.com")
    smtp_port = int(os.environ.get("SMTP_PORT", 587))
    smtp_email = os.environ.get("SMTP_EMAIL", "mis2@frozenbottle.in")
    smtp_password = os.environ.get("SMTP_PASSWORD", "nfyx nyqp dpyb hlig").replace(" ", "")

    if not all([smtp_email, smtp_password]):
        logger.warning("SMTP credentials missing.")
        return False

    msg = EmailMessage()
    msg['Subject'] = "🔐 Your Frozen Bottle Analytics Passcode"
    msg['From'] = f"Frozen Bottle Analytics <{smtp_email}>"
    msg['To'] = user_email

    html_content = f"""
    <html>
      <body style="font-family: Arial, sans-serif; color: #333; line-height: 1.6;">
        <div style="max-width: 500px; margin: auto; padding: 20px; border: 1px solid #e0e0e0; border-radius: 8px;">
          <h2 style="color: #1F4E78; text-align: center;">Frozen Bottle Analytics</h2>
          <p>Hello,</p>
          <p>Your passcode for the <strong>Analytics Telegram Bot</strong> is:</p>
          <div style="background-color: #f4f6f8; padding: 15px; text-align: center; border-radius: 6px; margin: 20px 0;">
            <span style="font-size: 26px; font-weight: bold; letter-spacing: 4px; color: #1F4E78;">{passcode}</span>
          </div>
          <p style="font-size: 13px; color: #666;">Enter this passcode in your Telegram chat to unlock access.</p>
        </div>
      </body>
    </html>
    """
    msg.set_content(f"Your passcode for Frozen Bottle Analytics Bot is: {passcode}")
    msg.add_alternative(html_content, subtype='html')

    try:
        if smtp_port == 465:
            with smtplib.SMTP_SSL(smtp_server, smtp_port, timeout=15) as server:
                server.login(smtp_email, smtp_password)
                server.send_message(msg)
        else:
            with smtplib.SMTP(smtp_server, smtp_port, timeout=15) as server:
                server.ehlo()
                server.starttls()
                server.ehlo()
                server.login(smtp_email, smtp_password)
                server.send_message(msg)
        logger.info(f"Email sent successfully to {user_email}")
        return True
    except Exception as e:
        logger.error(f"Failed to send email to {user_email}: {e}")
        return False

# ---------------------------------------------------------
# Dynamic Remote GitHub & Local Dataset Loader
# ---------------------------------------------------------
GITHUB_CSV_URL = "https://raw.githubusercontent.com/mis2-ship-it/gsheet-automation/main/historical_data/historical_sales_summary.csv"

GLOBAL_DF = None

def fetch_data():
    """Fetches sales data dynamically from GitHub and local files."""
    dfs = []

    try:
        resp = requests.get(GITHUB_CSV_URL, timeout=12)
        if resp.status_code == 200:
            gh_df = pd.read_csv(io.StringIO(resp.text))
            dfs.append(gh_df)
            logger.info("Successfully loaded GitHub historical summary CSV.")
    except Exception as e:
        logger.warning(f"Could not fetch GitHub CSV: {e}")

    local_csvs = sorted(list(set(glob.glob("**/*.csv", recursive=True) + glob.glob("/home/runner/work/**/*.csv", recursive=True))))
    target_cols = ['Date', 'date', 'Brand Name', 'Brand', 'brand', 'Branch', 'Store', 'branch', 'Store Type', 'Region', 'Source', 'source', 'Session', 'Net Sales', 'net_sales', 'net_amount', 'Orders', 'Discount', 'Gross Sales']

    for f in local_csvs:
        try:
            s_df = pd.read_csv(f, nrows=1)
            v_cols = [c for c in target_cols if c in s_df.columns]
            if v_cols:
                df_part = pd.read_csv(f, usecols=v_cols, low_memory=False)
                dfs.append(df_part)
        except Exception:
            continue

    if not dfs:
        logger.error("No sales datasets available.")
        return pd.DataFrame()

    df = pd.concat(dfs, ignore_index=True)

    col_map = {
        'date': 'Date',
        'branch': 'Branch',
        'store': 'Branch',
        'Store': 'Branch',
        'brand': 'Brand Name',
        'Brand': 'Brand Name',
        'source': 'Source',
        'net_sales': 'Net Sales',
        'net_amount': 'Net Sales'
    }
    df.rename(columns=col_map, inplace=True)

    df['Date'] = pd.to_datetime(df['Date'], errors='coerce')
    df = df.dropna(subset=['Date'])

    df['Brand Name'] = df.get('Brand Name', 'Frozen Bottle').astype(str).str.strip()
    df['Branch'] = df.get('Branch', 'Unknown').astype(str).str.strip()
    df['Source'] = df.get('Source', 'Unknown').astype(str).str.strip()
    df['Store Type'] = df.get('Store Type', 'COCO').astype(str).str.strip()
    df['Region'] = df.get('Region', 'General').astype(str).str.strip()
    df['Session'] = df.get('Session', 'All Day').astype(str).str.strip()

    for num_col in ['Net Sales', 'Gross Sales', 'Discount']:
        if num_col in df:
            df[num_col] = pd.to_numeric(df[num_col], errors='coerce').fillna(0.0).astype('float32')
            if df[num_col].max() > 10000:
                df[num_col] = df[num_col] / 100000.0

    if 'Orders' not in df:
        df['Orders'] = 1
    df['Orders'] = pd.to_numeric(df['Orders'], errors='coerce').fillna(1).astype('int32')

    df['YearMonth'] = df['Date'].dt.strftime('%Y-%m')
    df['MonthLabel'] = df['Date'].dt.strftime('%b %Y')

    raw_sales_inr = df['Net Sales'] * 100000.0
    df['Calc_AOV'] = np.where(df['Orders'] > 0, raw_sales_inr / df['Orders'], 0.0).astype('float32')
    df['Calc_Disc_Pct'] = np.where(df.get('Gross Sales', 0) > 0, (df.get('Discount', 0) / df.get('Gross Sales', 1)) * 100, 0.0).astype('float32')

    df['AOV Bucket'] = pd.cut(df['Calc_AOV'], bins=[-np.inf, 200, 400, 600, 800, np.inf], labels=['< ₹200', '₹200 - ₹400', '₹400 - ₹600', '₹600 - ₹800', '> ₹800']).astype(str)
    df['Discount Bucket'] = pd.cut(df['Calc_Disc_Pct'], bins=[-np.inf, 5, 15, 25, 35, np.inf], labels=['0 - 5%', '5 - 15%', '15 - 25%', '25 - 35%', '> 35%']).astype(str)

    return df

def refresh_global_data():
    global GLOBAL_DF
    GLOBAL_DF = fetch_data()
    return GLOBAL_DF

GLOBAL_DF = refresh_global_data()

DIM_COL_MAP = {
    'Brand': 'Brand Name',
    'Region': 'Region',
    'Source': 'Source',
    'Session': 'Session',
    'Store': 'Branch',
    'AOV Bucket': 'AOV Bucket',
    'Discount Bucket': 'Discount Bucket'
}

def get_filtered_data(filters_dict, timeframe, user_config):
    df = refresh_global_data()

    if df.empty:
        return pd.DataFrame(), pd.DataFrame(), []

    allowed_stores = user_config.get('allowed_stores', 'ALL')
    if allowed_stores != 'ALL':
        allowed_clean = [str(s).strip().lower() for s in allowed_stores]
        df = df[df['Branch'].str.strip().str.lower().isin(allowed_clean)]

    # Store Type Filter
    st_val = filters_dict.get('Store Type')
    if st_val and st_val != 'ALL':
        df = df[df['Store Type'].str.strip().str.lower() == st_val.strip().lower()]

    # Region Filter
    reg_set = filters_dict.get('Region', set())
    if reg_set and 'ALL' not in reg_set:
        reg_clean = [str(r).strip().lower() for r in reg_set]
        df = df[df['Region'].str.strip().str.lower().isin(reg_clean)]

    # Store Filter
    store_set = filters_dict.get('Store', set())
    if store_set and 'ALL' not in store_set:
        st_clean = [str(s).strip().lower() for s in store_set]
        df = df[df['Branch'].str.strip().str.lower().isin(st_clean)]

    # Brand Filter
    brand_set = filters_dict.get('Brand', set())
    if brand_set and 'ALL' not in brand_set:
        b_clean = [str(b).strip().lower() for b in brand_set]
        df = df[df['Brand Name'].str.strip().str.lower().isin(b_clean)]

    avail = sorted(df['YearMonth'].dropna().unique().tolist())
    if not avail:
        return df, pd.DataFrame(), []

    if timeframe == "Current Month":
        months = [avail[-1]]
    elif timeframe == "Last Month":
        months = [avail[-2]] if len(avail) >= 2 else [avail[-1]]
    else:
        tf_map = {
            "Last 2 Months": 2,
            "Quarterly": 3,
            "Half-Yearly": 6,
            "Yearly": 12
        }
        count = tf_map.get(timeframe, 3)
        months = avail[-count:]
    
    return df, df[df['YearMonth'].isin(months)].copy(), months

# ---------------------------------------------------------
# SUMMARY DASHBOARD BUILDER
# ---------------------------------------------------------
def build_comprehensive_summary(df_eval, timeframe):
    if df_eval.empty:
        return "⚠️ **No data available for the selected parameters.**"

    net_sales = df_eval['Net Sales'].sum()
    gross_sales = df_eval.get('Gross Sales', df_eval['Net Sales']).sum()
    discount = df_eval.get('Discount', 0).sum()
    disc_pct = (discount / gross_sales * 100) if gross_sales > 0 else 0.0
    orders = df_eval['Orders'].sum()
    aov = (net_sales * 100000.0 / orders) if orders > 0 else 0.0

    lines = []
    lines.append(f"📊 **SALES PERFORMANCE SUMMARY ({timeframe})**")
    lines.append("="*34)
    lines.append(f"💰 **Total Net Sales:** ₹{net_sales:,.2f} Lacs")
    lines.append(f"🏷️ **Gross Sales:** ₹{gross_sales:,.2f} Lacs")
    lines.append(f"🔴 **Discount:** ₹{discount:,.2f} Lacs ({disc_pct:.1f}%)")
    lines.append(f"🛒 **Total Orders:** {orders:,}")
    lines.append(f"💵 **AOV:** ₹{aov:,.2f}")
    lines.append("")

    if 'Branch' in df_eval.columns:
        st_grp = df_eval.groupby('Branch')['Net Sales'].sum().reset_index().sort_values(by='Net Sales', ascending=False)
        lines.append("🏬 **Top Stores (Net Sales in Lacs):**")
        for _, r in st_grp.head(5).iterrows():
            lines.append(f" • `{r['Branch'][:15]:<15} : ₹{r['Net Sales']:.2f}L`")
        lines.append("")

    if 'Region' in df_eval.columns:
        reg_grp = df_eval.groupby('Region')['Net Sales'].sum().reset_index().sort_values(by='Net Sales', ascending=False)
        lines.append("🗺 **Region Breakdown:**")
        for _, r in reg_grp.iterrows():
            lines.append(f" • `{r['Region'][:15]:<15} : ₹{r['Net Sales']:.2f}L`")
        lines.append("")

    if 'Brand Name' in df_eval.columns:
        b_grp = df_eval.groupby('Brand Name')['Net Sales'].sum().reset_index().sort_values(by='Net Sales', ascending=False)
        lines.append("🏷️ **Brand Breakdown:**")
        for _, r in b_grp.iterrows():
            lines.append(f" • `{r['Brand Name'][:15]:<15} : ₹{r['Net Sales']:.2f}L`")
        lines.append("")

    if 'Source' in df_eval.columns:
        src_grp = df_eval.groupby('Source')['Net Sales'].sum().reset_index().sort_values(by='Net Sales', ascending=False)
        lines.append("🌐 **Source Summary:**")
        for _, r in src_grp.iterrows():
            lines.append(f" • `{r['Source'][:15]:<15} : ₹{r['Net Sales']:.2f}L`")
        lines.append("")

    if 'Session' in df_eval.columns:
        ses_grp = df_eval.groupby('Session')['Net Sales'].sum().reset_index().sort_values(by='Net Sales', ascending=False)
        lines.append("🕒 **Session Summary:**")
        for _, r in ses_grp.iterrows():
            lines.append(f" • `{r['Session'][:15]:<15} : ₹{r['Net Sales']:.2f}L`")

    return "\n".join(lines)

def build_multi_sheet_excel(df_filtered, months):
    out = BytesIO()
    wb = openpyxl.Workbook()
    wb.remove(wb.active)

    sections = [
        ("Store Summary", "Branch"),
        ("Region Summary", "Region"),
        ("Brand Summary", "Brand Name"),
        ("Source Summary", "Source"),
        ("Session Summary", "Session")
    ]

    header_fill = PatternFill(start_color="1F4E78", end_color="1F4E78", fill_type="solid")
    header_font = Font(color="FFFFFF", bold=True)

    for sheet_title, dim_col in sections:
        if df_filtered.empty or dim_col not in df_filtered.columns:
            continue

        df_eval = df_filtered[df_filtered['YearMonth'].isin(months)].copy()
        piv = pd.pivot_table(df_eval, index=dim_col, columns='YearMonth', values='Net Sales', aggfunc='sum', fill_value=0)
        piv['Total Sales (Lacs)'] = piv.sum(axis=1)
        piv = piv.sort_values(by='Total Sales (Lacs)', ascending=False)

        ws = wb.create_sheet(title=sheet_title)
        ws.append([f"{sheet_title} Report (in ₹ Lacs)"])
        ws.cell(1, 1).font = Font(size=14, bold=True, color="1F4E78")
        ws.append([])

        reset_piv = piv.reset_index()
        headers = list(reset_piv.columns)
        ws.append(headers)

        for col_idx, h in enumerate(headers, 1):
            cell = ws.cell(3, col_idx)
            cell.fill = header_fill
            cell.font = header_font
            cell.alignment = Alignment(horizontal="center")

        for r in reset_piv.values:
            ws.append([round(float(v), 2) if isinstance(v, (float, np.floating, int, np.integer)) else v for v in r])

        for col in ws.columns:
            max_len = max(len(str(cell.value or '')) for cell in col)
            ws.column_dimensions[get_column_letter(col[0].column)].width = max(max_len + 3, 14)

    wb.save(out)
    out.seek(0)
    return out

# ---------------------------------------------------------
# STEP-BY-STEP MENUS WITH CLEAR BACK BUTTONS
# ---------------------------------------------------------
def get_main_menu():
    kb = [
        [InlineKeyboardButton("🏷️ Brand", callback_data="p_Brand"), InlineKeyboardButton("🏬 Store", callback_data="p_Store")],
        [InlineKeyboardButton("🗺️ Region", callback_data="p_Region"), InlineKeyboardButton("🌐 Source", callback_data="p_Source")],
        [InlineKeyboardButton("🕒 Session", callback_data="p_Session"), InlineKeyboardButton("💰 AOV Bucket", callback_data="p_AOV Bucket")]
    ]
    return InlineKeyboardMarkup(kb)

def get_store_type_menu():
    kb = [
        [InlineKeyboardButton("🌐 ALL Types", callback_data="st_ALL")],
        [InlineKeyboardButton("🏬 FOFO", callback_data="st_FOFO"), InlineKeyboardButton("🏢 COCO", callback_data="st_COCO")],
        [InlineKeyboardButton("🤝 Partner", callback_data="st_Partner")],
        [InlineKeyboardButton("🔙 Back to Main Menu", callback_data="back_to_main")]
    ]
    return InlineKeyboardMarkup(kb)

def get_region_menu(selected_regions):
    regions = sorted(GLOBAL_DF['Region'].dropna().unique().tolist()) if GLOBAL_DF is not None else []
    kb = []
    
    all_mark = "✅ " if "ALL" in selected_regions or not selected_regions else ""
    kb.append([InlineKeyboardButton(f"{all_mark}ALL Regions", callback_data="reg_ALL")])
    
    row = []
    for r in regions:
        mark = "✅ " if r in selected_regions else ""
        row.append(InlineKeyboardButton(f"{mark}{r}", callback_data=f"reg_{r}"))
        if len(row) == 2:
            kb.append(row)
            row = []
    if row:
        kb.append(row)

    kb.append([
        InlineKeyboardButton("🔙 Back to Store Type", callback_data="back_to_store_type"),
        InlineKeyboardButton("➡️ Select Store List", callback_data="step_store_list")
    ])
    return InlineKeyboardMarkup(kb)

def get_store_list_menu(selected_regions, selected_stores):
    df_temp = GLOBAL_DF.copy() if GLOBAL_DF is not None else pd.DataFrame()
    if selected_regions and "ALL" not in selected_regions and 'Region' in df_temp.columns:
        df_temp = df_temp[df_temp['Region'].isin(selected_regions)]

    stores = sorted(df_temp['Branch'].dropna().unique().tolist()) if not df_temp.empty else []
    kb = []

    all_mark = "✅ " if "ALL" in selected_stores or not selected_stores else ""
    kb.append([InlineKeyboardButton(f"{all_mark}ALL Stores in Region", callback_data="str_ALL")])

    row = []
    for s in stores[:14]:
        mark = "✅ " if s in selected_stores else ""
        row.append(InlineKeyboardButton(f"{mark}{s[:14]}", callback_data=f"str_{s}"))
        if len(row) == 2:
            kb.append(row)
            row = []
    if row:
        kb.append(row)

    kb.append([
        InlineKeyboardButton("🔙 Back to Region", callback_data="back_to_region"),
        InlineKeyboardButton("➡️ Select Brand Filters", callback_data="step_brand_filter")
    ])
    return InlineKeyboardMarkup(kb)

def get_brand_menu(selected_brands):
    brands = sorted(GLOBAL_DF['Brand Name'].dropna().unique().tolist()) if GLOBAL_DF is not None else []
    kb = []

    all_mark = "✅ " if "ALL" in selected_brands or not selected_brands else ""
    kb.append([InlineKeyboardButton(f"{all_mark}ALL Brands", callback_data="brd_ALL")])

    row = []
    for b in brands:
        mark = "✅ " if b in selected_brands else ""
        row.append(InlineKeyboardButton(f"{mark}{b}", callback_data=f"brd_{b}"))
        if len(row) == 2:
            kb.append(row)
            row = []
    if row:
        kb.append(row)

    kb.append([
        InlineKeyboardButton("🔙 Back to Store List", callback_data="step_store_list"),
        InlineKeyboardButton("➡️ Select Timeframe", callback_data="step_timeframe")
    ])
    return InlineKeyboardMarkup(kb)

def get_timeframe_menu():
    kb = [
        [InlineKeyboardButton("📅 Current Month", callback_data="tf_Current Month"), InlineKeyboardButton("📅 Last Month", callback_data="tf_Last Month")],
        [InlineKeyboardButton("📊 Last 2 Months", callback_data="tf_Last 2 Months"), InlineKeyboardButton("📈 Quarterly", callback_data="tf_Quarterly")],
        [InlineKeyboardButton("📉 Half-Yearly", callback_data="tf_Half-Yearly"), InlineKeyboardButton("📅 Yearly", callback_data="tf_Yearly")],
        [InlineKeyboardButton("🔙 Back to Brand Filters", callback_data="step_brand_filter")]
    ]
    return InlineKeyboardMarkup(kb)

# ---------------------------------------------------------
# TELEGRAM COMMANDS & CALLBACK ROUTING
# ---------------------------------------------------------
async def show_dimension_menu(message, user_config):
    store_info = "All Stores" if user_config['allowed_stores'] == "ALL" else ", ".join(user_config['allowed_stores'])
    await message.reply_text(
        f"👋 **Welcome ({user_config['email']})**\n"
        f"🔑 **Role:** `{user_config['role']}`\n"
        f"🏬 **Scope:** `{store_info}`\n"
        f"💰 **Figures:** `Values in ₹ Lacs`\n\n"
        f"Select Primary Dimension:", 
        reply_markup=get_main_menu(), 
        parse_mode="Markdown"
    )

async def start(u: Update, c: ContextTypes.DEFAULT_TYPE):
    user_id = u.effective_user.id
    
    if user_id not in SESSION_CACHE or not SESSION_CACHE[user_id].get("authenticated"):
        c.user_data['login_stage'] = 'AWAITING_EMAIL'
        await u.message.reply_text(
            "🔐 **Data Access Control System**\n\n"
            "Please enter your registered **corporate email address** to begin:",
            parse_mode="Markdown"
        )
        return

    c.user_data['filters'] = {'Region': set(), 'Store': set(), 'Brand': set()}
    await show_dimension_menu(u.message, SESSION_CACHE[user_id])

async def handle_callback(u: Update, c: ContextTypes.DEFAULT_TYPE):
    q = u.callback_query
    await q.answer()
    user_id = q.from_user.id
    
    if user_id not in SESSION_CACHE or not SESSION_CACHE[user_id].get("authenticated"):
        await q.message.reply_text("🔒 **Access Denied.** Please log in using /start.")
        return

    data = q.data
    user_config = SESSION_CACHE[user_id]

    if 'filters' not in c.user_data:
        c.user_data['filters'] = {'Region': set(), 'Store': set(), 'Brand': set()}

    # STEP 1: DIMENSION SELECTION
    if data.startswith("p_"):
        c.user_data['prim'] = data.split("_")[1]
        await q.edit_message_text("🏬 **Select Store Type:**", reply_markup=get_store_type_menu(), parse_mode="Markdown")

    elif data == "back_to_main":
        await q.edit_message_text("Select Primary Dimension:", reply_markup=get_main_menu(), parse_mode="Markdown")

    # STEP 2: STORE TYPE
    elif data.startswith("st_"):
        c.user_data['filters']['Store Type'] = data.split("_")[1]
        curr_regs = c.user_data['filters'].get('Region', set())
        await q.edit_message_text("🗺️ **Select Region(s):**", reply_markup=get_region_menu(curr_regs), parse_mode="Markdown")

    elif data == "back_to_store_type":
        await q.edit_message_text("🏬 **Select Store Type:**", reply_markup=get_store_type_menu(), parse_mode="Markdown")

    # STEP 3: REGION SELECTION
    elif data.startswith("reg_"):
        val = data.split("_", 1)[1]
        if val == "ALL":
            c.user_data['filters']['Region'] = {"ALL"}
        else:
            c.user_data['filters']['Region'].discard("ALL")
            if val in c.user_data['filters']['Region']:
                c.user_data['filters']['Region'].remove(val)
            else:
                c.user_data['filters']['Region'].add(val)
        await q.edit_message_reply_markup(reply_markup=get_region_menu(c.user_data['filters']['Region']))

    elif data == "step_store_list":
        curr_stores = c.user_data['filters'].get('Store', set())
        curr_regs = c.user_data['filters'].get('Region', set())
        await q.edit_message_text("🏬 **Select Stores related to Selected Region(s):**", reply_markup=get_store_list_menu(curr_regs, curr_stores), parse_mode="Markdown")

    elif data == "back_to_region":
        curr_regs = c.user_data['filters'].get('Region', set())
        await q.edit_message_text("🗺️ **Select Region(s):**", reply_markup=get_region_menu(curr_regs), parse_mode="Markdown")

    # STEP 4: STORE SELECTION
    elif data.startswith("str_"):
        val = data.split("_", 1)[1]
        if val == "ALL":
            c.user_data['filters']['Store'] = {"ALL"}
        else:
            c.user_data['filters']['Store'].discard("ALL")
            if val in c.user_data['filters']['Store']:
                c.user_data['filters']['Store'].remove(val)
            else:
                c.user_data['filters']['Store'].add(val)
        curr_regs = c.user_data['filters'].get('Region', set())
        await q.edit_message_reply_markup(reply_markup=get_store_list_menu(curr_regs, c.user_data['filters']['Store']))

    # STEP 5: BRAND SELECTION
    elif data == "step_brand_filter":
        curr_brands = c.user_data['filters'].get('Brand', set())
        await q.edit_message_text("🏷️ **Select Brand Filter:**", reply_markup=get_brand_menu(curr_brands), parse_mode="Markdown")

    elif data.startswith("brd_"):
        val = data.split("_", 1)[1]
        if val == "ALL":
            c.user_data['filters']['Brand'] = {"ALL"}
        else:
            c.user_data['filters']['Brand'].discard("ALL")
            if val in c.user_data['filters']['Brand']:
                c.user_data['filters']['Brand'].remove(val)
            else:
                c.user_data['filters']['Brand'].add(val)
        await q.edit_message_reply_markup(reply_markup=get_brand_menu(c.user_data['filters']['Brand']))

    # STEP 6: TIMEFRAME
    elif data == "step_timeframe":
        await q.edit_message_text("📅 **Select Timeframe:**", reply_markup=get_timeframe_menu(), parse_mode="Markdown")

    # STEP 7: EXECUTE & DISPLAY
    elif data.startswith("tf_"):
        tf = data.split("_")[1]
        c.user_data['timeframe'] = tf

        df_all, df_eval, months = get_filtered_data(c.user_data['filters'], tf, user_config)
        c.user_data['df_eval'] = df_eval
        c.user_data['months'] = months

        summary_text = build_comprehensive_summary(df_eval, tf)
        kb = [
            [InlineKeyboardButton("📄 Download Excel", callback_data="dl_xls")],
            [InlineKeyboardButton("🔙 Change Timeframe", callback_data="step_timeframe"), InlineKeyboardButton("🏠 Main Menu", callback_data="back_to_main")]
        ]

        await q.edit_message_text(f"{summary_text}\n\nChoose export format:", reply_markup=InlineKeyboardMarkup(kb), parse_mode="Markdown")

    elif data == "dl_xls":
        doc = build_multi_sheet_excel(c.user_data['df_eval'], c.user_data['months'])
        await c.bot.send_document(q.message.chat_id, doc, filename=f"FrozenBottle_Sales_Summary.xlsx")

async def handle_text_messages(u: Update, c: ContextTypes.DEFAULT_TYPE):
    user_id = u.effective_user.id
    text = u.message.text.strip()
    stage = c.user_data.get('login_stage')

    if user_id in SESSION_CACHE and SESSION_CACHE[user_id].get("authenticated"):
        await start(u, c)
        return

    if stage == 'AWAITING_EMAIL' or "@" in text:
        email = text.lower()
        if email not in AUTHORIZED_USERS:
            await u.message.reply_text("⛔ **Access Denied:** Email address is not authorized.")
            return

        c.user_data['pending_email'] = email

        generated_pwd = generate_random_password(8)
        USER_PASSWORDS[email] = {
            "hash": hash_pass(generated_pwd),
            "plain": generated_pwd
        }
        
        status_msg = await u.message.reply_text("📧 Sending access code to your email...")
        email_sent = await asyncio.to_thread(send_access_email, email, generated_pwd)

        if email_sent:
            await status_msg.edit_text(
                f"✅ **Passcode Sent!**\n\n"
                f"A secure code has been sent to `{email}`.\n"
                f"Please check your inbox (or spam) and reply with the passcode to log in:",
                parse_mode="Markdown"
            )
        else:
            await status_msg.edit_text(
                f"⚠️ **Passcode Generated (Email Delivery Failed)**\n\n"
                f"Could not reach SMTP server. Use this code to log in: `{generated_pwd}`",
                parse_mode="Markdown"
            )

        c.user_data['login_stage'] = 'AWAITING_PASSWORD'
        return

    if stage == 'AWAITING_PASSWORD':
        email = c.user_data.get('pending_email')
        if not email or email not in USER_PASSWORDS:
            c.user_data['login_stage'] = 'AWAITING_EMAIL'
            await u.message.reply_text("Session expired. Please send your email ID again using /start.")
            return

        entered_hash = hash_pass(text)
        stored_hash = USER_PASSWORDS[email]['hash']

        if entered_hash == stored_hash:
            SESSION_CACHE[user_id] = AUTHORIZED_USERS[email].copy()
            SESSION_CACHE[user_id]['email'] = email
            SESSION_CACHE[user_id]['authenticated'] = True
            c.user_data['login_stage'] = None

            await u.message.reply_text("🔓 **Authentication Successful! Access Granted.**", parse_mode="Markdown")
            await show_dimension_menu(u.message, SESSION_CACHE[user_id])
        else:
            await u.message.reply_text("❌ **Incorrect Passcode.** Please check your email and try again.", parse_mode="Markdown")
        return

    await u.message.reply_text(
        "👋 **Welcome to Analytics Control System**\n\n"
        "Please enter your **registered corporate email address** to continue:",
        parse_mode="Markdown"
    )

if __name__ == '__main__':
    threading.Thread(target=run_flask, daemon=True).start()

    token = os.environ.get("ANALYTICS_BOT_TOKEN") or os.environ.get("TELEGRAM_BOT_TOKEN")
    if not token:
        raise KeyError("Bot token missing in Environment Variables!")

    app = ApplicationBuilder().token(token).build()
    
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CallbackQueryHandler(handle_callback))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text_messages))

    print("📊 Password-Protected Analytics Bot Online...")
    app.run_polling(drop_pending_updates=True, poll_interval=1.0)
