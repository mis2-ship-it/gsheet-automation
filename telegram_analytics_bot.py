import os
import glob
import subprocess
import pandas as pd
from datetime import datetime, timedelta
from telegram import Update
from telegram.ext import ApplicationBuilder, CommandHandler, ContextTypes

# =========================================================
# DATA LOADER ENGINE (With Auto-Sync from GitHub)
# =========================================================
def load_historical_data(months=3):
    """Syncs with Git repository and loads CSV files from monthly_data/ and historical_data/."""
    # Pull latest data from GitHub Actions if inside a git repository
    try:
        subprocess.run(["git", "pull"], check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        print("✅ Git pull executed successfully.")
    except Exception as e:
        print(f"⚠️ Git pull warning: {e}")

    base_dir = os.path.dirname(os.path.abspath(__file__))
    
    # Match both monthly_data/ and historical_data/ subdirectories
    csv_files = glob.glob(os.path.join(base_dir, "monthly_data", "**", "*.csv"), recursive=True) + \
                glob.glob(os.path.join(base_dir, "historical_data", "**", "*.csv"), recursive=True)
    
    df_list = []
    for f in csv_files:
        try:
            df = pd.read_csv(f, low_memory=False)
            df_list.append(df)
        except Exception as e:
            print(f"Error reading {f}: {e}")
            
    if not df_list:
        return pd.DataFrame()
        
    full_df = pd.concat(df_list, ignore_index=True)
    full_df.columns = full_df.columns.str.strip()
    
    # Handle Date parsing and dynamic timeframe calculation
    if "Date" in full_df.columns:
        full_df["Date"] = pd.to_datetime(full_df["Date"], errors='coerce')
        full_df = full_df.dropna(subset=["Date"])
        
        # Calculate dynamic cutoff from latest available date in dataset
        max_date = full_df["Date"].max()
        cutoff_date = max_date - timedelta(days=months * 31)
        full_df = full_df[full_df["Date"] >= cutoff_date]
    
    return full_df.copy()

# =========================================================
# HELPER: PARSE TIMEFRAME ARGUMENTS
# =========================================================
def extract_months(args):
    """Extracts duration integer from arguments (e.g., '3m' -> 3)."""
    for arg in reversed(args):
        if 'm' in arg.lower():
            try:
                return int(arg.lower().replace('m', ''))
            except ValueError:
                pass
    return 3

# =========================================================
# COMMAND HANDLER: /store <Branch Name> <Type: COCO|FOFO|ALL> <Period>
# =========================================================
async def store_analysis(update: Update, context: ContextTypes.DEFAULT_TYPE):
    args = context.args
    if not args:
        await update.message.reply_text(
            "⚠️ *Usage Format:*\n"
            "`/store <Branch Name> <Type: COCO|FOFO|ALL> <Period: 1m|3m|6m>`\n\n"
            "📌 *Examples:*\n"
            "• `/store Koramangala COCO 3m`\n"
            "• `/store Indiranagar FOFO 6m`\n"
            "• `/store Whitefield ALL 1m`", 
            parse_mode="Markdown"
        )
        return

    months = extract_months(args)
    store_type = "ALL"
    
    # Check if second or third argument is store type
    for arg in args[1:]:
        if arg.upper() in ["COCO", "FOFO", "ALL"]:
            store_type = arg.upper()
            break

    # Extract raw query (excludes duration and type tokens)
    query_parts = [a for a in args if not ('m' in a.lower() and a[:-1].isdigit()) and a.upper() not in ["COCO", "FOFO", "ALL"]]
    raw_branch = " ".join(query_parts) if query_parts else args[0]

    await update.message.reply_text(
        f"⏳ Fetching *{raw_branch}* ({store_type}) performance for the last {months} months...", 
        parse_mode="Markdown"
    )
    
    df = load_historical_data(months=months)
    if df.empty:
        await update.message.reply_text("❌ No historical CSV data found in repository.")
        return

    # Flexible store matching on 'Branch' column
    branch_col = "Branch" if "Branch" in df.columns else [c for c in df.columns if "branch" in c.lower() or "store" in c.lower()][0]
    filtered_df = df[df[branch_col].astype(str).str.contains(raw_branch, case=False, na=False)].copy()

    # Filter by Store Type if specified
    type_col = "Type" if "Type" in df.columns else [c for c in df.columns if "type" in c.lower() or "coco" in c.lower()]
    if type_col and isinstance(type_col, list):
        type_col = type_col[0]

    if store_type in ["COCO", "FOFO"] and type_col and type_col in filtered_df.columns:
        filtered_df = filtered_df[filtered_df[type_col].astype(str).str.upper() == store_type]

    if filtered_df.empty:
        await update.message.reply_text(f"❌ No records found matching Branch: *{raw_branch}* | Type: *{store_type}*.", parse_mode="Markdown")
        return

    # Calculate Aggregated Metrics
    net_sales = filtered_df["Net Sales"].sum() / 100000.0 if "Net Sales" in filtered_df.columns else 0
    gross_sales = filtered_df["Gross Sales"].sum() if "Gross Sales" in filtered_df.columns else 0
    discounts = filtered_df["Discount"].sum() if "Discount" in filtered_df.columns else 0
    orders = filtered_df["Orders"].sum() if "Orders" in filtered_df.columns else 0
    
    dis_pct = (discounts / gross_sales * 100) if gross_sales > 0 else 0
    aov = (filtered_df["Net Sales"].sum() / orders) if orders > 0 else 0
    matched_branch = filtered_df[branch_col].iloc[0]

    # Monthly Breakdown Trend
    filtered_df["Month_Year"] = filtered_df["Date"].dt.strftime("%b %Y")
    monthly_trend = filtered_df.groupby("Month_Year", sort=False).agg({
        "Net Sales": lambda x: x.sum() / 100000.0,
        "Orders": "sum"
    }).reset_index()

    # Build Telegram Output Message
    msg = [
        f"📊 *BRANCH PERFORMANCE REPORT*",
        f"🏪 *Branch:* {matched_branch}",
        f"🏢 *Store Type:* {store_type}",
        f"📅 *Period:* Last {months} Months\n",
        f"💰 *Overall Summary*",
        f"💵 *Net Revenue:* ₹{net_sales:.2f}L",
        f"🧾 *Total Orders:* {int(orders):,}",
        f"🧺 *Avg AOV:* ₹{aov:.0f}",
        f"📉 *Avg Discount:* {dis_pct:.1f}%\n",
        f"📈 *Monthly Breakdown*"
    ]

    for _, row in monthly_trend.iterrows():
        msg.append(f"🔹 *{row['Month_Year']}:* ₹{row['Net Sales']:.2f}L ({int(row['Orders']):,} orders)")

    await update.message.reply_text("\n".join(msg), parse_mode="Markdown")

# =========================================================
# COMMAND HANDLER: /brand <Brand Name> <Period>
# =========================================================
async def brand_analysis(update: Update, context: ContextTypes.DEFAULT_TYPE):
    args = context.args
    if not args:
        await update.message.reply_text("⚠️ *Usage:* `/brand <Brand Name> <Period: 1m|3m|6m>`", parse_mode="Markdown")
        return

    months = extract_months(args)
    query_parts = [a for a in args if not ('m' in a.lower() and a[:-1].isdigit())]
    brand_query = " ".join(query_parts)

    df = load_historical_data(months=months)
    if df.empty or "Brand" not in df.columns:
        await update.message.reply_text("❌ No data or 'Brand' column found.")
        return

    filtered_df = df[df["Brand"].astype(str).str.contains(brand_query, case=False, na=False)].copy()
    if filtered_df.empty:
        await update.message.reply_text(f"❌ No records found for brand: *{brand_query}*", parse_mode="Markdown")
        return

    net_sales = filtered_df["Net Sales"].sum() / 100000.0
    orders = filtered_df["Orders"].sum()
    brand_name = filtered_df["Brand"].iloc[0]

    await update.message.reply_text(
        f"🏷️ *BRAND PERFORMANCE REPORT*\n"
        f"📌 *Brand:* {brand_name}\n"
        f"📅 *Period:* Last {months} Months\n\n"
        f"💵 *Net Sales:* ₹{net_sales:.2f}L\n"
        f"🧾 *Orders:* {int(orders):,}",
        parse_mode="Markdown"
    )

# =========================================================
# BOT SERVER STARTUP
# =========================================================
if __name__ == "__main__":
    bot_token = os.getenv("TELEGRAM_BOT_TOKEN")
    if not bot_token:
        raise ValueError("TELEGRAM_BOT_TOKEN environment variable is not set!")

    app = ApplicationBuilder().token(bot_token).build()
    
    app.add_handler(CommandHandler("store", store_analysis))
    app.add_handler(CommandHandler("brand", brand_analysis))
    
    print("🤖 Telegram Analytics Bot is running...")
    app.run_polling()
