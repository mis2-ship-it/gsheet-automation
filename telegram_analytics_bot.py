import os
import glob
import pandas as pd
from datetime import datetime, timedelta
from telegram import Update
from telegram.ext import ApplicationBuilder, CommandHandler, ContextTypes

def load_historical_data(months=3):
    """Loads CSV files from monthly_data/ and historical/ folders."""
    csv_files = glob.glob("monthly_data/**/*.csv", recursive=True) + \
                glob.glob("historical/**/*.csv", recursive=True)
    
    df_list = []
    for f in csv_files:
        try:
            df = pd.read_csv(f, low_memory=False)
            df_list.append(df)
        except Exception as e:
            print(f"Error loading {f}: {e}")
            
    if not df_list:
        return pd.DataFrame()
        
    full_df = pd.concat(df_list, ignore_index=True)
    full_df["Date"] = pd.to_datetime(full_df["Date"])
    
    cutoff_date = datetime.now() - timedelta(days=months * 30)
    return full_df[full_df["Date"] >= cutoff_date].copy()

async def store_analysis(update: Update, context: ContextTypes.DEFAULT_TYPE):
    args = context.args
    if not args or len(args) < 1:
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

    # Extract command arguments
    raw_branch = args[0]
    store_type = args[1].upper() if len(args) > 1 and args[1].upper() in ["COCO", "FOFO", "ALL"] else "ALL"
    
    # Extract time period
    period_arg = args[-1] if len(args) > 1 and 'm' in args[-1] else "3m"
    try:
        months = int(period_arg.replace('m', ''))
    except ValueError:
        months = 3

    await update.message.reply_text(
        f"⏳ Fetching *{raw_branch}* ({store_type}) performance for the last {months} months...", 
        parse_mode="Markdown"
    )
    
    df = load_historical_data(months=months)
    if df.empty:
        await update.message.reply_text("❌ No historical data found.")
        return

    # Filter by Branch Name
    filtered_df = df[df["Branch"].astype(str).str.contains(raw_branch, case=False, na=False)].copy()

    # Filter by Store Type if specified (assumes 'Type' column exists in your CSVs)
    if store_type in ["COCO", "FOFO"] and "Type" in filtered_df.columns:
        filtered_df = filtered_df[filtered_df["Type"].astype(str).str.upper() == store_type]

    if filtered_df.empty:
        await update.message.reply_text(f"❌ No records found matching Branch: '{raw_branch}' | Type: '{store_type}'.")
        return

    # Calculate Aggregated Metrics
    net_sales = filtered_df["Net Sales"].sum() / 100000.0  # In Lacs
    gross_sales = filtered_df["Gross Sales"].sum()
    discounts = filtered_df["Discount"].sum()
    orders = filtered_df["Orders"].sum()
    
    dis_pct = (discounts / gross_sales * 100) if gross_sales > 0 else 0
    aov = (filtered_df["Net Sales"].sum() / orders) if orders > 0 else 0
    matched_branch = filtered_df["Branch"].iloc[0]

    # Monthly Trend Breakdown
    filtered_df["Month_Year"] = filtered_df["Date"].dt.strftime("%b %Y")
    monthly_trend = filtered_df.groupby("Month_Year", sort=False).agg({
        "Net Sales": lambda x: x.sum() / 100000.0,
        "Orders": "sum"
    }).reset_index()

    # Build Telegram Output Response
    msg = [
        f"📊 *BRANCH PERFORMANCE REPORT*",
        f"🏪 *Branch:* {matched_branch}",
        f"🏢 *Store Type:* {store_type}",
        f"📅 *Period:* Last {months} Months\n",
        f"💰 *Overall Summary*",
        f"💵 *Net Revenue:* ₹{net_sales:.2f}L",
        f"🧾 *Total Orders:* {orders:,}",
        f"🧺 *Avg AOV:* ₹{aov:.0f}",
        f"📉 *Avg Discount:* {dis_pct:.1f}%\n",
        f"📈 *Monthly Breakdown*"
    ]

    for _, row in monthly_trend.iterrows():
        msg.append(f"🔹 *{row['Month_Year']}:* ₹{row['Net Sales']:.2f}L ({row['Orders']:,} orders)")

    await update.message.reply_text("\n".join(msg), parse_mode="Markdown")

if __name__ == "__main__":
    bot_token = os.getenv("TELEGRAM_BOT_TOKEN")
    app = ApplicationBuilder().token(bot_token).build()
    
    app.add_handler(CommandHandler("store", store_analysis))
    
    print("🤖 Telegram Analytics Bot is online...")
    app.run_polling()
