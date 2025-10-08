# Financial Services Demo with Snowflake Cortex AI
# https://www.webfx.com/tools/emoji-cheat-sheet/

import streamlit as st
from snowflake.snowpark.context import get_active_session
import snowflake.snowpark.functions as F
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

# Configure Streamlit page
st.set_page_config(
    page_title="Financial Services AI Demo",
    page_icon="🏦",
    layout="wide"
)

# Main title and description
st.title("🏦 Financial Services AI Analytics Demo")
st.markdown("""
### Powered by Snowflake Cortex AI Functions
This demo showcases intelligent financial data analysis using:
- **AI_COMPLETE**: Generate trading insights and summaries
- **AI_CLASSIFY**: Categorize trades and assess risk levels  
- **AI_FILTER**: Intelligent filtering with natural language
- **AI_AGG**: Aggregate insights across time periods
- **AI_SENTIMENT**: Analyze market sentiment from trading patterns

**Dataset includes:** Portfolio Managers, Traders, Trades, Stock History, and Current Positions
""")

# Get the current session
session = get_active_session()

# Helper function to get filtered/sampled data for AI analysis with better diversity
def get_sample_data_sql(limit=1000, date_filter="All Available Data", additional_where=""):
    """Generate SQL for sampling large dataset efficiently with diversity across traders, stocks, and trade sizes"""
    
    # Date filter conditions based on the actual max date in the dataset
    date_conditions = {
        "Most Recent 30 Days": """AND "DATE" >= (
            SELECT DATEADD(day, -30, MAX("DATE")) FROM FINSERVAM_DEMO.AISQL_DEMO.TRADE
        )""",
        "Most Recent 90 Days": """AND "DATE" >= (
            SELECT DATEADD(day, -90, MAX("DATE")) FROM FINSERVAM_DEMO.AISQL_DEMO.TRADE
        )""",
        "Most Recent Year": """AND "DATE" >= (
            SELECT DATEADD(year, -1, MAX("DATE")) FROM FINSERVAM_DEMO.AISQL_DEMO.TRADE
        )""",
        "All Available Data": ""
    }
    
    date_clause = date_conditions.get(date_filter, "")
    where_clause = f"WHERE 1=1 {date_clause} {additional_where}" if date_clause or additional_where else ""
    
    # Simplified but effective sampling strategy
    return f"""
    SELECT * FROM FINSERVAM_DEMO.AISQL_DEMO.TRADE 
    {where_clause}
    ORDER BY RANDOM()
    LIMIT {limit}
    """

# Sidebar for navigation
st.sidebar.title("🧭 Navigation")
analysis_type = st.sidebar.radio(
    "Choose Analysis Type:",
    ["📊 Basic Analytics", "🤖 AI-Powered Insights", "🔍 Intelligent Filtering", "📈 Market Sentiment", "💬 Natural Language Queries"]
)

# Sampling controls for large dataset
st.sidebar.markdown("---")
st.sidebar.markdown("### ⚙️ Data Sampling")
st.sidebar.info("💡 With 10B+ rows, we use smart sampling for AI analysis")

sample_size = st.sidebar.selectbox(
    "AI Analysis Sample Size:",
    [100, 500, 1000, 5000, 10000],
    index=4,  # Default to 1000
    help="Number of rows to analyze with AI functions"
)

date_filter = st.sidebar.selectbox(
    "Data Range:",
    ["Most Recent 30 Days", "Most Recent 90 Days", "Most Recent Year", "All Available Data"],
    index=3,  # Default to all data since it's demo data
    help="Filter based on the most recent data in the dataset (not current date)"
)

# Get Snowpark DataFrames (stay in Snowflake - no data movement)
try:
    df_trader = session.table("FINSERVAM_DEMO.AISQL_DEMO.TRADER")
    df_trades = session.table("FINSERVAM_DEMO.AISQL_DEMO.TRADE") 
    df_position = session.table("FINSERVAM_DEMO.AISQL_DEMO.POSITION_NOW")
    
    # Test connection by getting row counts
    trader_count = df_trader.count()
    trades_count = df_trades.count()
    
    st.sidebar.success(f"✅ Connected to Snowflake")
    st.sidebar.info(f"📊 {trader_count:,} traders, {trades_count:,} trades")
    st.sidebar.warning(f"🎯 AI analysis uses {sample_size:,} row samples")
    
    # Show actual date range in the dataset
    try:
        date_range_query = """
        SELECT 
            MIN("DATE") as earliest_date,
            MAX("DATE") as latest_date,
            DATEDIFF(day, MIN("DATE"), MAX("DATE")) as total_days
        FROM FINSERVAM_DEMO.AISQL_DEMO.TRADE
        """
        date_info = session.sql(date_range_query).to_pandas().iloc[0]
        st.sidebar.info(f"📅 Data: {date_info['EARLIEST_DATE']} to {date_info['LATEST_DATE']} ({date_info['TOTAL_DAYS']} days)")
    except Exception as e:
        st.sidebar.warning("📅 Could not determine date range")
    
    # Show table structure for debugging
    if st.sidebar.button("🔍 Show Table Structure"):
        st.sidebar.write("**TRADE Table Columns:**")
        try:
            sample_row = df_trades.limit(1).to_pandas()
            for col in sample_row.columns:
                st.sidebar.write(f"- {col}")
        except Exception as e:
            st.sidebar.error(f"Error: {e}")
    
except Exception as e:
    st.error(f"❌ Error connecting to Snowflake tables: {e}")
    st.stop()

# Debug section - show available data
if st.sidebar.button("🔍 Show Available Traders"):
    st.sidebar.write("**All Traders:**")
    try:
        all_traders = df_trader.select("TRADER", "PM").distinct().collect()
        for trader_row in all_traders:
            st.sidebar.write(f"- {trader_row['TRADER']} (PM: {trader_row['PM']})")
    except Exception as e:
        st.sidebar.error(f"Error: {e}")

# =================== BASIC ANALYTICS ===================
if analysis_type == "📊 Basic Analytics":
    st.header("📊 Basic Financial Analytics")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("🎯 Trader Selection")

        # Get unique PMs using Snowpark (stays in Snowflake)
        pm_list = df_trader.select("PM").distinct().collect()
        pm_options = [row['PM'] for row in pm_list]
        
        # Set the default PM to 'Kim Stephens'
        default_pm_index = 0
        if 'Kim Stephens' in pm_options:
            default_pm_index = pm_options.index('Kim Stephens')
        
        portfolio_manager = st.selectbox("Portfolio Manager:", pm_options, index=default_pm_index)
         
        # Get traders for selected PM
        trader_list = df_trader.filter(F.col("PM") == portfolio_manager).select("TRADER").distinct().collect()
        trader_options = [row['TRADER'] for row in trader_list]
        
        # Set the default trader to 'Jesse Henry' or first available
        default_trader_index = 0
        if 'Jesse Henry' in trader_options:
            default_trader_index = trader_options.index('Jesse Henry')
        elif len(trader_options) > 0:
            # If Jesse Henry not found, use the first trader
            default_trader_index = 0
        
        trader = st.selectbox("Trader:", trader_options, index=default_trader_index)
        
        # Get buying power for selected trader with error handling
        try:
            buying_power_result = df_trader.filter(F.col("TRADER") == trader).select("BUYING_POWER").collect()
            if buying_power_result and len(buying_power_result) > 0:
                buying_power = buying_power_result[0]['BUYING_POWER']
                if buying_power is not None:
                    st.metric("💰 Buying Power", f"${buying_power:,.2f}")
                else:
                    st.metric("💰 Buying Power", "N/A")
            else:
                st.metric("💰 Buying Power", "No data")
        except Exception as e:
            st.error(f"Error getting buying power: {e}")
            st.metric("💰 Buying Power", "Error")
    
    with col2:
        st.subheader("📈 Quick Stats")
        
        # Get trade statistics using Snowpark aggregations
        trader_trades_df = df_trades.filter(F.col("TRADER") == trader)
        
        # Calculate stats in Snowflake
        stats = trader_trades_df.agg([
            F.count("*").alias("total_trades"),
            F.count_distinct(F.col("SYMBOL")).alias("unique_stocks"), 
            F.avg(F.col("NUM_SHARES")).alias("avg_quantity")
        ]).collect()[0]
        
        col2a, col2b, col2c = st.columns(3)
        with col2a:
            st.metric("🔄 Total Trades", stats['TOTAL_TRADES'] or 0)
        with col2b:
            st.metric("📊 Unique Stocks", stats['UNIQUE_STOCKS'] or 0)
        with col2c:
            # Handle None values for average quantity
            avg_qty = stats['AVG_QUANTITY']
            if avg_qty is not None:
                st.metric("📏 Avg Quantity", f"{avg_qty:.0f}")
            else:
                st.metric("📏 Avg Quantity", "N/A")
    
    # Display trader's trades (only convert to pandas for display)
    st.subheader(f"📋 Trades by {trader}")
    try:
        trader_trades_display = trader_trades_df.to_pandas()
        if len(trader_trades_display) > 0:
            st.dataframe(trader_trades_display, use_container_width=True)
        else:
            st.info(f"No trades found for trader: {trader}")
    except Exception as e:
        st.error(f"Error displaying trades: {e}")
    
    # Large quantity filter
    st.subheader("🔍 Large Quantity Filter")
    col3, col4 = st.columns(2)
    with col3:
        quantity_threshold = st.number_input("Minimum Quantity Threshold:", min_value=1, value=1000)
    with col4:
        # Get stock list efficiently
        stock_list = df_trades.select(F.col("SYMBOL")).distinct().collect()
        stock_options = ['ALL'] + [row["SYMBOL"] for row in stock_list]
        selected_stock = st.selectbox("Stock Symbol:", stock_options)
    
    # Filter large trades using Snowpark
    large_trades_df = df_trades.filter(F.col("NUM_SHARES") >= quantity_threshold)
    if selected_stock != 'ALL':
        large_trades_df = large_trades_df.filter(F.col("SYMBOL") == selected_stock)
    
    # Get count and display
    large_trades_count = large_trades_df.count()
    st.write(f"**Found {large_trades_count} trades with quantity >= {quantity_threshold}**")
    
    if large_trades_count > 0:
        # Only convert to pandas for display, limit to reasonable number
        large_trades_display = large_trades_df.limit(1000).to_pandas()
        st.dataframe(large_trades_display, use_container_width=True)

# =================== AI-POWERED INSIGHTS ===================
elif analysis_type == "🤖 AI-Powered Insights":
    st.header("🤖 AI-Powered Trading Insights")
    
    # AI_COMPLETE for trading insights
    st.subheader("💡 AI Trading Analysis")
    
    col1, col2 = st.columns(2)
    
    with col1:
        # Get trader list using Snowpark
        trader_list = df_trader.select("TRADER").distinct().collect()
        trader_options = [row['TRADER'] for row in trader_list]
        
        # Set default to Jesse Henry
        default_trader_index = 0
        if 'Jesse Henry' in trader_options:
            default_trader_index = trader_options.index('Jesse Henry')
        
        selected_trader = st.selectbox("Select Trader for Analysis:", trader_options, index=default_trader_index)
    
    with col2:
        analysis_period = st.selectbox("Analysis Period:", ["All Time", "Last 7 Days", "Last 30 Days"])
    
    if st.button("🔍 Generate AI Insights"):
        # Clear explanation of what's happening
        st.info("🔄 **What's happening:** Using AI_COMPLETE to analyze trader performance data and generate intelligent insights about trading patterns, risk profile, and recommendations.")
        
        with st.spinner("🤖 Generating AI insights..."):
            try:
                # Use SQL directly to avoid Snowpark column name issues
                trader_stats_query = f"""
                WITH trader_data AS (
                    SELECT * FROM FINSERVAM_DEMO.AISQL_DEMO.TRADE 
                    WHERE "TRADER" = '{selected_trader}'
                ),
                stats AS (
                    SELECT 
                        COUNT(*) as total_trades,
                        COUNT(DISTINCT "SYMBOL") as unique_stocks,
                        AVG("NUM_SHARES") as avg_quantity,
                        SUM("NUM_SHARES") as total_volume
                    FROM trader_data
                ),
                top_stock AS (
                    SELECT "SYMBOL" as most_traded_stock
                    FROM trader_data
                    GROUP BY "SYMBOL"
                    ORDER BY COUNT(*) DESC
                    LIMIT 1
                )
                SELECT 
                    s.total_trades,
                    s.unique_stocks,
                    s.avg_quantity,
                    s.total_volume,
                    COALESCE(t.most_traded_stock, 'N/A') as most_traded_stock
                FROM stats s
                CROSS JOIN top_stock t
                """
                
                result = session.sql(trader_stats_query).to_pandas()
                
                if len(result) > 0:
                    row = result.iloc[0]
                    # Create trade summary for AI analysis
                    trade_summary = f"""
                    Trader: {selected_trader}
                    Total Trades: {row['TOTAL_TRADES']}
                    Unique Stocks: {row['UNIQUE_STOCKS']}
                    Average Quantity: {row['AVG_QUANTITY']:.0f}
                    Most Traded Stock: {row['MOST_TRADED_STOCK']}
                    Total Volume: {row['TOTAL_VOLUME']:,}
                    """
                else:
                    trade_summary = f"No trades found for trader: {selected_trader}"
                
                # AI_COMPLETE for insights
                insights_query = f"""
                SELECT AI_COMPLETE('mistral-7b', 
                    'Analyze this trader\\'s performance and provide 3 key insights about their trading patterns and risk profile: {trade_summary}'
                ) as trading_insights
                """
                
                result = session.sql(insights_query).to_pandas()
                
                st.success("✅ AI Analysis Complete!")
                
                # Format the AI insights better
                st.markdown("### 🧠 AI-Generated Trading Insights")
                
                insights_text = result['TRADING_INSIGHTS'].iloc[0]
                
                # Clean up the text and format it properly
                # Remove extra whitespace and newlines
                cleaned_text = insights_text.replace('\\n', '\n').strip()
                
                # Create an attractive container for the insights
                with st.container():
                    st.markdown("""
                    <div style="background-color: #f8f9fa; padding: 25px; border-radius: 12px; border-left: 5px solid #1f77b4; margin: 10px 0;">
                    """, unsafe_allow_html=True)
                    
                    # Split the text into sections and format each one
                    sections = cleaned_text.split('\n\n')
                    
                    for i, section in enumerate(sections):
                        section = section.strip()
                        if not section:
                            continue
                            
                        # Check if this is an introduction/header
                        if i == 0 and not any(section.startswith(marker) for marker in ['1.', '2.', '3.', '1)', '2)', '3)', '•', '-']):
                            st.markdown(f"**{section}**")
                            st.markdown("---")
                            continue
                        
                        # Process numbered insights
                        lines = section.split('\n')
                        for line in lines:
                            line = line.strip()
                            if not line:
                                continue
                                
                            # Handle numbered points (1., 2., 3.)
                            if any(line.startswith(f"{num}.") for num in ['1', '2', '3']):
                                # Extract the title and content
                                parts = line.split(':', 1)
                                if len(parts) == 2:
                                    title = parts[0].strip()
                                    content = parts[1].strip()
                                    
                                    # Create an insight card
                                    st.markdown(f"""
                                    <div style="background-color: white; padding: 15px; border-radius: 8px; margin: 10px 0; border-left: 4px solid #28a745;">
                                        <h4 style="color: #28a745; margin-top: 0;">{title}</h4>
                                        <p style="margin-bottom: 0; line-height: 1.6;">{content}</p>
                                    </div>
                                    """, unsafe_allow_html=True)
                                else:
                                    # Fallback for different formatting
                                    st.markdown(f"""
                                    <div style="background-color: white; padding: 15px; border-radius: 8px; margin: 10px 0; border-left: 4px solid #28a745;">
                                        <p style="margin: 0; line-height: 1.6;"><strong>{line}</strong></p>
                                    </div>
                                    """, unsafe_allow_html=True)
                            else:
                                # Handle continuation text or other content
                                if line:
                                    st.markdown(f"<p style='margin: 5px 0; line-height: 1.6; color: #555;'>{line}</p>", unsafe_allow_html=True)
                    
                    st.markdown("</div>", unsafe_allow_html=True)
                
                # Add trader summary for context
                st.markdown("---")
                st.markdown("### 📊 Trader Performance Summary")
                
                # Get the stats from our earlier query
                trader_stats_result = session.sql(trader_stats_query).to_pandas()
                if len(trader_stats_result) > 0:
                    stats_row = trader_stats_result.iloc[0]
                    
                    # Create metrics columns for the summary
                    col1, col2, col3, col4 = st.columns(4)
                    
                    with col1:
                        st.metric(
                            label="Total Trades",
                            value=f"{stats_row['TOTAL_TRADES']:,}",
                            help="Total number of trades executed"
                        )
                    
                    with col2:
                        st.metric(
                            label="Unique Stocks",
                            value=f"{stats_row['UNIQUE_STOCKS']:,}",
                            help="Number of different stocks traded"
                        )
                    
                    with col3:
                        avg_qty = stats_row['AVG_QUANTITY']
                        if avg_qty is not None:
                            st.metric(
                                label="Avg Trade Size",
                                value=f"{avg_qty:,.0f}",
                                help="Average number of shares per trade"
                            )
                        else:
                            st.metric(label="Avg Trade Size", value="N/A")
                    
                    with col4:
                        total_vol = stats_row['TOTAL_VOLUME']
                        if total_vol is not None:
                            st.metric(
                                label="Total Volume",
                                value=f"{total_vol:,.0f}",
                                help="Total shares traded"
                            )
                        else:
                            st.metric(label="Total Volume", value="N/A")
                    
                    # Most traded stock info
                    most_traded = stats_row['MOST_TRADED_STOCK']
                    if most_traded and most_traded != 'N/A':
                        st.info(f"🎯 **Most Traded Stock:** {most_traded}")
                
            except Exception as e:
                st.error(f"❌ Error generating insights: {e}")
                st.error("Debug info: Check if AI_COMPLETE function is available and properly configured")
    
    # AI_CLASSIFY for trade categorization
    st.subheader("🏷️ Trade Classification")
    
    if st.button("📊 Classify Recent Trades"):
        # Clear explanation of what's happening
        st.info("🔄 **What's happening:** Using AI_CLASSIFY to automatically categorize a sample of recent trades by volume and activity patterns (High/Medium/Low Volume, Unusual Activity).")
        
        with st.spinner("🏷️ Classifying trades..."):
            try:
                # Use smart sampling for AI classification
                sample_limit = min(sample_size, 50)  # Limit classification to reasonable number
                
                classify_query = f"""
                WITH sample_trades AS (
                    {get_sample_data_sql(sample_limit, date_filter)}
                ),
                trade_descriptions AS (
                    SELECT 
                        "DATE",
                        "SYMBOL",
                        "NUM_SHARES",
                        "TRADER",
                        "ACTION",
                        CONCAT('Stock: ', "SYMBOL", ', Shares: ', "NUM_SHARES", ', Action: ', "ACTION") as trade_description
                    FROM sample_trades
                )
                SELECT 
                    "DATE",
                    "SYMBOL",
                    "NUM_SHARES",
                    "TRADER",
                    "ACTION",
                    AI_CLASSIFY(
                        trade_description,
                        ARRAY_CONSTRUCT('High Volume', 'Medium Volume', 'Low Volume', 'Unusual Activity')
                    ) as ai_classification
                FROM trade_descriptions
                ORDER BY "DATE" DESC
                """
                
                classification_results = session.sql(classify_query).to_pandas()
                
                if len(classification_results) > 0:
                    st.success(f"✅ Classification Complete! Analyzed {len(classification_results)} trades")
                    st.dataframe(classification_results)
                else:
                    st.warning("⚠️ No trades found to classify. Try adjusting your date filter or sample size.")
                
            except Exception as e:
                st.error(f"❌ Error classifying trades: {e}")
                st.error("Debug info: Check if AI_CLASSIFY function is available and sample data exists")

# =================== INTELLIGENT FILTERING ===================
elif analysis_type == "🔍 Intelligent Filtering":
    st.header("🔍 Intelligent Trade Filtering")
    
    st.markdown("Use natural language to filter your trading data!")
    
    filter_options = [
        "Show me high-volume trades",
        "Find potentially risky transactions", 
        "Show me trades that might indicate market volatility",
        "Find trades with unusual timing patterns",
        "Show me trades that suggest strong market confidence"
    ]
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("🎯 Quick Filters")
        selected_filter = st.selectbox("Choose a suggested filter:", [""] + filter_options)
        
        # Auto-populate the text input when a filter is selected
        if selected_filter:
            filter_query = selected_filter
        else:
            filter_query = ""
    
    with col2:
        st.subheader("⚙️ Settings")
        custom_threshold = st.number_input("Quantity Threshold:", min_value=1, value=50)
    
    # Natural language filter input - use the selected filter as default
    st.subheader("🗣️ Filter Description")
    filter_query = st.text_input(
        "Describe what trades you want to see:",
        value=filter_query,  # Pre-populate with selected filter
        placeholder="e.g., 'Show me high-risk trades' or 'Find trades with unusual patterns'"
    )
    
    if filter_query and st.button("🔍 Apply AI Filter", type="primary"):
        # Clear explanation of what's happening
        st.info(f"🔄 **What's happening:** Using AI_FILTER with PROMPT to intelligently evaluate trade data against your criteria: '{filter_query}'. The AI will analyze each trade and determine if it matches your requirements.")
        
        with st.spinner("🤖 Applying intelligent filter..."):
            try:
                # Use smart sampling for AI filtering
                filter_limit = min(sample_size, 100)  # Limit for AI_FILTER performance
                
                filter_sql = f"""
                WITH sample_trades AS (
                    {get_sample_data_sql(filter_limit, date_filter)}
                ),
                trade_descriptions AS (
                    SELECT 
                        "DATE",
                        "SYMBOL",
                        "NUM_SHARES",
                        "TRADER",
                        "ACTION",
                        CONCAT('Trade: Stock ', "SYMBOL", ', Shares: ', "NUM_SHARES", ', Trader: ', "TRADER", ', Action: ', "ACTION", ', Date: ', "DATE") as trade_description
                    FROM sample_trades
                )
                SELECT 
                    "DATE",
                    "SYMBOL",
                    "NUM_SHARES",
                    "TRADER",
                    "ACTION"
                FROM trade_descriptions
                WHERE AI_FILTER(PROMPT('{filter_query}: {{0}}', trade_description))
                ORDER BY "DATE" DESC
                """
                
                filtered_results = session.sql(filter_sql).to_pandas()
                
                if len(filtered_results) > 0:
                    st.success(f"✅ AI_FILTER found {len(filtered_results)} matching trades!")
                    
                    # Show summary stats
                    col1, col2, col3 = st.columns(3)
                    with col1:
                        st.metric("Filtered Trades", len(filtered_results))
                    with col2:
                        st.metric("Avg Volume", f"{filtered_results['NUM_SHARES'].mean():,.0f}")
                    with col3:
                        st.metric("Unique Stocks", filtered_results['SYMBOL'].nunique())
                    
                    st.dataframe(filtered_results, use_container_width=True)
                else:
                    st.info("ℹ️ AI_FILTER found no trades matching your criteria. Try a different filter or broader terms.")
                    
            except Exception as e:
                st.error(f"❌ AI_FILTER error: {e}")
                st.warning("🔄 Falling back to rule-based filtering...")
                
                # Fallback to rule-based filtering (keeping the existing fallback code)
                try:
                    sample_query = f"""
                    WITH sample_trades AS (
                        {get_sample_data_sql(filter_limit, date_filter)}
                    )
                    SELECT 
                        "DATE",
                        "SYMBOL",
                        "NUM_SHARES",
                        "TRADER",
                        "ACTION"
                    FROM sample_trades
                    ORDER BY "DATE" DESC
                    """
                    
                    sample_results = session.sql(sample_query).to_pandas()
                    
                    if len(sample_results) > 0:
                        # Apply rule-based filtering
                        filtered_df = sample_results.copy()
                        filter_applied = False
                        
                        if "high-volume" in filter_query.lower() or "high volume" in filter_query.lower():
                            threshold = sample_results['NUM_SHARES'].quantile(0.8)
                            filtered_df = filtered_df[filtered_df['NUM_SHARES'] >= threshold]
                            st.info(f"📊 Rule-based filter: high-volume trades (>= {threshold:.0f} shares)")
                            filter_applied = True
                            
                        elif "low-volume" in filter_query.lower() or "low volume" in filter_query.lower():
                            threshold = sample_results['NUM_SHARES'].quantile(0.2)
                            filtered_df = filtered_df[filtered_df['NUM_SHARES'] <= threshold]
                            st.info(f"📊 Rule-based filter: low-volume trades (<= {threshold:.0f} shares)")
                            filter_applied = True
                            
                        elif "risky" in filter_query.lower() or "risk" in filter_query.lower():
                            threshold = sample_results['NUM_SHARES'].quantile(0.9)
                            filtered_df = filtered_df[filtered_df['NUM_SHARES'] >= threshold]
                            st.info(f"⚠️ Rule-based filter: high-risk trades (>= {threshold:.0f} shares)")
                            filter_applied = True
                            
                        elif "volatility" in filter_query.lower() or "volatile" in filter_query.lower():
                            stock_counts = sample_results.groupby('SYMBOL').size()
                            volatile_stocks = stock_counts[stock_counts >= stock_counts.quantile(0.7)].index
                            filtered_df = filtered_df[filtered_df['SYMBOL'].isin(volatile_stocks)]
                            st.info(f"📈 Rule-based filter: trades in frequently traded stocks")
                            filter_applied = True
                            
                        elif "confidence" in filter_query.lower() or "strong" in filter_query.lower():
                            threshold = sample_results['NUM_SHARES'].quantile(0.75)
                            filtered_df = filtered_df[filtered_df['NUM_SHARES'] >= threshold]
                            st.info(f"💪 Rule-based filter: high-confidence trades (>= {threshold:.0f} shares)")
                            filter_applied = True
                        
                        if not filter_applied:
                            filtered_df = filtered_df[filtered_df['NUM_SHARES'] >= custom_threshold]
                            st.info(f"🔧 Rule-based filter: custom threshold (>= {custom_threshold} shares)")
                        
                        if len(filtered_df) > 0:
                            st.success(f"✅ Fallback filtering found {len(filtered_df)} trades!")
                            
                            # Show summary stats for fallback
                            col1, col2, col3 = st.columns(3)
                            with col1:
                                st.metric("Filtered Trades", len(filtered_df))
                            with col2:
                                st.metric("Avg Volume", f"{filtered_df['NUM_SHARES'].mean():,.0f}")
                            with col3:
                                st.metric("Unique Stocks", filtered_df['SYMBOL'].nunique())
                            
                            st.dataframe(filtered_df, use_container_width=True)
                        else:
                            st.warning("⚠️ No trades matched the fallback criteria either.")
                    
                except Exception as fallback_error:
                    st.error(f"❌ Fallback filtering also failed: {fallback_error}")

# =================== MARKET SENTIMENT ===================
elif analysis_type == "📈 Market Sentiment":
    st.header("📈 Market Sentiment Analysis")
    
    st.markdown("Analyze market sentiment based on trading patterns and volumes.")
    
    # AI_SENTIMENT and AI_AGG for market analysis
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("📊 Trading Volume Sentiment")
        if st.button("🎭 Analyze Volume Sentiment"):
            # Clear explanation of what's happening
            st.info("🔄 **What's happening:** Using AI_SENTIMENT to analyze trading volume descriptions and determine market sentiment (positive/negative/neutral) for top traded stocks.")
            
            with st.spinner("🎭 Analyzing market sentiment..."):
                try:
                    # Use sampled data for sentiment analysis
                    sentiment_limit = min(sample_size, 100)  # Limit for sentiment analysis
                    
                    sentiment_query = f"""
                    WITH sample_data AS (
                        {get_sample_data_sql(sentiment_limit * 5, date_filter)}
                    ),
                    stock_stats AS (
                        SELECT 
                            "SYMBOL",
                            SUM("NUM_SHARES") as total_volume,
                            COUNT(*) as trade_count,
                            AVG("NUM_SHARES") as avg_quantity,
                            CONCAT('Stock ', "SYMBOL", ' has total volume of ', SUM("NUM_SHARES"), 
                                   ' shares across ', COUNT(*), ' trades with average of ', 
                                   ROUND(AVG("NUM_SHARES"), 0), ' shares per trade in recent period') as volume_description
                        FROM sample_data
                        GROUP BY "SYMBOL"
                        HAVING COUNT(*) >= 2  -- Ensure we have enough data
                        ORDER BY total_volume DESC
                        LIMIT 10
                    )
                    SELECT 
                        "SYMBOL",
                        total_volume,
                        trade_count,
                        AI_SENTIMENT(volume_description) as sentiment_score
                    FROM stock_stats
                    """
                    
                    sentiment_results = session.sql(sentiment_query).to_pandas()
                    
                    if len(sentiment_results) > 0:
                        st.success("✅ Sentiment Analysis Complete!")
                        st.dataframe(sentiment_results)
                    else:
                        st.warning("⚠️ No sufficient data for sentiment analysis. Try adjusting your date filter or sample size.")
                    
                except Exception as e:
                    st.error(f"❌ Error analyzing sentiment: {e}")
                    st.error("Debug info: Check if AI_SENTIMENT function is available and sample data exists")
    
    with col2:
        st.subheader("📈 Aggregated Market Insights")
        if st.button("🧮 Generate Market Summary"):
            # Clear explanation of what's happening
            st.info("🔄 **What's happening:** Using AI_AGG to aggregate market data and generate comprehensive insights about trading trends, risk factors, and market activity patterns.")
            
            with st.spinner("🧮 Aggregating market insights..."):
                try:
                    # Use sampled data for market analysis
                    market_sample = min(sample_size * 3, 5000)  # Larger sample for market overview
                    
                    agg_query = f"""
                    WITH sample_data AS (
                        {get_sample_data_sql(market_sample, date_filter)}
                    ),
                    market_stats AS (
                        SELECT 
                            COUNT(*) as sample_trades,
                            SUM("NUM_SHARES") as total_volume,
                            COUNT(DISTINCT "SYMBOL") as unique_stocks,
                            AVG("NUM_SHARES") as avg_trade_size,
                            COUNT(DISTINCT "TRADER") as active_traders
                        FROM sample_data
                    ),
                    top_stock AS (
                        SELECT "SYMBOL" as most_active_stock
                        FROM sample_data
                        GROUP BY "SYMBOL"
                        ORDER BY COUNT(*) DESC
                        LIMIT 1
                    ),
                    market_summary AS (
                        SELECT CONCAT(
                            'Recent Market Analysis (', '{date_filter}', ' sample): Trades Analyzed: ', ms.sample_trades,
                            ', Total Volume: ', ms.total_volume, ' shares',
                            ', Unique Stocks: ', ms.unique_stocks,
                            ', Active Traders: ', ms.active_traders,
                            ', Most Active Stock: ', COALESCE(ts.most_active_stock, 'N/A'),
                            ', Average Trade Size: ', ROUND(ms.avg_trade_size, 0), ' shares'
                        ) as summary_text
                        FROM market_stats ms
                        CROSS JOIN top_stock ts
                    )
                    SELECT AI_AGG(
                        summary_text,
                        'Provide a comprehensive market analysis including key trends, risk factors, and trading activity insights based on this recent sample.'
                    ) as market_insights
                    FROM market_summary
                    """
                    
                    result = session.sql(agg_query).to_pandas()
                    
                    if len(result) > 0 and result['MARKET_INSIGHTS'].iloc[0]:
                        st.success("✅ Market Analysis Complete!")
                        st.markdown("### 🎯 AI Market Insights:")
                        st.write(result['MARKET_INSIGHTS'].iloc[0])
                    else:
                        st.warning("⚠️ No sufficient data for market analysis. Try adjusting your date filter or sample size.")
                    
                except Exception as e:
                    st.error(f"❌ Error generating market summary: {e}")
                    st.error("Debug info: Check if AI_AGG function is available and sample data exists")

# =================== NATURAL LANGUAGE QUERIES ===================
elif analysis_type == "💬 Natural Language Queries":
    st.header("💬 Natural Language Trading Queries")
    
    st.markdown("Ask questions about your trading data in plain English!")
    
    # Predefined query examples
    st.subheader("🎯 Quick Questions")
    
    query_examples = [
        "What are the riskiest trades in my portfolio?",
        "Which stocks show the most volatile trading patterns?", 
        "What trading strategies would you recommend based on recent activity?",
        "Are there any unusual trading patterns I should be concerned about?",
        "What insights can you provide about market momentum?"
    ]
    
    selected_query = st.selectbox("Choose a question or type your own:", ["Custom Query"] + query_examples)
    
    if selected_query != "Custom Query":
        user_query = selected_query
    else:
        user_query = st.text_input("💭 Ask your question:", placeholder="e.g., What patterns do you see in recent trading activity?")
    
    if user_query and st.button("🤖 Get AI Answer"):
        # Clear explanation of what's happening
        st.info(f"🔄 **What's happening:** Using AI_COMPLETE to analyze trading data context and provide intelligent answers to your question: '{user_query}'. The AI will examine trading patterns, volumes, and trends to give actionable insights.")
        
        with st.spinner("🤖 Analyzing your question..."):
            try:
                # Use sampled data for natural language queries
                context_sample = min(sample_size * 3, 5000)  # Reasonable sample for context
                
                complete_query = f"""
                WITH sample_data AS (
                    {get_sample_data_sql(context_sample, date_filter)}
                ),
                trading_context AS (
                    SELECT 
                        COUNT(*) as sample_trades,
                        COUNT(DISTINCT "SYMBOL") as unique_stocks,
                        SUM("NUM_SHARES") as total_volume,
                        COUNT(DISTINCT "TRADER") as active_traders,
                        MAX("NUM_SHARES") as highest_volume_trade,
                        AVG("NUM_SHARES") as avg_trade_size
                    FROM sample_data
                ),
                top_stocks AS (
                    SELECT LISTAGG("SYMBOL", ', ') WITHIN GROUP (ORDER BY trade_count DESC) as top_5_stocks
                    FROM (
                        SELECT "SYMBOL", COUNT(*) as trade_count
                        FROM sample_data
                        GROUP BY "SYMBOL"
                        ORDER BY trade_count DESC
                        LIMIT 5
                    )
                ),
                context_summary AS (
                    SELECT CONCAT(
                        'Recent Trading Analysis (', '{date_filter}', '): Sample Trades: ', tc.sample_trades,
                        ', Unique Stocks: ', tc.unique_stocks,
                        ', Total Volume: ', tc.total_volume, ' shares',
                        ', Active Traders: ', tc.active_traders,
                        ', Top 5 Most Traded Stocks: ', COALESCE(ts.top_5_stocks, 'N/A'),
                        ', Highest Volume Trade: ', tc.highest_volume_trade, ' shares',
                        ', Average Trade Size: ', ROUND(tc.avg_trade_size, 0), ' shares'
                    ) as context_data
                    FROM trading_context tc
                    CROSS JOIN top_stocks ts
                )
                SELECT AI_COMPLETE('mistral-7b',
                    CONCAT('Based on this recent financial trading data sample, please answer: {user_query}. Context: ', 
                           context_data, '. Note: This analysis is based on a recent sample, not the full dataset. Please provide actionable insights.')
                ) as ai_response
                FROM context_summary
                """
                
                result = session.sql(complete_query).to_pandas()
                
                if len(result) > 0 and result['AI_RESPONSE'].iloc[0]:
                    st.success("✅ AI Analysis Complete!")
                    
                    # Display the response in a beautifully formatted container
                    st.markdown("### 🤖 AI Response")
                    
                    response = result['AI_RESPONSE'].iloc[0]
                    
                    # Create an attractive container for the AI response
                    with st.container():
                        st.markdown("""
                        <div style="background-color: #e8f4fd; padding: 20px; border-radius: 10px; border-left: 5px solid #0066cc;">
                        """, unsafe_allow_html=True)
                        
                        # Format the response with better structure
                        if '\n' in response:
                            # Split into paragraphs for better readability
                            paragraphs = [p.strip() for p in response.split('\n') if p.strip()]
                            for paragraph in paragraphs:
                                if paragraph:
                                    st.markdown(f"💡 {paragraph}")
                                    st.markdown("")  # Add spacing
                        else:
                            st.markdown(f"💡 **{response}**")
                        
                        st.markdown("</div>", unsafe_allow_html=True)
                    
                    # Add follow-up suggestions with better formatting
                    st.markdown("---")
                    st.markdown("### 🎯 Recommended Follow-up Actions")
                    
                    follow_up_query = f"""
                    SELECT AI_COMPLETE('mistral-7b',
                        'Based on the previous analysis, suggest 3 specific follow-up actions or analyses that would be valuable for this trading portfolio.'
                    ) as follow_up_suggestions
                    """
                    
                    follow_up_result = session.sql(follow_up_query).to_pandas()
                    if len(follow_up_result) > 0:
                        suggestions = follow_up_result['FOLLOW_UP_SUGGESTIONS'].iloc[0]
                        
                        # Format follow-up suggestions in a nice container
                        with st.container():
                            st.markdown("""
                            <div style="background-color: #f8f9fa; padding: 15px; border-radius: 8px; border-left: 4px solid #28a745;">
                            """, unsafe_allow_html=True)
                            
                            if '\n' in suggestions:
                                lines = [line.strip() for line in suggestions.split('\n') if line.strip()]
                                for line in lines:
                                    if line:
                                        if any(line.startswith(marker) for marker in ['1.', '2.', '3.', '•', '-']):
                                            st.markdown(f"✅ **{line}**")
                                        else:
                                            st.markdown(f"✅ {line}")
                            else:
                                st.markdown(f"✅ **{suggestions}**")
                            
                            st.markdown("</div>", unsafe_allow_html=True)
                
            except Exception as e:
                st.error(f"❌ Error processing query: {e}")
                st.error("Debug info: Check if AI_COMPLETE function is available and sample data exists")

# =================== FOOTER ===================
st.markdown("---")
st.markdown("### 🎯 About This Demo")
st.markdown("""
This financial services demo showcases the power of **Snowflake Cortex AI functions** for intelligent data analysis:

- **AI_COMPLETE**: Generates comprehensive trading insights and recommendations
- **AI_CLASSIFY**: Automatically categorizes trades by risk level and volume
- **AI_FILTER**: Enables natural language filtering of complex trading data  
- **AI_AGG**: Aggregates insights across multiple data points and time periods
- **AI_SENTIMENT**: Analyzes market sentiment from trading patterns and volumes

**Key Benefits:**
- 🚀 **Speed**: Get instant insights without complex queries
- 🎯 **Accuracy**: AI-powered analysis reduces human error  
- 📈 **Scalability**: Handles large datasets efficiently
- 💡 **Intelligence**: Discovers patterns humans might miss
""")

# Sidebar information - only show if we have data
st.sidebar.markdown("---")
st.sidebar.markdown("### 📊 Data Overview")
# The metrics are already shown in the connection section above

st.sidebar.markdown("### 🤖 AI Functions Used")
st.sidebar.markdown("""
- ✅ AI_COMPLETE
- ✅ AI_CLASSIFY  
- ✅ AI_FILTER
- ✅ AI_AGG
- ✅ AI_SENTIMENT
""")
