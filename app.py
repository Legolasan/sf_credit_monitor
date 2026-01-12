"""
Snowflake Credit Monitor - Main Application
A Streamlit dashboard for monitoring Snowflake credit consumption

Run with: streamlit run app.py
"""

import streamlit as st
from datetime import datetime

# Configuration and styling
from config import PAGE_CONFIG, CUSTOM_CSS, CREDIT_RATE
from database import get_connection

# UI Components
from components.sidebar import render_sidebar
from components.metrics import render_metrics
from components.charts import render_daily_chart, render_hourly_chart, render_query_breakdown
from components.efficiency import render_efficiency_section
from components.expensive_queries import render_expensive_queries
from components.warehouse_breakdown import render_warehouse_breakdown


def main():
    """Main application entry point"""
    
    # Page configuration
    st.set_page_config(**PAGE_CONFIG)
    
    # Apply custom CSS
    st.markdown(CUSTOM_CSS, unsafe_allow_html=True)
    
    # Header
    st.title("❄️ Snowflake Credit Monitor")
    st.markdown("*Real-time credit consumption tracking for Fivetran loads*")
    
    # Render sidebar and get user selections
    selected_warehouses, days_back = render_sidebar()
    
    # Check connection
    if not get_connection():
        st.warning("⚠️ Please configure your Snowflake credentials in the .env file")
        st.code("""
SNOWFLAKE_ACCOUNT=your_account_identifier
SNOWFLAKE_USER=your_username
SNOWFLAKE_PASSWORD=your_password
SNOWFLAKE_WAREHOUSE=COMPUTE_WH
        """)
        return
    
    # Convert list to tuple for caching (lists aren't hashable)
    warehouses_tuple = tuple(selected_warehouses)
    
    # ===== Summary Metrics =====
    render_metrics(days_back, warehouses_tuple)
    
    st.markdown("---")
    
    # ===== Warehouse Breakdown (if multiple selected) =====
    render_warehouse_breakdown(days_back, warehouses_tuple, selected_warehouses)
    
    # ===== Daily and Hourly Charts =====
    col1, col2 = st.columns(2)
    
    with col1:
        daily_df = render_daily_chart(days_back, warehouses_tuple, selected_warehouses)
    
    with col2:
        render_hourly_chart(days_back, warehouses_tuple)
    
    st.markdown("---")
    
    # ===== Query Breakdown =====
    render_query_breakdown(days_back, warehouses_tuple)
    
    st.markdown("---")
    
    # ===== Warehouse Efficiency =====
    render_efficiency_section(days_back, warehouses_tuple)
    
    st.markdown("---")
    
    # ===== Expensive Queries (Lazy Loaded) =====
    render_expensive_queries(days_back, warehouses_tuple)
    
    st.markdown("---")
    
    # ===== Daily Breakdown Table =====
    st.subheader("📅 Daily Breakdown")
    if not daily_df.empty:
        daily_df['Cost ($)'] = daily_df['Total Credits'].apply(lambda x: f"${x * CREDIT_RATE:.2f}")
        st.dataframe(
            daily_df[['Date', 'Total Credits', 'Compute Credits', 'Cloud Credits', 'Cost ($)', 'Events']],
            use_container_width=True,
            hide_index=True
        )
    else:
        st.info("No daily data available")
    
    # ===== Footer =====
    st.markdown("---")
    warehouse_names = ", ".join(selected_warehouses)
    st.caption(f"Last updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} | Warehouses: {warehouse_names} | Period: Last {days_back} days")


if __name__ == "__main__":
    main()
