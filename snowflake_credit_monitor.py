"""
Snowflake Credit Monitor Dashboard
Monitors credit consumption for Fivetran ETL loads with auto-refresh
"""

import streamlit as st
import snowflake.connector
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Page config
st.set_page_config(
    page_title="Snowflake Credit Monitor",
    page_icon="❄️",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS for styling
st.markdown("""
<style>
    .metric-card {
        background: linear-gradient(135deg, #1e3a5f 0%, #0d1b2a 100%);
        border-radius: 12px;
        padding: 20px;
        margin: 10px 0;
    }
    .stMetric {
        background: linear-gradient(135deg, #1e3a5f 0%, #0d1b2a 100%);
        border-radius: 12px;
        padding: 15px;
    }
    h1 {
        color: #29b5e8;
    }
    .stDataFrame {
        border-radius: 8px;
    }
</style>
""", unsafe_allow_html=True)

# Configuration
SNOWFLAKE_CONFIG = {
    "account": os.getenv("SNOWFLAKE_ACCOUNT", "NRMGHHK-QKB80056"),
    "user": os.getenv("SNOWFLAKE_USER", "yuki"),
    "password": os.getenv("SNOWFLAKE_PASSWORD", ""),
    "warehouse": "COMPUTE_WH",
    "database": "SNOWFLAKE",
    "schema": "ACCOUNT_USAGE",
}

FIVETRAN_WAREHOUSE = "FIVETRAN_WAREHOUSE"
CREDIT_RATE = 3.00  # USD per credit
DEFAULT_DAYS = 7


@st.cache_resource
def get_connection():
    """Establish Snowflake connection"""
    try:
        conn = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
        return conn
    except Exception as e:
        st.error(f"Connection failed: {e}")
        return None


@st.cache_data(ttl=60)
def get_daily_credits(days_back: int, warehouses: tuple):
    """Get daily credit consumption for multiple warehouses"""
    conn = get_connection()
    if not conn:
        return pd.DataFrame()
    
    warehouse_list = "', '".join(warehouses)
    query = f"""
    SELECT 
        DATE_TRUNC('day', start_time)::date as load_date,
        warehouse_name,
        ROUND(SUM(credits_used), 4) as total_credits,
        ROUND(SUM(credits_used_compute), 4) as compute_credits,
        ROUND(SUM(credits_used_cloud_services), 4) as cloud_credits,
        COUNT(*) as metering_events
    FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
    WHERE warehouse_name IN ('{warehouse_list}')
      AND start_time >= DATEADD('day', -{days_back}, CURRENT_TIMESTAMP())
    GROUP BY 1, 2
    ORDER BY 1 DESC
    """
    
    try:
        cur = conn.cursor()
        cur.execute(query)
        df = pd.DataFrame(cur.fetchall(), columns=[
            'Date', 'Warehouse', 'Total Credits', 'Compute Credits', 
            'Cloud Credits', 'Events'
        ])
        # Convert Decimal to float for arithmetic operations
        for col in ['Total Credits', 'Compute Credits', 'Cloud Credits']:
            if col in df.columns:
                df[col] = df[col].astype(float)
        return df
    except Exception as e:
        st.error(f"Query failed: {e}")
        return pd.DataFrame()


@st.cache_data(ttl=60)
def get_hourly_breakdown(days_back: int, warehouses: tuple):
    """Get hourly credit breakdown for multiple warehouses"""
    conn = get_connection()
    if not conn:
        return pd.DataFrame()
    
    warehouse_list = "', '".join(warehouses)
    query = f"""
    SELECT 
        DATE_TRUNC('hour', start_time) as load_hour,
        ROUND(SUM(credits_used), 4) as credits,
        COUNT(*) as operations
    FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
    WHERE warehouse_name IN ('{warehouse_list}')
      AND start_time >= DATEADD('day', -{days_back}, CURRENT_TIMESTAMP())
    GROUP BY 1
    ORDER BY 1 DESC
    """
    
    try:
        cur = conn.cursor()
        cur.execute(query)
        df = pd.DataFrame(cur.fetchall(), columns=['Hour', 'Credits', 'Operations'])
        # Convert Decimal to float
        if 'Credits' in df.columns:
            df['Credits'] = df['Credits'].astype(float)
        return df
    except Exception as e:
        st.error(f"Query failed: {e}")
        return pd.DataFrame()


@st.cache_data(ttl=60)
def get_query_breakdown(days_back: int, warehouses: tuple):
    """Get query type breakdown for multiple warehouses"""
    conn = get_connection()
    if not conn:
        return pd.DataFrame()
    
    warehouse_list = "', '".join(warehouses)
    query = f"""
    SELECT 
        query_type,
        COUNT(*) as query_count,
        ROUND(SUM(total_elapsed_time)/1000, 2) as total_seconds,
        ROUND(SUM(bytes_scanned)/1e9, 2) as gb_scanned,
        SUM(rows_produced) as rows_produced
    FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
    WHERE warehouse_name IN ('{warehouse_list}')
      AND start_time >= DATEADD('day', -{days_back}, CURRENT_TIMESTAMP())
      AND query_type IN ('INSERT', 'COPY', 'MERGE', 'CREATE_TABLE_AS_SELECT', 'SELECT')
    GROUP BY 1
    ORDER BY 2 DESC
    """
    
    try:
        cur = conn.cursor()
        cur.execute(query)
        df = pd.DataFrame(cur.fetchall(), columns=[
            'Query Type', 'Count', 'Total Seconds', 'GB Scanned', 'Rows Produced'
        ])
        # Convert Decimal to float
        for col in ['Total Seconds', 'GB Scanned']:
            if col in df.columns:
                df[col] = df[col].astype(float)
        return df
    except Exception as e:
        st.error(f"Query failed: {e}")
        return pd.DataFrame()


@st.cache_data(ttl=60)
def get_per_warehouse_credits(days_back: int, warehouses: tuple):
    """Get credit breakdown per warehouse"""
    conn = get_connection()
    if not conn:
        return pd.DataFrame()
    
    warehouse_list = "', '".join(warehouses)
    query = f"""
    SELECT 
        warehouse_name,
        ROUND(SUM(credits_used), 4) as total_credits,
        ROUND(SUM(credits_used_compute), 4) as compute_credits,
        ROUND(SUM(credits_used_cloud_services), 4) as cloud_credits,
        COUNT(*) as metering_events
    FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
    WHERE warehouse_name IN ('{warehouse_list}')
      AND start_time >= DATEADD('day', -{days_back}, CURRENT_TIMESTAMP())
    GROUP BY 1
    ORDER BY 2 DESC
    """
    
    try:
        cur = conn.cursor()
        cur.execute(query)
        df = pd.DataFrame(cur.fetchall(), columns=[
            'Warehouse', 'Total Credits', 'Compute Credits', 'Cloud Credits', 'Events'
        ])
        # Convert Decimal to float
        for col in ['Total Credits', 'Compute Credits', 'Cloud Credits']:
            if col in df.columns:
                df[col] = df[col].astype(float)
        return df
    except Exception as e:
        st.error(f"Query failed: {e}")
        return pd.DataFrame()


@st.cache_data(ttl=60)
def get_total_credits(days_back: int, warehouses: tuple):
    """Get total credit summary for multiple warehouses"""
    conn = get_connection()
    if not conn:
        return {'credits': 0, 'cost': 0, 'queries': 0}
    
    warehouse_list = "', '".join(warehouses)
    query = f"""
    SELECT 
        ROUND(SUM(credits_used), 4) as total_credits
    FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
    WHERE warehouse_name IN ('{warehouse_list}')
      AND start_time >= DATEADD('day', -{days_back}, CURRENT_TIMESTAMP())
    """
    
    try:
        cur = conn.cursor()
        cur.execute(query)
        result = cur.fetchone()
        credits = float(result[0]) if result and result[0] else 0.0
        
        # Get query count
        query2 = f"""
        SELECT COUNT(*) 
        FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
        WHERE warehouse_name IN ('{warehouse_list}')
          AND start_time >= DATEADD('day', -{days_back}, CURRENT_TIMESTAMP())
        """
        cur.execute(query2)
        queries = cur.fetchone()[0] or 0
        
        return {
            'credits': credits,
            'cost': credits * CREDIT_RATE,
            'queries': int(queries)
        }
    except Exception as e:
        st.error(f"Query failed: {e}")
        return {'credits': 0, 'cost': 0, 'queries': 0}


@st.cache_data(ttl=30)
def get_warehouse_list():
    """Get list of currently active warehouses"""
    conn = get_connection()
    if not conn:
        return [FIVETRAN_WAREHOUSE]
    
    # Query active warehouses from SHOW WAREHOUSES (not historical data)
    query = "SHOW WAREHOUSES"
    
    try:
        cur = conn.cursor()
        cur.execute(query)
        warehouses = [row[0] for row in cur.fetchall()]
        return warehouses if warehouses else [FIVETRAN_WAREHOUSE]
    except:
        return [FIVETRAN_WAREHOUSE]


def main():
    # Header
    st.title("❄️ Snowflake Credit Monitor")
    st.markdown("*Real-time credit consumption tracking for Fivetran loads*")
    
    # Sidebar controls
    with st.sidebar:
        st.header("⚙️ Settings")
        
        # Warehouse selector (multi-select)
        warehouses = get_warehouse_list()
        default_warehouses = [FIVETRAN_WAREHOUSE] if FIVETRAN_WAREHOUSE in warehouses else warehouses[:1]
        selected_warehouses = st.multiselect(
            "Select Warehouses",
            warehouses,
            default=default_warehouses,
            help="Select one or more warehouses to analyze"
        )
        
        if not selected_warehouses:
            st.warning("Please select at least one warehouse")
            selected_warehouses = warehouses[:1] if warehouses else [FIVETRAN_WAREHOUSE]
        
        # Time range
        days_back = st.slider("Days to analyze", 1, 30, DEFAULT_DAYS)
        
        # Auto-refresh
        st.markdown("---")
        auto_refresh = st.checkbox("Auto-refresh", value=False)
        if auto_refresh:
            refresh_interval = st.selectbox(
                "Refresh interval",
                ["30 seconds", "1 minute", "5 minutes"],
                index=1
            )
            intervals = {"30 seconds": 30, "1 minute": 60, "5 minutes": 300}
            st.info(f"Refreshing every {refresh_interval}")
            
            # Trigger auto-refresh
            import time
            time.sleep(0.1)
            st.rerun() if auto_refresh else None
        
        # Manual refresh
        if st.button("🔄 Refresh Data", use_container_width=True):
            st.cache_data.clear()
            st.rerun()
        
        if st.button("🔃 Reload Warehouses", use_container_width=True):
            get_warehouse_list.clear()
            st.rerun()
        
        # Credit rate
        st.markdown("---")
        st.markdown(f"**Credit Rate:** ${CREDIT_RATE:.2f}/credit")
        
        # Connection status
        st.markdown("---")
        conn = get_connection()
        if conn:
            st.success("✓ Connected to Snowflake")
        else:
            st.error("✗ Not connected")
    
    # Main content
    if not get_connection():
        st.warning("⚠️ Please configure your Snowflake credentials in the .env file")
        st.code("""
SNOWFLAKE_ACCOUNT=NRMGHHK-QKB80056
SNOWFLAKE_USER=yuki
SNOWFLAKE_PASSWORD=your_password_here
        """)
        return
    
    # Convert list to tuple for caching (lists aren't hashable)
    warehouses_tuple = tuple(selected_warehouses)
    
    # Summary metrics
    summary = get_total_credits(days_back, warehouses_tuple)
    
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric(
            label="Total Credits",
            value=f"{summary['credits']:.4f}",
            delta=None
        )
    with col2:
        st.metric(
            label="Estimated Cost",
            value=f"${summary['cost']:.2f}",
            delta=None
        )
    with col3:
        st.metric(
            label="Total Queries",
            value=f"{summary['queries']:,}",
            delta=None
        )
    with col4:
        avg_cost_per_query = summary['cost'] / summary['queries'] if summary['queries'] > 0 else 0
        st.metric(
            label="Avg Cost/Query",
            value=f"${avg_cost_per_query:.4f}",
            delta=None
        )
    
    st.markdown("---")
    
    # Per-warehouse breakdown (when multiple warehouses selected)
    if len(selected_warehouses) > 1:
        st.subheader("🏭 Cost Breakdown by Warehouse")
        warehouse_df = get_per_warehouse_credits(days_back, warehouses_tuple)
        
        if not warehouse_df.empty:
            # Calculate cost and percentage
            warehouse_df['Cost ($)'] = warehouse_df['Total Credits'] * CREDIT_RATE
            total_credits = warehouse_df['Total Credits'].sum()
            warehouse_df['% of Total'] = (warehouse_df['Total Credits'] / total_credits * 100).round(1)
            
            col1, col2 = st.columns([2, 1])
            
            with col1:
                # Display table
                display_df = warehouse_df[['Warehouse', 'Total Credits', 'Cost ($)', '% of Total', 'Events']].copy()
                display_df['Cost ($)'] = display_df['Cost ($)'].apply(lambda x: f"${x:.2f}")
                display_df['% of Total'] = display_df['% of Total'].apply(lambda x: f"{x}%")
                st.dataframe(display_df, use_container_width=True, hide_index=True)
            
            with col2:
                # Pie chart
                fig = px.pie(
                    warehouse_df,
                    values='Total Credits',
                    names='Warehouse',
                    title='Credit Distribution',
                    color_discrete_sequence=px.colors.sequential.Blues_r
                )
                fig.update_layout(height=250, showlegend=True)
                st.plotly_chart(fig, use_container_width=True)
        
        st.markdown("---")
    
    # Charts row
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("📊 Daily Credit Consumption")
        daily_df = get_daily_credits(days_back, warehouses_tuple)
        
        if not daily_df.empty:
            daily_df['Cost ($)'] = daily_df['Total Credits'] * CREDIT_RATE
            
            # If multiple warehouses, show stacked bar by warehouse
            if len(selected_warehouses) > 1:
                fig = px.bar(
                    daily_df,
                    x='Date',
                    y='Total Credits',
                    color='Warehouse',
                    hover_data=['Cost ($)', 'Events'],
                    barmode='stack'
                )
            else:
                fig = px.bar(
                    daily_df,
                    x='Date',
                    y='Total Credits',
                    color='Total Credits',
                    color_continuous_scale='Blues',
                    hover_data=['Cost ($)', 'Events']
                )
            fig.update_layout(
                xaxis_title="Date",
                yaxis_title="Credits",
                height=350
            )
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No data available for the selected period")
    
    with col2:
        st.subheader("⏰ Hourly Usage Pattern")
        hourly_df = get_hourly_breakdown(days_back, warehouses_tuple)
        
        if not hourly_df.empty:
            # Extract hour of day for pattern analysis
            hourly_df['Hour of Day'] = pd.to_datetime(hourly_df['Hour']).dt.hour
            hourly_agg = hourly_df.groupby('Hour of Day')['Credits'].sum().reset_index()
            
            fig = px.bar(
                hourly_agg,
                x='Hour of Day',
                y='Credits',
                color='Credits',
                color_continuous_scale='Oranges'
            )
            fig.update_layout(
                xaxis_title="Hour of Day (UTC)",
                yaxis_title="Total Credits",
                showlegend=False,
                height=350
            )
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No hourly data available")
    
    st.markdown("---")
    
    # Query breakdown
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("🔍 Query Type Breakdown")
        query_df = get_query_breakdown(days_back, warehouses_tuple)
        
        if not query_df.empty:
            fig = px.pie(
                query_df,
                values='Count',
                names='Query Type',
                color_discrete_sequence=px.colors.sequential.Blues_r
            )
            fig.update_layout(height=300)
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No query data available")
    
    with col2:
        st.subheader("📋 Query Statistics")
        if not query_df.empty:
            st.dataframe(
                query_df,
                use_container_width=True,
                hide_index=True
            )
        else:
            st.info("No query data available")
    
    st.markdown("---")
    
    # Detailed daily table
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
    
    # Footer
    st.markdown("---")
    warehouse_names = ", ".join(selected_warehouses)
    st.caption(f"Last updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} | Warehouses: {warehouse_names} | Period: Last {days_back} days")


if __name__ == "__main__":
    main()
