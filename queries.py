"""
Data fetching functions for Snowflake Credit Monitor
All query functions are cached for performance
"""

import streamlit as st
import pandas as pd
from database import get_connection
from config import CACHE_TTL, CREDIT_RATE, FIVETRAN_WAREHOUSE, WAREHOUSE_SIZE_MULTIPLIERS


@st.cache_data(ttl=CACHE_TTL)
def get_daily_credits(days_back: int, warehouses: tuple) -> pd.DataFrame:
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


@st.cache_data(ttl=CACHE_TTL)
def get_hourly_breakdown(days_back: int, warehouses: tuple) -> pd.DataFrame:
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
        if 'Credits' in df.columns:
            df['Credits'] = df['Credits'].astype(float)
        return df
    except Exception as e:
        st.error(f"Query failed: {e}")
        return pd.DataFrame()


@st.cache_data(ttl=CACHE_TTL)
def get_query_breakdown(days_back: int, warehouses: tuple) -> pd.DataFrame:
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
        for col in ['Total Seconds', 'GB Scanned']:
            if col in df.columns:
                df[col] = df[col].astype(float)
        return df
    except Exception as e:
        st.error(f"Query failed: {e}")
        return pd.DataFrame()


@st.cache_data(ttl=CACHE_TTL)
def get_per_warehouse_credits(days_back: int, warehouses: tuple) -> pd.DataFrame:
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
        for col in ['Total Credits', 'Compute Credits', 'Cloud Credits']:
            if col in df.columns:
                df[col] = df[col].astype(float)
        return df
    except Exception as e:
        st.error(f"Query failed: {e}")
        return pd.DataFrame()


@st.cache_data(ttl=CACHE_TTL)
def get_total_credits(days_back: int, warehouses: tuple) -> dict:
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


@st.cache_data(ttl=CACHE_TTL)
def get_warehouse_efficiency(days_back: int, warehouses: tuple) -> pd.DataFrame:
    """Get warehouse efficiency metrics"""
    conn = get_connection()
    if not conn:
        return pd.DataFrame()
    
    warehouse_list = "', '".join(warehouses)
    
    query = f"""
    WITH load_stats AS (
        SELECT 
            warehouse_name,
            AVG(avg_running) as avg_running_queries,
            AVG(avg_queued_load) as avg_queue_load,
            AVG(avg_queued_provisioning) as avg_queue_provisioning,
            COUNT(*) as load_samples
        FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_LOAD_HISTORY
        WHERE warehouse_name IN ('{warehouse_list}')
          AND start_time >= DATEADD('day', -{days_back}, CURRENT_TIMESTAMP())
        GROUP BY 1
    ),
    metering_stats AS (
        SELECT 
            warehouse_name,
            SUM(credits_used) as total_credits,
            COUNT(DISTINCT DATE_TRUNC('hour', start_time)) as active_hours
        FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
        WHERE warehouse_name IN ('{warehouse_list}')
          AND start_time >= DATEADD('day', -{days_back}, CURRENT_TIMESTAMP())
        GROUP BY 1
    ),
    query_stats AS (
        SELECT 
            warehouse_name,
            COUNT(*) as total_queries,
            AVG(queued_overload_time)/1000 as avg_queue_time_sec,
            AVG(execution_time)/1000 as avg_exec_time_sec
        FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
        WHERE warehouse_name IN ('{warehouse_list}')
          AND start_time >= DATEADD('day', -{days_back}, CURRENT_TIMESTAMP())
        GROUP BY 1
    )
    SELECT 
        COALESCE(l.warehouse_name, m.warehouse_name, q.warehouse_name) as warehouse_name,
        COALESCE(l.avg_running_queries, 0) as avg_running,
        COALESCE(l.avg_queue_load, 0) as avg_queue_load,
        COALESCE(m.total_credits, 0) as total_credits,
        COALESCE(m.active_hours, 0) as active_hours,
        COALESCE(q.total_queries, 0) as total_queries,
        COALESCE(q.avg_queue_time_sec, 0) as avg_queue_time_sec,
        COALESCE(q.avg_exec_time_sec, 0) as avg_exec_time_sec
    FROM load_stats l
    FULL OUTER JOIN metering_stats m ON l.warehouse_name = m.warehouse_name
    FULL OUTER JOIN query_stats q ON COALESCE(l.warehouse_name, m.warehouse_name) = q.warehouse_name
    ORDER BY total_credits DESC
    """
    
    try:
        cur = conn.cursor()
        cur.execute(query)
        df = pd.DataFrame(cur.fetchall(), columns=[
            'Warehouse', 'Avg Running Queries', 'Avg Queue Load', 'Total Credits',
            'Active Hours', 'Total Queries', 'Avg Queue Time (s)', 'Avg Exec Time (s)'
        ])
        for col in df.columns:
            if col != 'Warehouse':
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
        return df
    except Exception as e:
        st.error(f"Efficiency query failed: {e}")
        return pd.DataFrame()


@st.cache_data(ttl=CACHE_TTL)
def get_expensive_queries(days_back: int, warehouses: tuple, limit: int = 10) -> pd.DataFrame:
    """Get most expensive queries by execution time"""
    conn = get_connection()
    if not conn:
        return pd.DataFrame()
    
    warehouse_list = "', '".join(warehouses)
    
    query = f"""
    SELECT 
        query_id,
        user_name,
        warehouse_name,
        warehouse_size,
        query_type,
        ROUND(execution_time/1000, 2) as exec_seconds,
        ROUND(total_elapsed_time/1000, 2) as total_seconds,
        ROUND(bytes_scanned/1e9, 3) as gb_scanned,
        rows_produced,
        ROUND(credits_used_cloud_services, 6) as cloud_credits,
        start_time,
        SUBSTR(query_text, 1, 150) as query_preview
    FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
    WHERE warehouse_name IN ('{warehouse_list}')
      AND start_time >= DATEADD('day', -{days_back}, CURRENT_TIMESTAMP())
      AND execution_time > 0
    ORDER BY execution_time DESC
    LIMIT {limit}
    """
    
    try:
        cur = conn.cursor()
        cur.execute(query)
        df = pd.DataFrame(cur.fetchall(), columns=[
            'Query ID', 'User', 'Warehouse', 'Size', 'Type', 'Exec (s)', 
            'Total (s)', 'GB Scanned', 'Rows', 'Cloud Credits', 'Start Time', 'Query Preview'
        ])
        for col in ['Exec (s)', 'Total (s)', 'GB Scanned', 'Cloud Credits']:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
        
        # Estimate credits based on warehouse size and time
        df['Est. Credits'] = df.apply(
            lambda row: round((row['Exec (s)'] / 3600) * WAREHOUSE_SIZE_MULTIPLIERS.get(row['Size'], 1), 6),
            axis=1
        )
        df['Est. Cost ($)'] = df['Est. Credits'] * CREDIT_RATE
        
        return df
    except Exception as e:
        st.error(f"Expensive queries failed: {e}")
        return pd.DataFrame()


@st.cache_data(ttl=30)
def get_warehouse_list() -> list:
    """Get list of currently active warehouses"""
    conn = get_connection()
    if not conn:
        return [FIVETRAN_WAREHOUSE]
    
    query = "SHOW WAREHOUSES"
    
    try:
        cur = conn.cursor()
        cur.execute(query)
        warehouses = [row[0] for row in cur.fetchall()]
        return warehouses if warehouses else [FIVETRAN_WAREHOUSE]
    except:
        return [FIVETRAN_WAREHOUSE]
