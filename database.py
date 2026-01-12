"""
Database connection handling for Snowflake
"""

import streamlit as st
import snowflake.connector


def get_current_config():
    """
    Get the current Snowflake configuration.
    Uses connection_manager if available, falls back to config.
    """
    try:
        from connection_manager import get_snowflake_config
        return get_snowflake_config()
    except ImportError:
        from config import SNOWFLAKE_CONFIG
        return SNOWFLAKE_CONFIG


@st.cache_resource
def get_connection():
    """
    Establish and cache Snowflake connection.
    Uses st.cache_resource to maintain connection across reruns.
    """
    config = get_current_config()
    
    # Validate config has required fields
    if not config.get("account") or not config.get("user"):
        return None
    
    try:
        conn = snowflake.connector.connect(**config)
        return conn
    except Exception as e:
        st.error(f"Connection failed: {e}")
        return None


def clear_connection_cache():
    """
    Clear the cached connection.
    Call this when switching connections.
    """
    get_connection.clear()


def test_connection():
    """Test if connection is valid"""
    conn = get_connection()
    if conn:
        try:
            cur = conn.cursor()
            cur.execute("SELECT 1")
            return True
        except:
            return False
    return False


def execute_query(query: str):
    """
    Execute a query and return results as list of tuples.
    
    Args:
        query: SQL query string
        
    Returns:
        List of tuples containing query results, or empty list on error
    """
    conn = get_connection()
    if not conn:
        return []
    
    try:
        cur = conn.cursor()
        cur.execute(query)
        return cur.fetchall()
    except Exception as e:
        st.error(f"Query failed: {e}")
        return []
