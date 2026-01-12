"""
Database connection handling for Snowflake
"""

import streamlit as st
import snowflake.connector
from config import SNOWFLAKE_CONFIG


@st.cache_resource
def get_connection():
    """
    Establish and cache Snowflake connection.
    Uses st.cache_resource to maintain connection across reruns.
    """
    try:
        conn = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
        return conn
    except Exception as e:
        st.error(f"Connection failed: {e}")
        return None


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
