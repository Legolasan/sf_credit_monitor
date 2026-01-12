"""
Sidebar component for Snowflake Credit Monitor
"""

import streamlit as st
from config import FIVETRAN_WAREHOUSE, DEFAULT_DAYS, CREDIT_RATE
from database import get_connection
from queries import get_warehouse_list


def render_sidebar():
    """
    Render the sidebar with all controls.
    
    Returns:
        tuple: (selected_warehouses, days_back) - User selections
    """
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
            st.info(f"Refreshing every {refresh_interval}")
            
            import time
            time.sleep(0.1)
            st.rerun()
        
        # Manual refresh buttons
        if st.button("🔄 Refresh Data", use_container_width=True):
            st.cache_data.clear()
            st.rerun()
        
        if st.button("🔃 Reload Warehouses", use_container_width=True):
            get_warehouse_list.clear()
            st.rerun()
        
        # Credit rate info
        st.markdown("---")
        st.markdown(f"**Credit Rate:** ${CREDIT_RATE:.2f}/credit")
        
        # Connection status
        st.markdown("---")
        conn = get_connection()
        if conn:
            st.success("✓ Connected to Snowflake")
        else:
            st.error("✗ Not connected")
    
    return selected_warehouses, days_back
