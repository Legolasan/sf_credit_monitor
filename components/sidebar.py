"""
Sidebar component for Snowflake Credit Monitor
"""

import streamlit as st
from config import FIVETRAN_WAREHOUSE, DEFAULT_DAYS, CREDIT_RATE
from database import get_connection, clear_connection_cache
from queries import get_warehouse_list
from connection_manager import (
    load_connections,
    save_connection,
    delete_connection,
    get_connection_names,
    get_active_connection,
    set_active_connection,
    test_connection,
    get_snowflake_config
)


def render_connection_manager():
    """
    Render the connection management section in the sidebar.
    """
    # Use checkbox toggle instead of expander for better compatibility
    show_connections = st.checkbox("🔌 Manage Connections", value=False, key="show_connections_toggle")
    
    if show_connections:
        # Get available connections
        connection_names = get_connection_names()
        active_name, _ = get_active_connection()
        
        # Connection selector
        if connection_names:
            # Find default index
            default_idx = 0
            if active_name in connection_names:
                default_idx = connection_names.index(active_name)
            
            selected = st.selectbox(
                "Active Connection",
                connection_names,
                index=default_idx,
                key="connection_selector"
            )
            
            # Switch connection if changed
            if selected != active_name and selected != "Environment (.env)":
                if set_active_connection(selected):
                    clear_connection_cache()
                    st.success(f"Switched to {selected}")
                    st.rerun()
        else:
            st.info("No saved connections. Add one below or configure .env file.")
        
        st.markdown("---")
        
        # Add new connection section
        st.markdown("**➕ Add New Connection**")
        
        conn_name = st.text_input(
            "Connection Name",
            placeholder="e.g., Production, Development",
            key="new_conn_name"
        )
        account_id = st.text_input(
            "Account ID",
            placeholder="e.g., abc12345.us-east-1",
            key="new_account_id"
        )
        username = st.text_input(
            "Username",
            placeholder="your_username",
            key="new_username"
        )
        password = st.text_input(
            "Password",
            type="password",
            placeholder="your_password",
            key="new_password"
        )
        warehouse = st.text_input(
            "Default Warehouse",
            value="COMPUTE_WH",
            placeholder="COMPUTE_WH",
            key="new_warehouse"
        )
        
        new_config = {
            "account": account_id,
            "user": username,
            "password": password,
            "warehouse": warehouse
        }
        
        col1, col2 = st.columns(2)
        with col1:
            if st.button("🔍 Test", use_container_width=True, key="test_conn_btn"):
                if account_id and username and password:
                    with st.spinner("Testing connection..."):
                        success, message = test_connection(new_config)
                    if success:
                        st.success(f"✓ {message}")
                    else:
                        st.error(f"✗ {message}")
                else:
                    st.warning("Please fill in Account ID, Username, and Password")
        
        with col2:
            if st.button("💾 Save", use_container_width=True, key="save_conn_btn"):
                if conn_name and account_id and username and password:
                    if save_connection(conn_name, new_config):
                        clear_connection_cache()
                        st.success(f"✓ Connection '{conn_name}' saved!")
                        st.rerun()
                    else:
                        st.error("Failed to save connection")
                else:
                    st.warning("Please fill in all required fields")
        
        # Delete connection
        st.markdown("---")
        if connection_names:
            # Filter out environment option for deletion
            deletable = [c for c in connection_names if c != "Environment (.env)"]
            if deletable:
                st.markdown("**🗑️ Delete Connection**")
                to_delete = st.selectbox(
                    "Select to delete",
                    deletable,
                    key="delete_selector",
                    label_visibility="collapsed"
                )
                if st.button("Delete Connection", type="secondary", use_container_width=True):
                    if delete_connection(to_delete):
                        clear_connection_cache()
                        st.success(f"Deleted '{to_delete}'")
                        st.rerun()
                    else:
                        st.error("Failed to delete connection")
        
        # Security warning
        st.markdown("---")
        st.caption("⚠️ Credentials are stored locally in connections.json")


def render_sidebar():
    """
    Render the sidebar with all controls.
    
    Returns:
        tuple: (selected_warehouses, days_back) - User selections
    """
    with st.sidebar:
        st.header("⚙️ Settings")
        
        # Connection manager section
        render_connection_manager()
        
        st.markdown("---")
        
        # Check connection status
        conn = get_connection()
        if not conn:
            st.error("✗ Not connected to Snowflake")
            st.info("Add a connection above or configure your .env file")
            return [], DEFAULT_DAYS
        
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
        active_name, _ = get_active_connection()
        if conn:
            st.success(f"✓ Connected: {active_name or 'Unknown'}")
        else:
            st.error("✗ Not connected")
    
    return selected_warehouses, days_back
