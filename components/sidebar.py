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


@st.dialog("🔌 Manage Connections", width="large")
def connection_management_dialog():
    """
    Modal dialog for managing Snowflake connections.
    """
    # Get current connections
    connection_names = get_connection_names()
    active_name, _ = get_active_connection()
    
    # Tabs for better organization
    tab1, tab2 = st.tabs(["📋 Saved Connections", "➕ Add New"])
    
    with tab1:
        st.markdown("### Your Connections")
        
        if not connection_names:
            st.info("No connections saved yet. Add one in the 'Add New' tab.")
        else:
            # List all connections
            for conn_name in connection_names:
                is_active = conn_name == active_name
                is_env = conn_name == "Environment (.env)"
                
                col1, col2, col3 = st.columns([3, 1, 1])
                
                with col1:
                    if is_active:
                        st.markdown(f"**🟢 {conn_name}** *(active)*")
                    else:
                        st.markdown(f"⚪ {conn_name}")
                
                with col2:
                    if not is_active:
                        if st.button("Activate", key=f"activate_{conn_name}", use_container_width=True):
                            if is_env or set_active_connection(conn_name):
                                clear_connection_cache()
                                st.success(f"Switched to {conn_name}")
                                st.rerun()
                
                with col3:
                    if not is_env:
                        if st.button("🗑️", key=f"delete_{conn_name}", help=f"Delete {conn_name}"):
                            if delete_connection(conn_name):
                                clear_connection_cache()
                                st.success(f"Deleted '{conn_name}'")
                                st.rerun()
                            else:
                                st.error("Failed to delete")
                
                st.divider()
    
    with tab2:
        st.markdown("### Add New Connection")
        
        conn_name = st.text_input(
            "Connection Name",
            placeholder="e.g., Production, Development",
            key="modal_conn_name"
        )
        
        col1, col2 = st.columns(2)
        with col1:
            account_id = st.text_input(
                "Account ID",
                placeholder="e.g., abc12345.us-east-1",
                key="modal_account_id"
            )
        with col2:
            warehouse = st.text_input(
                "Default Warehouse",
                value="COMPUTE_WH",
                key="modal_warehouse"
            )
        
        username = st.text_input(
            "Username",
            placeholder="your_username",
            key="modal_username"
        )
        
        password = st.text_input(
            "Password",
            type="password",
            placeholder="your_password",
            key="modal_password"
        )
        
        new_config = {
            "account": account_id,
            "user": username,
            "password": password,
            "warehouse": warehouse
        }
        
        st.markdown("")  # Spacing
        
        col1, col2, col3 = st.columns([1, 1, 1])
        
        with col1:
            if st.button("🔍 Test Connection", use_container_width=True):
                if account_id and username and password:
                    with st.spinner("Testing..."):
                        success, message = test_connection(new_config)
                    if success:
                        st.success(f"✓ {message}")
                    else:
                        st.error(f"✗ {message}")
                else:
                    st.warning("Fill in all required fields")
        
        with col2:
            if st.button("💾 Save Connection", use_container_width=True, type="primary"):
                if conn_name and account_id and username and password:
                    if save_connection(conn_name, new_config):
                        clear_connection_cache()
                        st.success(f"✓ '{conn_name}' saved!")
                        st.rerun()
                    else:
                        st.error("Failed to save")
                else:
                    st.warning("Fill in all required fields")
        
        with col3:
            if st.button("Cancel", use_container_width=True):
                st.rerun()
    
    # Footer
    st.markdown("---")
    st.caption("⚠️ Credentials are stored locally in `connections.json`")


def render_sidebar():
    """
    Render the sidebar with all controls.
    
    Returns:
        tuple: (selected_warehouses, days_back) - User selections
    """
    with st.sidebar:
        st.header("⚙️ Settings")
        
        # Connection section - clean and compact
        connection_names = get_connection_names()
        active_name, _ = get_active_connection()
        
        # Quick switch dropdown
        if connection_names:
            default_idx = 0
            if active_name in connection_names:
                default_idx = connection_names.index(active_name)
            
            selected = st.selectbox(
                "🔌 Active Connection",
                connection_names,
                index=default_idx,
                key="quick_connection_switch"
            )
            
            # Switch connection if changed
            if selected != active_name and selected != "Environment (.env)":
                if set_active_connection(selected):
                    clear_connection_cache()
                    st.rerun()
        
        # Button to open modal
        if st.button("⚙️ Manage Connections", use_container_width=True):
            connection_management_dialog()
        
        st.markdown("---")
        
        # Check connection status
        conn = get_connection()
        if not conn:
            st.error("✗ Not connected to Snowflake")
            st.info("Click 'Manage Connections' to add a connection")
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
    
    return selected_warehouses, days_back
