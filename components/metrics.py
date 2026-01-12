"""
Summary metrics component for Snowflake Credit Monitor
"""

import streamlit as st
from queries import get_total_credits


def render_metrics(days_back: int, warehouses_tuple: tuple):
    """
    Render the summary metrics row.
    
    Args:
        days_back: Number of days to analyze
        warehouses_tuple: Tuple of warehouse names
    """
    with st.spinner("Loading summary metrics..."):
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
