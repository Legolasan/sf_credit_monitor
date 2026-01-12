"""
Expensive queries component for Snowflake Credit Monitor
"""

import streamlit as st
import pandas as pd
import plotly.express as px
from queries import get_expensive_queries


def render_expensive_queries(days_back: int, warehouses_tuple: tuple):
    """Render the expensive queries section (lazy loaded)"""
    st.subheader("💰 Most Expensive Queries")
    
    load_expensive = st.checkbox(
        "Load expensive queries analysis", 
        value=False,
        help="This query may take longer to load"
    )
    
    if load_expensive:
        col1, col2 = st.columns([3, 1])
        with col2:
            query_limit = st.selectbox("Show top", [10, 15, 25, 50], index=0)
        
        with st.spinner("Analyzing expensive queries..."):
            expensive_df = get_expensive_queries(days_back, warehouses_tuple, query_limit)
        
        if not expensive_df.empty:
            # Summary stats
            total_exec_time = expensive_df['Exec (s)'].sum()
            total_est_cost = expensive_df['Est. Cost ($)'].sum()
            
            col1, col2, col3 = st.columns(3)
            with col1:
                st.metric("Total Exec Time (top queries)", f"{total_exec_time:,.0f}s")
            with col2:
                st.metric("Est. Cost (top queries)", f"${total_est_cost:.2f}")
            with col3:
                avg_gb = expensive_df['GB Scanned'].mean()
                st.metric("Avg Data Scanned", f"{avg_gb:.2f} GB")
            
            # Query type distribution for expensive queries
            col1, col2 = st.columns([2, 1])
            
            with col1:
                # Main table
                display_cols = ['User', 'Warehouse', 'Type', 'Exec (s)', 'GB Scanned', 'Est. Cost ($)', 'Start Time']
                display_df = expensive_df[display_cols].copy()
                display_df['Est. Cost ($)'] = display_df['Est. Cost ($)'].apply(lambda x: f"${x:.4f}")
                display_df['Start Time'] = pd.to_datetime(display_df['Start Time']).dt.strftime('%Y-%m-%d %H:%M')
                
                st.dataframe(display_df, use_container_width=True, hide_index=True)
            
            with col2:
                # Query type pie for expensive queries
                type_counts = expensive_df.groupby('Type')['Est. Cost ($)'].sum().reset_index()
                fig = px.pie(
                    type_counts,
                    values='Est. Cost ($)',
                    names='Type',
                    title='Cost by Query Type',
                    color_discrete_sequence=px.colors.sequential.Reds_r
                )
                fig.update_layout(height=300)
                st.plotly_chart(fig, use_container_width=True)
            
            # Expandable query details
            with st.expander("🔍 View Query Details"):
                for idx, row in expensive_df.head(5).iterrows():
                    st.markdown(f"**Query {idx+1}** - {row['Type']} by `{row['User']}` ({row['Exec (s)']}s)")
                    st.code(row['Query Preview'], language='sql')
                    st.markdown("---")
        else:
            st.info("No query data available")
    else:
        st.caption("👆 Check the box above to load expensive queries analysis")
