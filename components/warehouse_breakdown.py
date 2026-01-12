"""
Warehouse breakdown component for Snowflake Credit Monitor
"""

import streamlit as st
import plotly.express as px
from config import CREDIT_RATE
from queries import get_per_warehouse_credits


def render_warehouse_breakdown(days_back: int, warehouses_tuple: tuple, selected_warehouses: list):
    """Render per-warehouse cost breakdown (only when multiple warehouses selected)"""
    if len(selected_warehouses) <= 1:
        return
    
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
