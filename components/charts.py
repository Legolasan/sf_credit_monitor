"""
Chart components for Snowflake Credit Monitor
"""

import streamlit as st
import pandas as pd
import plotly.express as px
from config import CREDIT_RATE
from queries import get_daily_credits, get_hourly_breakdown, get_query_breakdown


def render_daily_chart(days_back: int, warehouses_tuple: tuple, selected_warehouses: list):
    """Render the daily credit consumption chart"""
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
        return daily_df
    else:
        st.info("No data available for the selected period")
        return pd.DataFrame()


def render_hourly_chart(days_back: int, warehouses_tuple: tuple):
    """Render the hourly usage pattern chart"""
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


def render_query_breakdown(days_back: int, warehouses_tuple: tuple):
    """Render the query type breakdown section"""
    col1, col2 = st.columns(2)
    
    query_df = get_query_breakdown(days_back, warehouses_tuple)
    
    with col1:
        st.subheader("🔍 Query Type Breakdown")
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
