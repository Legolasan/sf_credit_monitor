"""
Warehouse efficiency component for Snowflake Credit Monitor
"""

import streamlit as st
import plotly.express as px
from queries import get_warehouse_efficiency


def render_efficiency_section(days_back: int, warehouses_tuple: tuple):
    """Render the warehouse efficiency section with metrics and recommendations"""
    st.subheader("⚡ Warehouse Efficiency")
    efficiency_df = get_warehouse_efficiency(days_back, warehouses_tuple)
    
    if not efficiency_df.empty:
        col1, col2 = st.columns([2, 1])
        
        with col1:
            # Efficiency metrics table
            eff_display = efficiency_df.copy()
            eff_display['Credits/Query'] = (eff_display['Total Credits'] / eff_display['Total Queries'].replace(0, 1)).round(6)
            eff_display['Queries/Hour'] = (eff_display['Total Queries'] / eff_display['Active Hours'].replace(0, 1)).round(1)
            
            # Calculate efficiency score (higher is better: more queries per credit)
            max_qpc = eff_display['Credits/Query'].max()
            if max_qpc > 0:
                eff_display['Efficiency'] = ((1 - eff_display['Credits/Query'] / max_qpc) * 100).round(1).apply(lambda x: f"{max(0, x):.0f}%")
            else:
                eff_display['Efficiency'] = "N/A"
            
            st.dataframe(
                eff_display[['Warehouse', 'Total Queries', 'Active Hours', 'Total Credits', 'Credits/Query', 'Avg Queue Time (s)', 'Efficiency']],
                use_container_width=True,
                hide_index=True
            )
        
        with col2:
            # Recommendations
            st.markdown("**💡 Recommendations**")
            for _, row in efficiency_df.iterrows():
                wh_name = row['Warehouse']
                queue_time = row['Avg Queue Time (s)']
                avg_running = row['Avg Running Queries']
                
                if queue_time > 5:
                    st.warning(f"**{wh_name}**: High queue time ({queue_time:.1f}s avg). Consider scaling up.")
                elif avg_running < 0.1 and row['Total Credits'] > 0:
                    st.info(f"**{wh_name}**: Low utilization. Consider reducing auto-suspend timeout.")
                elif row['Total Queries'] > 0:
                    st.success(f"**{wh_name}**: Running efficiently ✓")
        
        # Queue time visualization
        if efficiency_df['Avg Queue Time (s)'].sum() > 0:
            fig = px.bar(
                efficiency_df,
                x='Warehouse',
                y='Avg Queue Time (s)',
                color='Avg Queue Time (s)',
                color_continuous_scale='Reds',
                title='Average Queue Time by Warehouse'
            )
            fig.update_layout(height=250, showlegend=False)
            st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No efficiency data available")
