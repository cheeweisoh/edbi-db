import streamlit as st
import pandas as pd
import plotly.graph_objects as go

# Set page config
st.set_page_config(layout="wide", page_title="Court Cases Workload Dashboard")

# Custom CSS for styling
st.markdown("""
    <style>
    .main-header {
        background-color: #802a3a;
        color: white;
        padding: 10px;
        text-align: center;
        font-size: 24px;
        font-weight: bold;
        margin-bottom: 20px;
        border-radius: 5px;
    }
    .info-box {
        background-color: #e0e0e0;
        padding: 8px 15px;
        border-radius: 5px;
        font-size: 14px;
        display: flex;
        align-items: center;
        font-weight: bold;
    }
    .chart-title {
        background-color: #1a1a1a;
        color: white;
        padding: 5px;
        text-align: center;
        font-size: 14px;
        font-weight: bold;
        margin-bottom: 10px;
    }
    .cluster-label {
        font-weight: bold;
        display: flex;
        align-items: center;
        justify-content: center;
        height: 100%;
        font-size: 16px;
    }
    .stPlotlyChart {
        margin-bottom: -20px;
    }
    </style>
    """, unsafe_allow_html=True)

# --- HEADER ---
st.markdown('<div class="main-header">COURT CASES WORKLOAD DISTRIBUTION OVERVIEW</div>', unsafe_allow_html=True)

# --- TOP FILTERS & INFO ---
top_col1, top_col2, top_col3 = st.columns([1, 1, 2.5])
with top_col1:
    st.selectbox("CLUSTER FILTER", ["All Clusters", "Cluster 1", "Cluster 2"], label_visibility="collapsed")
with top_col2:
    st.selectbox("DATE FILTER", ["All Dates", "Last 12 Months", "YTD"], label_visibility="collapsed")
with top_col3:
    st.markdown('<div class="info-box">ⓘ Click into the charts to see Officer Breakdown</div>', unsafe_allow_html=True)

st.write("")

# --- MOCK DATA ---
clusters = ["Cluster 1", "Cluster 2", "Cluster 3", "Cluster 4", "Cluster 5"]

# Data for Number of Cases per Officer
cases_open = [76, 49, 35, 27, 12]
cases_closed = [83, 135, 113, 109, 80]

# Data for Processing Time
time_open = [20, 24, 19, 20, 20]
time_closed = [77, 60, 62, 56, 85]

# Data for Complexity
complex_simple = [129, 142, 120, 111, 75]
complex_hard = [30, 42, 28, 25, 17]

# Colors
color_open = '#a34e5d'
color_closed = '#a6a6a6'
color_simple = '#e9a0dc'
color_hard = '#702269'

# --- CHART HEADERS ---
_, hcol1, hcol2, hcol3 = st.columns([0.5, 2, 2, 2])
with hcol1:
    st.markdown('<div class="chart-title">Number of Cases per Officer</div>', unsafe_allow_html=True)
with hcol2:
    st.markdown('<div class="chart-title">Average Case Processing Time</div>', unsafe_allow_html=True)
with hcol3:
    st.markdown('<div class="chart-title">Distribution of Case Complexity</div>', unsafe_allow_html=True)

# --- GRID ROWS ---
for i in range(len(clusters)):
    # Create row for each cluster
    row_container = st.container()
    with row_container:
        lcol, ccol1, ccol2, ccol3 = st.columns([0.5, 2, 2, 2])
        
        with lcol:
            st.markdown(f'<div class="cluster-label">{clusters[i]}</div>', unsafe_allow_html=True)
        
        # 1. Number of Cases per Officer Chart
        with ccol1:
            fig1 = go.Figure()
            fig1.add_trace(go.Bar(y=['Closed', 'Open'], x=[cases_closed[i], cases_open[i]], 
                                 orientation='h', marker_color=[color_closed, color_open],
                                 text=[cases_closed[i], cases_open[i]], textposition='outside'))
            fig1.update_layout(height=120, margin=dict(l=0, r=40, t=10, b=10), showlegend=False,
                              xaxis=dict(visible=False, range=[0, 180]), yaxis=dict(visible=False),
                              paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)')
            st.plotly_chart(fig1, use_container_width=True, config={'displayModeBar': False})

        # 2. Average Case Processing Time Chart
        with ccol2:
            fig2 = go.Figure()
            fig2.add_trace(go.Bar(y=['Closed', 'Open'], x=[time_closed[i], time_open[i]], 
                                 orientation='h', marker_color=[color_closed, color_open],
                                 text=[f"{time_closed[i]} days", f"{time_open[i]} days"], textposition='outside'))
            # Vertical KPI Line
            fig2.add_vline(x=70, line_width=2, line_color="#004d66")
            
            # Label for KPI (Only on last row)
            if i == 4:
                fig2.add_annotation(x=70, y=-0.5, text="KPI: 70 days", showarrow=False, 
                                   bgcolor="#004d66", font=dict(color="white", size=10))
                
            fig2.update_layout(height=120, margin=dict(l=0, r=60, t=10, b=10), showlegend=False,
                              xaxis=dict(visible=False, range=[0, 120]), yaxis=dict(visible=False),
                              paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)')
            st.plotly_chart(fig2, use_container_width=True, config={'displayModeBar': False})

        # 3. Distribution of Case Complexity Chart
        with ccol3:
            fig3 = go.Figure()
            fig3.add_trace(go.Bar(y=['Comp'], x=[complex_simple[i]], orientation='h', 
                                 marker_color=color_simple, text=[complex_simple[i]], textposition='inside'))
            fig3.add_trace(go.Bar(y=['Comp'], x=[complex_hard[i]], orientation='h', 
                                 marker_color=color_hard, text=[complex_hard[i]], textposition='inside'))
            fig3.update_layout(height=100, barmode='stack', margin=dict(l=0, r=0, t=20, b=20), showlegend=False,
                              xaxis=dict(visible=False), yaxis=dict(visible=False),
                              paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)')
            st.plotly_chart(fig3, use_container_width=True, config={'displayModeBar': False})

# --- LEGENDS ---
st.write("")
leg_col1, leg_col2, leg_col3 = st.columns([1, 1, 1])

def create_legend_item(color, text):
    return f'<span style="display:inline-block; width:12px; height:12px; background-color:{color}; margin-right:5px;"></span>{text}'

with leg_col1:
    st.markdown(f'<div style="text-align:center; font-size:14px;">'
                f'{create_legend_item(color_open, "Currently Open")} &nbsp;&nbsp; '
                f'{create_legend_item(color_closed, "Closed To-date")}</div>', unsafe_allow_html=True)
with leg_col2:
    st.markdown(f'<div style="text-align:center; font-size:14px;">'
                f'{create_legend_item(color_open, "Currently Open")} &nbsp;&nbsp; '
                f'{create_legend_item(color_closed, "Closed To-date")}</div>', unsafe_allow_html=True)
with leg_col3:
    st.markdown(f'<div style="text-align:center; font-size:14px;">'
                f'{create_legend_item(color_simple, "Simple Cases")} &nbsp;&nbsp; '
                f'{create_legend_item(color_hard, "Complex Cases")}</div>', unsafe_allow_html=True)