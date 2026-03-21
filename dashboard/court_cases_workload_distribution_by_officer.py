import streamlit as st
import pandas as pd
import plotly.graph_objects as go

# Set page config
st.set_page_config(layout="wide", page_title="Court Cases Workload By Officer")

# Custom CSS for styling to match the screenshot aesthetics
st.markdown("""
    <style>
    .main-header {
        background-color: #802a3a;
        color: white;
        padding: 10px;
        text-align: center;
        font-size: 22px;
        font-weight: bold;
        margin-bottom: 15px;
        border-radius: 4px;
        text-transform: uppercase;
    }
    .chart-title-box {
        background-color: #1a1a1a;
        color: white;
        padding: 4px;
        text-align: center;
        font-size: 14px;
        font-weight: bold;
        margin-bottom: 8px;
    }
    .officer-name {
        font-weight: bold;
        text-align: right;
        padding-right: 15px;
        font-size: 13px;
        display: flex;
        align-items: center;
        justify-content: flex-end;
        height: 100%;
    }
    .stPlotlyChart {
        margin-bottom: -18px;
    }
    /* Control layout spacing */
    .block-container {
        padding-top: 2rem;
        padding-bottom: 2rem;
    }
    </style>
    """, unsafe_allow_html=True)

# --- HEADER ---
st.markdown('<div class="main-header">COURT CASES WORKLOAD DISTRIBUTION BY OFFICER</div>', unsafe_allow_html=True)

# --- FILTERS ---
f_col1, f_col2, f_col3 = st.columns([1, 1, 3])
with f_col1:
    st.selectbox("CLUSTER FILTER", ["All Clusters", "Cluster 1", "Cluster 2"], label_visibility="collapsed")
with f_col2:
    st.selectbox("DATE FILTER", ["All Dates", "Last 12 Months", "YTD"], label_visibility="collapsed")

st.write("")

# --- DATASET ---
officers = [
    "Abel", "Ben", "Cain", "Derek", "Elise", "Frank", "Greg", "Howard", "Isabelle", "James",
    "Kenny", "Leon", "Mark", "Natalie", "Olivia", "Penelope", "Quincy", "Rachel", "Sam", "Terry"
]

# Number of Cases
n_open = [20, 20, 19, 17, 13, 13, 12, 11, 10, 9, 8, 8, 8, 7, 7, 5, 5, 3, 2, 2]
n_closed = [20, 29, 15, 19, 50, 20, 32, 33, 28, 36, 25, 24, 32, 33, 8, 36, 32, 17, 7, 24]

# Processing Time
t_open = [13, 26, 26, 13, 28, 19, 19, 30, 12, 10, 25, 28, 16, 15, 28, 19, 18, 24, 25, 14]
t_closed = [79, 68, 92, 70, 38, 96, 42, 63, 64, 89, 36, 58, 68, 46, 82, 26, 71, 73, 100, 97]

# Complexity
c_simple = [31, 40, 27, 31, 60, 18, 31, 33, 29, 36, 24, 31, 38, 37, 9, 27, 36, 16, 8, 15]
c_hard = [9, 9, 7, 5, 3, 15, 13, 11, 9, 9, 9, 1, 2, 3, 6, 14, 1, 4, 1, 11]

# Global Colors
c_maroon = '#a34e5d'
c_gray = '#a6a6a6'
c_pink = '#e9a0dc'
c_purple = '#702269'
c_kpi = '#004d66'

# --- CHART TITLES ---
_, tcol1, tcol2, tcol3 = st.columns([0.6, 2, 2, 2])
tcol1.markdown('<div class="chart-title-box">Number of Cases per Officer</div>', unsafe_allow_html=True)
tcol2.markdown('<div class="chart-title-box">Average Case Processing Time</div>', unsafe_allow_html=True)
tcol3.markdown('<div class="chart-title-box">Distribution of Case Complexity</div>', unsafe_allow_html=True)

# --- ROWS (Loop through Officers) ---
for i in range(len(officers)):
    row_col, col1, col2, col3 = st.columns([0.6, 2, 2, 2])
    
    with row_col:
        st.markdown(f'<div class="officer-name">{officers[i]}</div>', unsafe_allow_html=True)
        
    # Chart 1: Case Numbers
    with col1:
        fig1 = go.Figure()
        fig1.add_trace(go.Bar(y=['C', 'O'], x=[n_closed[i], n_open[i]], orientation='h', 
                             marker_color=[c_gray, c_maroon], text=[n_closed[i], n_open[i]], 
                             textposition='outside', textfont=dict(size=10)))
        fig1.update_layout(height=65, margin=dict(l=0, r=40, t=5, b=5), showlegend=False,
                          xaxis=dict(visible=False, range=[0, 65]), yaxis=dict(visible=False),
                          paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)')
        st.plotly_chart(fig1, use_container_width=True, config={'displayModeBar': False})

    # Chart 2: Processing Time
    with col2:
        fig2 = go.Figure()
        fig2.add_trace(go.Bar(y=['C', 'O'], x=[t_closed[i], t_open[i]], orientation='h', 
                             marker_color=[c_gray, c_maroon], text=[f"{t_closed[i]}d", f"{t_open[i]}d"], 
                             textposition='outside', textfont=dict(size=10)))
        fig2.add_vline(x=70, line_width=1.5, line_color=c_kpi)
        
        # Show KPI label only on the very last row
        if i == 19:
            fig2.add_annotation(x=70, y=-0.5, text="KPI: 70 days", showarrow=False, 
                               bgcolor=c_kpi, font=dict(color="white", size=9))
            
        fig2.update_layout(height=65, margin=dict(l=0, r=60, t=5, b=5), showlegend=False,
                          xaxis=dict(visible=False, range=[0, 125]), yaxis=dict(visible=False),
                          paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)')
        st.plotly_chart(fig2, use_container_width=True, config={'displayModeBar': False})

    # Chart 3: Complexity
    with col3:
        fig3 = go.Figure()
        fig3.add_trace(go.Bar(y=['Comp'], x=[c_simple[i]], orientation='h', marker_color=c_pink, 
                             text=[c_simple[i]], textposition='inside', textfont=dict(size=10, color='black')))
        fig3.add_trace(go.Bar(y=['Comp'], x=[c_hard[i]], orientation='h', marker_color=c_purple, 
                             text=[c_hard[i]], textposition='inside', textfont=dict(size=10, color='white')))
        fig3.update_layout(height=50, barmode='stack', margin=dict(l=0, r=0, t=10, b=10), showlegend=False,
                          xaxis=dict(visible=False, range=[0, 75]), yaxis=dict(visible=False),
                          paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)')
        st.plotly_chart(fig3, use_container_width=True, config={'displayModeBar': False})

# --- LEGENDS ---
st.write("")
leg_col1, leg_col2, leg_col3 = st.columns([1, 1, 1])

def get_leg_html(color, label):
    return f'<span style="display:inline-block; width:12px; height:12px; background-color:{color}; margin-right:5px; vertical-align:middle;"></span><span style="font-size:12px; color:#555;">{label}</span>'

with leg_col1:
    st.markdown(f'<div style="text-align:center;">{get_leg_html(c_maroon, "Currently Open")} &nbsp;&nbsp; {get_leg_html(c_gray, "Closed To-date")}</div>', unsafe_allow_html=True)
with leg_col2:
    st.markdown(f'<div style="text-align:center;">{get_leg_html(c_maroon, "Currently Open")} &nbsp;&nbsp; {get_leg_html(c_gray, "Closed To-date")}</div>', unsafe_allow_html=True)
with leg_col3:
    st.markdown(f'<div style="text-align:center;">{get_leg_html(c_pink, "Simple Cases")} &nbsp;&nbsp; {get_leg_html(c_purple, "Complex Cases")}</div>', unsafe_allow_html=True)