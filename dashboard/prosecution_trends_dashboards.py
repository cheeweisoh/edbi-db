import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import os
import sys
from pathlib import Path
from dotenv import load_dotenv
from typing import Optional, List

# Load dashboard .env to ensure Databricks credentials are available
print(Path(__file__).resolve().parent)
load_dotenv(Path(__file__).resolve().parent / '.env')

# Make parent workspace importable from dashboard folder
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)

from dashboard.databricks_connector import DatabricksConnector

# --- PAGE CONFIG ---
st.set_page_config(layout="wide", page_title="Operational Data Hub")

# --- GLOBAL STYLES ---
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
        text-transform: uppercase;
    }
    .sub-header {
        background-color: #1a1a1a;
        color: white;
        padding: 5px 15px;
        font-size: 16px;
        font-weight: bold;
        text-align: center;
        margin-bottom: 10px;
    }
    .info-box {
        background-color: #e0e0e0;
        padding: 8px 15px;
        border-radius: 5px;
        font-size: 14px;
        font-weight: bold;
        color: #333;
    }
    .data-table {
        width: 100%;
        border-collapse: collapse;
        font-size: 13px;
    }
    .data-table th, .data-table td {
        padding: 8px;
        border: 1px solid #ccc;
    }
    .label-cell {
        background-color: #f2f2f2;
        font-weight: bold;
        width: 35%;
    }
    .product-tab {
        background-color: #1a1a1a;
        color: white;
        padding: 12px;
        text-align: center;
        border-radius: 8px;
        border: 2px solid #555;
        font-weight: bold;
    }
    /* Officer View Specifics */
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
    </style>
    """, unsafe_allow_html=True)

# --- NAVIGATION ---
with st.sidebar:
    st.title("Navigation")
    page = st.radio(
        "Select Dashboard",
        ["Prosecution Trends", "Workload Overview (Cluster)", "Officer Breakdown", "Self-Help Platform"]
    )
    st.divider()
    st.info("Unified Operational Data Hub v1.0")

# --- SHARED CONSTANTS ---
C_MAROON = '#a34e5d'
C_GRAY = '#a6a6a6'
C_PINK = '#e9a0dc'
C_PURPLE = '#702269'
C_KPI = '#004d66'

@st.cache_data(ttl=300)
def get_filter_values():
    """Load available cluster and year filters from table."""
    http_path = os.getenv("DATABRICKS_HTTP_PATH", "")
    if "<your-warehouse-id>" in http_path or http_path.strip() == "":
        return ["All Clusters"], ["All Years"]
    try:
        connector = DatabricksConnector()
        df = connector.query(
            """
            SELECT DISTINCT officer_cluster AS cluster,
                   date_format(first_mention_date, 'yyyy') AS year
            FROM edbi_teamg01.gold.case_offence_distribution
            WHERE officer_cluster IS NOT NULL
            ORDER BY cluster, year
            """
        )
        if df.empty:
            return ["All Clusters"], ["All Years"]
        df.columns = [c.lower() for c in df.columns]
        cluster_values = [x for x in sorted(df['cluster'].dropna().unique())]
        year_values = [x for x in sorted(df['year'].dropna().unique())]
        return ["All Clusters"] + cluster_values, ["All Years"] + year_values
    except Exception:
        return ["All Clusters"], ["All Years"]

@st.cache_data(ttl=300)
def load_case_offence_distribution(selected_clusters: Optional[List[str]] = None, selected_months: Optional[List[str]] = None):
    """Load case distribution from Databricks table with fallback sample data."""
    http_path = os.getenv("DATABRICKS_HTTP_PATH", "")
    if "<your-warehouse-id>" in http_path or http_path.strip() == "":
        raise ValueError("DATABRICKS_HTTP_PATH is not configured. Update dashboard/.env with your warehouse endpoint path.")

    def map_case_status(row):
        """Map case_status and case_type combination to display label."""
        status = row.get('case_status', '').upper()
        case_type = row.get('case_type', '').upper()
        
        if status == 'PEND' and case_type == 'PG':
            return 'Ongoing PG'
        elif status == 'PEND' and case_type == 'TRIAL':
            return 'Ongoing Trial'
        elif status == 'DISP' and case_type == 'PG':
            return 'Concluded PG'
        elif status == 'DISP' and case_type == 'TRIAL':
            return 'Concluded Trial'
        elif status in ('DATA', 'DNATA'):
            return 'Concluded Acquittal'
        else:
            return f'{status} {case_type}'.strip()

    try:
        connector = DatabricksConnector()
        params = {}
        query = """
        SELECT offence_group AS offence_type,
               case_status,
               case_type,
               SUM(case_count) AS case_count
        FROM edbi_teamg01.gold.case_offence_distribution
        WHERE 1=1
        """

        if selected_clusters:
            cluster_placeholders = []
            for idx, cluster in enumerate(selected_clusters):
                key = f"cluster_{idx}"
                cluster_placeholders.append(f":{key}")
                params[key] = cluster
            query += f" AND officer_cluster IN ({', '.join(cluster_placeholders)})"

        if selected_months:
            year_placeholders = []
            for idx, year in enumerate(selected_months):
                key = f"year_{idx}"
                year_placeholders.append(f":{key}")
                params[key] = year
            query += f" AND date_format(first_mention_date, 'yyyy') IN ({', '.join(year_placeholders)})"

        query += " GROUP BY offence_group, case_status, case_type"

        df = connector.query(query, params=params if params else None)
        if df.empty:
            raise ValueError("No rows returned from case_offence_distribution")

        df.columns = [c.lower() for c in df.columns]
        if 'offence_type' not in df.columns or 'case_status' not in df.columns or 'case_count' not in df.columns:
            raise ValueError('Expected columns offence_type, case_status, case_count not found')

        # Create composite case_status from case_status and case_type
        df['case_status'] = df.apply(map_case_status, axis=1)
        df = df[['offence_type', 'case_status', 'case_count']]
        return df
    except Exception as exc:
        st.warning(f"Databricks load failed: {exc}. Using fallback sample data.")
        return pd.DataFrame(
            {
                'offence_type': ['Hurt', 'Harassment', 'Vulnerable Victims', 'Sexual Exposure', 'Others'] * 4,
                'case_status': ['Ongoing PG', 'Ongoing Trial', 'Concluded PG', 'Concluded Trial'] * 5,
                'case_count': [40, 230, 80, 220, 20, 100, 80, 150, 20, 240, 110, 145, 40, 350, 40, 225, 80, 480, 235, 575]
            }
        )

# --- DASHBOARD FUNCTIONS ---

def show_prosecution_trends():
    st.markdown('<div class="main-header">PROSECUTION TRENDS</div>', unsafe_allow_html=True)
    col1, col2, _ = st.columns([1, 1, 3])
    st.markdown('<div class="sub-header">Number of Cases by Offence Type and Case Status</div>', unsafe_allow_html=True)
    clusters, years = get_filter_values()
    selected_cluster = col1.multiselect("CLUSTER FILTER", clusters, default=[clusters[0]] if clusters else [], key="cluster_filter", label_visibility="collapsed")
    selected_year = col2.multiselect("YEAR FILTER", years, default=[years[0]] if years else [], key="year_filter", label_visibility="collapsed")

    selected_clusters = [] if not selected_cluster or "All Clusters" in selected_cluster else selected_cluster
    selected_years = [] if not selected_year or "All Years" in selected_year else selected_year

    df = load_case_offence_distribution(selected_clusters=selected_clusters, selected_months=selected_years)
    if not df.empty:
        pivot = df.pivot_table(index='offence_type', columns='case_status', values='case_count', aggfunc='sum', fill_value=0)
        pivot = pivot.reset_index()
        # Keep offense order from table first and by total descending fallback
        if 'offence_type' in pivot.columns:
            offences = pivot['offence_type'].tolist()[::-1]
        else:
            offences = []

        fig = go.Figure()
        status_columns = [col for col in pivot.columns if col != 'offence_type']
        # Keep a stable color palette for up to 5 categories
        color_palette = ['#b86b7e', '#d291a1', '#e8b8c5', '#7a7a7a', '#a6a6a6']
        for i, status in enumerate(status_columns):
            values = pivot[status].tolist()[::-1]
            fig.add_trace(go.Bar(y=offences, x=values, name=status, orientation='h', marker_color=color_palette[i % len(color_palette)], text=values))
        if not status_columns:
            st.info('No case status columns available for charting.')
        else:
            # Increase chart height for larger row sets
            fig.update_layout(barmode='stack', height=500, margin=dict(l=0, r=0, t=10, b=10), legend=dict(orientation='h', y=-0.2), xaxis_title='Case Count', yaxis_title='Offence Group')
            st.plotly_chart(fig, width='stretch')
    else:
        st.info('No offence distribution data available yet.')

    st.markdown('<div class="sub-header">Average Time Spent in each Stage</div>', unsafe_allow_html=True)
    sc1, sc2 = st.columns(2)
    with sc1:
        st.write("**PG Cases**")
        fig_pg = go.Figure()
        fig_pg.add_trace(go.Bar(y=offences, x=[25,23,50,45,32], name="Non-PG", orientation='h', marker_color='#1f4e79'))
        fig_pg.add_trace(go.Bar(y=offences, x=[28,25,53,55,38], name="PG", orientation='h', marker_color='#2e75b6'))
        fig_pg.update_layout(barmode='stack', height=250, margin=dict(l=0, r=0, t=0, b=0))
        st.plotly_chart(fig_pg, width='stretch')
    with sc2:
        st.write("**Trial Cases**")
        fig_tr = go.Figure()
        fig_tr.add_trace(go.Bar(y=offences, x=[29,56,54,49,44], name="Non-PG", orientation='h', marker_color='#1f4e79'))
        fig_tr.add_trace(go.Bar(y=offences, x=[158,154,142,153,105], name="Trial", orientation='h', marker_color='#d9e9f6'))
        fig_tr.update_layout(barmode='stack', height=250, margin=dict(l=0, r=0, t=0, b=0))
        st.plotly_chart(fig_tr, width='stretch')

def show_workload_overview():
    st.markdown('<div class="main-header">COURT CASES WORKLOAD DISTRIBUTION OVERVIEW</div>', unsafe_allow_html=True)
    c1, c2, c3 = st.columns([1,1,2.5])
    c3.markdown('<div class="info-box">ⓘ Select "Officer Breakdown" in navigation for details</div>', unsafe_allow_html=True)
    
    clusters = ["Cluster 1", "Cluster 2", "Cluster 3", "Cluster 4", "Cluster 5"]
    case_c = [83, 135, 113, 109, 80]
    case_o = [76, 49, 35, 27, 12]

    _, h1, h2, h3 = st.columns([0.6, 2, 2, 2])
    h1.markdown('<div class="sub-header">Cases per Officer</div>', unsafe_allow_html=True)
    h2.markdown('<div class="sub-header">Processing Time</div>', unsafe_allow_html=True)
    h3.markdown('<div class="sub-header">Complexity</div>', unsafe_allow_html=True)

    for i in range(5):
        lc, cc1, cc2, cc3 = st.columns([0.6, 2, 2, 2])
        lc.write(f"**{clusters[i]}**")
        with cc1:
            f = go.Figure(go.Bar(y=['C','O'], x=[case_c[i], case_o[i]], orientation='h', marker_color=[C_GRAY, C_MAROON], text=[case_c[i], case_o[i]], textposition='outside'))
            f.update_layout(height=100, margin=dict(l=0,r=40,t=5,b=5), xaxis=dict(visible=False), yaxis=dict(visible=False))
            st.plotly_chart(f, use_container_width=True, config={'displayModeBar': False}, key=f'workload_cases_{i}')
        with cc2:
            f = go.Figure(go.Bar(y=['C','O'], x=[60, 20], orientation='h', marker_color=[C_GRAY, C_MAROON], text=["60d", "20d"], textposition='outside'))
            f.add_vline(x=70, line_width=2, line_color=C_KPI)
            f.update_layout(height=100, margin=dict(l=0,r=40,t=5,b=5), xaxis=dict(visible=False), yaxis=dict(visible=False))
            st.plotly_chart(f, use_container_width=True, config={'displayModeBar': False}, key=f'workload_time_{i}')
        with cc3:
            f = go.Figure([go.Bar(x=[100], orientation='h', marker_color=C_PINK), go.Bar(x=[30], orientation='h', marker_color=C_PURPLE)])
            f.update_layout(barmode='stack', height=80, margin=dict(l=0,r=0,t=10,b=10), xaxis=dict(visible=False), yaxis=dict(visible=False), showlegend=False)
            st.plotly_chart(f, use_container_width=True, config={'displayModeBar': False}, key=f'workload_complexity_{i}')

def show_officer_breakdown():
    st.markdown('<div class="main-header">COURT CASES WORKLOAD DISTRIBUTION BY OFFICER</div>', unsafe_allow_html=True)
    officers = ["Abel", "Ben", "Cain", "Derek", "Elise", "Frank", "Greg", "Howard", "Isabelle", "James"]
    
    _, h1, h2, h3 = st.columns([0.6, 2, 2, 2])
    h1.markdown('<div class="sub-header" style="font-size:12px;">Cases</div>', unsafe_allow_html=True)
    h2.markdown('<div class="sub-header" style="font-size:12px;">Processing Time</div>', unsafe_allow_html=True)
    h3.markdown('<div class="sub-header" style="font-size:12px;">Complexity</div>', unsafe_allow_html=True)

    for name in officers:
        lc, cc1, cc2, cc3 = st.columns([0.6, 2, 2, 2])
        lc.markdown(f'<div class="officer-name">{name}</div>', unsafe_allow_html=True)
        with cc1:
            f = go.Figure(go.Bar(y=['C','O'], x=[30, 15], orientation='h', marker_color=[C_GRAY, C_MAROON], text=[30, 15], textposition='outside'))
            f.update_layout(height=60, margin=dict(l=0,r=30,t=5,b=5), xaxis=dict(visible=False), yaxis=dict(visible=False))
            st.plotly_chart(f, use_container_width=True, config={'displayModeBar': False}, key=f'officer_cases_{name}')
        with cc2:
            f = go.Figure(go.Bar(y=['C','O'], x=[65, 12], orientation='h', marker_color=[C_GRAY, C_MAROON], text=["65d", "12d"], textposition='outside'))
            f.add_vline(x=70, line_width=1.5, line_color=C_KPI)
            f.update_layout(height=60, margin=dict(l=0,r=50,t=5,b=5), xaxis=dict(visible=False), yaxis=dict(visible=False))
            st.plotly_chart(f, use_container_width=True, config={'displayModeBar': False}, key=f'officer_time_{name}')
        with cc3:
            f = go.Figure([go.Bar(x=[25], orientation='h', marker_color=C_PINK), go.Bar(x=[8], orientation='h', marker_color=C_PURPLE)])
            f.update_layout(barmode='stack', height=50, margin=dict(l=0,r=0,t=5,b=5), xaxis=dict(visible=False), yaxis=dict(visible=False), showlegend=False)
            st.plotly_chart(f, use_container_width=True, config={'displayModeBar': False}, key=f'officer_complexity_{name}')

def show_self_help():
    st.markdown('<div class="main-header">SELF-HELP PLATFORM FOR OPERATIONAL DATA</div>', unsafe_allow_html=True)
    st.markdown('<div class="sub-header">Available Data Products</div>', unsafe_allow_html=True)
    pc1, pc2, pc3 = st.columns(3)
    pc1.markdown('<div class="product-tab">List of Ongoing Court Cases</div>', unsafe_allow_html=True)
    pc2.markdown('<div class="product-tab" style="opacity:0.6;">List of Court Events</div>', unsafe_allow_html=True)
    pc3.markdown('<div class="product-tab" style="opacity:0.6;">...</div>', unsafe_allow_html=True)
    
    st.write("")
    l, r = st.columns([1.5, 2.5])
    with l:
        st.markdown('<div class="sub-header">Data Product Details</div>', unsafe_allow_html=True)
        st.markdown('<table class="data-table"><tr><td class="label-cell">Update Freq</td><td>Every 1 Day</td></tr><tr><td class="label-cell">Coverage</td><td>From Jan 2025</td></tr><tr><td class="label-cell">Classification</td><td>Confidential</td></tr></table>', unsafe_allow_html=True)
        st.write("")
        st.markdown('<div class="sub-header">Column Definitions</div>', unsafe_allow_html=True)
        st.markdown('<table class="data-table"><tr><td class="label-cell">CaseID</td><td>Unique ID</td></tr><tr><td class="label-cell">Cluster</td><td>Overseeing Cluster</td></tr></table>', unsafe_allow_html=True)
    with r:
        st.markdown('<div class="sub-header">Data Download</div>', unsafe_allow_html=True)
        st.multiselect("Select Columns", ["CaseID", "Cluster", "DPPName", "Type"], default=["CaseID", "Cluster"])
        st.dataframe(pd.DataFrame({"CaseID": ["SC-123", "SC-456"], "Cluster": ["CL1", "CL2"]}), use_container_width=True)

# --- PAGE ROUTING ---
if page == "Prosecution Trends":
    show_prosecution_trends()
elif page == "Workload Overview (Cluster)":
    show_workload_overview()
elif page == "Officer Breakdown":
    show_officer_breakdown()
elif page == "Self-Help Platform":
    show_self_help()