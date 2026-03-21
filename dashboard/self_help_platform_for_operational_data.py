import streamlit as st
import pandas as pd

# Set page config
st.set_page_config(layout="wide", page_title="Self-Help Platform for Operational Data")

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
        margin-bottom: 15px;
        border-radius: 4px;
    }
    .sub-header {
        background-color: #000000;
        color: white;
        padding: 5px;
        text-align: center;
        font-size: 16px;
        font-weight: bold;
        margin-bottom: 10px;
    }
    .product-tab {
        background-color: #1a1a1a;
        color: white;
        padding: 15px;
        text-align: center;
        border-radius: 10px;
        border: 2px solid #555;
        font-weight: bold;
        margin-bottom: 20px;
    }
    .product-tab-inactive {
        background-color: #6c757d;
        color: white;
        padding: 15px;
        text-align: center;
        border-radius: 10px;
        border: 2px solid #555;
        font-weight: bold;
        opacity: 0.8;
    }
    .download-btn {
        background-color: #a34e5d;
        color: white;
        padding: 5px 15px;
        border-radius: 20px;
        text-align: center;
        font-weight: bold;
        float: right;
    }
    .data-table {
        width: 100%;
        border-collapse: collapse;
        font-size: 13px;
    }
    .data-table th {
        background-color: #e0e0e0;
        text-align: left;
        padding: 8px;
        border: 1px solid #ccc;
    }
    .data-table td {
        padding: 8px;
        border: 1px solid #ccc;
    }
    .label-cell {
        background-color: #f2f2f2;
        font-weight: bold;
        width: 30%;
    }
    </style>
    """, unsafe_allow_html=True)

# --- HEADER ---
st.markdown('<div class="main-header">SELF-HELP PLATFORM FOR OPERATIONAL DATA</div>', unsafe_allow_html=True)

# --- AVAILABLE DATA PRODUCTS ---
st.markdown('<div class="sub-header">Available Data Products</div>', unsafe_allow_html=True)

prod_col1, prod_col2, prod_col3 = st.columns(3)
with prod_col1:
    st.markdown('<div class="product-tab">List of Ongoing Court Cases</div>', unsafe_allow_html=True)
with prod_col2:
    st.markdown('<div class="product-tab-inactive">List of Court Events</div>', unsafe_allow_html=True)
with prod_col3:
    st.markdown('<div class="product-tab-inactive">...</div>', unsafe_allow_html=True)

# --- MAIN CONTENT ---
left_col, right_col = st.columns([1.5, 2.5])

with left_col:
    # Data Product Details
    st.markdown('<div class="sub-header">Data Product Details</div>', unsafe_allow_html=True)
    details_data = [
        ["Update Frequency", "Every 1 Day"],
        ["Coverage Range", "From January 2025"],
        ["Data Classification", "Confidential, Cloud Eligible"],
        ["Managed By", "Soh Chee Wei"]
    ]
    details_html = '<table class="data-table">'
    for row in details_data:
        details_html += f'<tr><td class="label-cell">{row[0]}</td><td>{row[1]}</td></tr>'
    details_html += '</table>'
    st.markdown(details_html, unsafe_allow_html=True)
    
    st.write("")
    
    # Column Definitions
    st.markdown('<div class="sub-header">Column Definitions</div>', unsafe_allow_html=True)
    cols_def = [
        ["CaseID", "Unique identifier for court cases"],
        ["Cluster", "Cluster overseeing this court case"],
        ["DPPName", "DPP assigned to this court case; one row per assigned DPP"],
        ["CourtEventType", "Court event type for scheduled court events"],
        ["CourtDateTime", "Court event date and time for scheduled court events"],
        ["CourtNo", "Court number for scheduled court events"]
    ]
    cols_html = '<table class="data-table">'
    for row in cols_def:
        cols_html += f'<tr><td class="label-cell">{row[0]}</td><td>{row[1]}</td></tr>'
    cols_html += '</table>'
    st.markdown(cols_html, unsafe_allow_html=True)

with right_col:
    # Data Download Header
    d_col1, d_col2 = st.columns([3, 1])
    with d_col1:
        st.markdown('<div class="sub-header">Data Download</div>', unsafe_allow_html=True)
    with d_col2:
        st.button("DOWNLOAD BUTTON", key="dl_btn")

    # Select Columns Section
    st.markdown('<div class="sub-header" style="font-size:14px;">SELECT COLUMNS</div>', unsafe_allow_html=True)
    sel_col1, sel_col2, sel_col3, sel_col4, sel_col5, sel_col6 = st.columns(6)
    c1 = sel_col1.checkbox("CaseID", value=True)
    c2 = sel_col2.checkbox("Cluster", value=True)
    c3 = sel_col3.checkbox("DPPName", value=True)
    c4 = sel_col4.checkbox("CourtEventType")
    c5 = sel_col5.checkbox("CourtDateTime")
    c6 = sel_col6.checkbox("CourtNo")

    # Filters Section
    st.markdown('<div class="sub-header" style="font-size:14px;">FILTERS</div>', unsafe_allow_html=True)
    f_col1, f_col2, f_col3, f_col4, f_col5, f_col6 = st.columns(6)
    f_col1.selectbox("CaseID ▼", ["All"], label_visibility="collapsed")
    f_col2.selectbox("Cluster ▼", ["All"], label_visibility="collapsed")
    f_col3.selectbox("DPPName ▼", ["All"], label_visibility="collapsed")
    f_col4.selectbox("CourtEventType ▼", ["All"], label_visibility="collapsed")
    f_col5.selectbox("CourtDateTime ▼", ["All"], label_visibility="collapsed")
    f_col6.selectbox("CourtNo ▼", ["All"], label_visibility="collapsed")

    # Data Preview Section
    st.markdown('<div class="sub-header" style="font-size:14px;">DATA PREVIEW</div>', unsafe_allow_html=True)
    
    preview_data = {
        "CaseID": ["SC-905025-2022", "SC-905025-2022", "SC-904539-2025", "SC-909210-2025", "SC-909210-2025", "SC-909210-2025"],
        "Cluster": ["CL1-T1", "CL1-T1", "CL1-T2", "CAD OFFICE", "CL4-T15", "CL4-T15"],
        "DPPName": ["Abel", "Abel", "Ben", "Cain", "Derek", "Derek"],
        "CourtEventType": ["Trial", "Trial", "Trial", "For Mention (PG)", "Trial", "For Mention (Verdict)"],
        "CourtDateTime": ["2026-01-08 09:30:00", "2026-01-09 14:30:00", "2026-01-08 14:30:00", "2026-01-09 14:30:00", "2026-01-08 09:30:00", "2026-03-09 14:30:00"],
        "CourtNo": ["Court 15B", "Court 15B", "Court 11D", "Court 11D", "Court 32A", "Court 32A"]
    }
    
    df = pd.DataFrame(preview_data)
    
    # Simple logic to show selected columns only
    cols_to_show = []
    if c1: cols_to_show.append("CaseID")
    if c2: cols_to_show.append("Cluster")
    if c3: cols_to_show.append("DPPName")
    if c4: cols_to_show.append("CourtEventType")
    if c5: cols_to_show.append("CourtDateTime")
    if c6: cols_to_show.append("CourtNo")
    
    if cols_to_show:
        st.dataframe(df[cols_to_show], use_container_width=True, hide_index=True)
    else:
        st.info("Select at least one column to preview data.")