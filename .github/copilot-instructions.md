# Copilot Instructions for edbi-db

## Purpose
This file tells Copilot Chat how to work effectively in this repository.

## Project overview
- Python-based ETL/dashboard project for operational data analytics.
- Main dashboard code in `dashboard/` with Streamlit (`streamlit` plus `plotly`, `pandas`).
- Databricks connector under `dashboard/databricks_connector.py`.
- DBT models live under `models/`.
- Sample data/metadata seeds are in `seeds/`.

## Setup
1. Create and activate venv:
   - `python3 -m venv .venv`
   - `source .venv/bin/activate`
2. Install dependencies:
   - `pip install -r requirements.txt`
3. Set Databricks env in `dashboard/.env` (or root `.env`) with:
   - `DATABRICKS_HOST`, `DATABRICKS_HTTP_PATH`, `DATABRICKS_TOKEN` or OAuth creds.

## Run
- Streamlit dashboard:
  - `streamlit run dashboard/prosecution_trends_dashboards.py`
- Databricks connector tests:
  - `pytest` (if tests are added; currently none present).

## Conventions
- Use `@st.cache_data` for expensive Databricks queries.
- Keep dashboards declarative: compute filter values, query once, then chart.
- Use plain Python loops and pandas DataFrames for transformations.
- SQL uses `DatabricksConnector.query(sql, params=...)` with named params (no SQL injection).

## What to change first
- Look for features in `dashboard/prosecution_trends_dashboards.py` and replicate in similar pages.
- Update existing logic safely using type hints and explicit list handling.

## Agent guidelines
- Prefer short, direct code modifications.
- Validate with `python3 -m py_compile <file>` when changing Python files.
- Keep `requirements.txt` dependencies as is unless new package is strictly necessary.
- When writing new code, include explanatory comments in the code file and follow existing style patterns.

## Example Copilot prompts
- "Add multi-select filtering for cluster and date in `dashboard/prosecution_trends_dashboards.py` using Streamlit."
- "Convert selectbox filters to multiselect and apply SQL `IN` logic properly with Databricks parameterization."
- "Create a unit test for `get_filter_values()` to ensure it returns `All Clusters` and `All Dates` fallback when query fails."

## Next customization ideas
- Add `dashboard/AGENTS.md` with role-specific workflows for data engineering, dashboard, and DBT.
- Add CI instructions for linting, formatting, and tests in GitHub Actions.
