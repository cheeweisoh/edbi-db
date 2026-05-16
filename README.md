# EDBI Data Warehouse

A data warehouse built on **dbt** and **Databricks**, implementing a medallion architecture for the EDBI analytics project. Includes data transformation pipelines and Streamlit dashboards for court operations and prosecution analytics.

## Architecture

Data flows through three transformation layers:

| Layer | Materialization | Purpose |
|-------|----------------|---------|
| Bronze | Incremental | Raw data ingestion |
| Silver | Incremental | Cleaned and transformed data |
| Gold | Table | Business-ready analytics |

All layers are stored in the `edbi_teamg01` catalog on Databricks.

## Project Structure

```
edbi-db/
├── models/
│   ├── bronze/        # Raw ingestion models
│   ├── silver/        # Transformation models
│   ├── gold/          # Aggregated, analytics-ready models
│   └── sources.yml    # Source definitions
├── dashboard/         # Streamlit dashboards
├── macros/            # Reusable dbt macros
├── seeds/             # Static reference data
├── snapshots/         # SCD Type 2 historical tracking (silver schema)
├── tests/             # Data quality tests
├── notebooks/         # Exploratory notebooks
├── dbt_project.yml
├── packages.yml
├── profiles.yml
└── requirements.txt
```

## Dashboards

Located in `dashboard/`, built with Streamlit and connected to Databricks:

- **Court Cases Workload** — Overview of case volumes and workload metrics
- **Workload Distribution by Officer** — Per-officer case distribution analysis
- **Prosecution Trends** — Time-series trends for prosecution activities
- **Self-Help Platform for Operational Data** — Self-service interface for operational reporting

## Prerequisites

- Python 3.9+
- Access to a Databricks workspace
- dbt-databricks adapter

## Setup

1. Clone the repository:
   ```bash
   git clone https://github.com/cheeweisoh/edbi-db.git
   cd edbi-db
   ```

2. Install Python dependencies:
   ```bash
   pip install -r requirements.txt
   ```

3. Install dbt packages:
   ```bash
   dbt deps
   ```

4. Configure your Databricks connection in `profiles.yml` (or set via environment variables using `.env`).

## Running dbt

```bash
# Run all models
dbt run

# Run a specific layer
dbt run --select bronze
dbt run --select silver
dbt run --select gold

# Run tests
dbt test

# Generate and serve documentation
dbt docs generate
dbt docs serve
```

## Running Dashboards

```bash
streamlit run dashboard/court_cases_workload_dashboard.py
```

## Dependencies

**dbt packages** (`packages.yml`):
- `dbt-labs/dbt_utils` >=1.0.0, <2.0.0

**Python packages** (`requirements.txt`):
- `streamlit` >=1.30.0
- `pandas` >=2.2.0
- `plotly` >=5.20.0
- `python-dotenv` >=1.0.0
- `databricks-sql-connector` >=0.19.0
