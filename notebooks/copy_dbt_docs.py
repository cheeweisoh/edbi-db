%pip install dbt-databricks

import os
import shutil
import subprocess
from pathlib import Path

dbutils.widgets.text("dbt_project_dir", "")
dbutils.widgets.text("dbfs_dest", "")
dbutils.widgets.text("databricks_host", "")

DBT_PROJECT_DIR = dbutils.widgets.get("dbt_project_dir")
DBFS_DEST = dbutils.widgets.get("dbfs_dest")
DATABRICKS_HOST = dbutils.widgets.get("databricks_host")

TARGET_DIR = Path(DBT_PROJECT_DIR) / "target"


def generate_dbt_docs():
    print("Generating dbt docs...")
    result = subprocess.run(["dbt", "docs", "generate"], cwd=DBT_PROJECT_DIR, capture_output=True, text=True)

    if result.returncode != 0:
        print(result.stdout)
        print(result.stderr)
        raise Exception("dbt docs generation failed")

    print("dbt docs generated successfully.")


def upload_to_dbfs():
    print("Uploading docs to DBFS...")

    # Clean existing folder (optional)
    subprocess.run(["databricks", "fs", "rm", "-r", DBFS_DEST], check=False)

    # Upload target folder
    subprocess.run(["databricks", "fs", "cp", "-r", str(TARGET_DIR), DBFS_DEST], check=True)

    print("Upload complete.")


def print_url():
    url = f"{DATABRICKS_HOST}/files/dbt-docs/index.html"
    print("\nDocs available at:")
    print(url)


if __name__ == "__main__":
    generate_dbt_docs()
    upload_to_dbfs()
    print_url()
