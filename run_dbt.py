import subprocess
import os

# Tell dbt where profiles.yml is
os.environ["DBT_PROFILES_DIR"] = os.getcwd()

subprocess.run(["dbt", "deps"])
subprocess.run(["dbt", "build", "--target", "prod"])
