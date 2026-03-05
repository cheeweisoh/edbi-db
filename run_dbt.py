import runpy
import sys
import os
import logging

PROJECT_DIR = "/Workspace/Users/soh_chee_wei@agc.gov.sg/edbi-db/"
PROFILES_DIR = PROJECT_DIR

DEBUG = True

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("run_dbt")

if "DATABRICKS_TOKEN" not in os.environ:
    logger.warning("DATABRICKS_TOKEN not found. Make sure OAuth is enabled on your job cluster.")

def run_dbt_command(command: str):
    """
    Run a dbt command via Python module instead of subprocess.
    command: e.g., "run", "test", "debug"
    """
    logger.info(f">>> Running dbt {command}")

    args = [
        command,
        "--project-dir", PROJECT_DIR,
        "--profiles-dir", PROFILES_DIR,
        "--no-use-colors"
    ]

    if DEBUG:
        args.append("--debug")

    sys.argv = args

    try:
        runpy.run_module("dbt.main", run_name="__main__")
    except SystemExit as e:
        if e.code != 0:
            logger.error(f"dbt {command} failed with exit code {e.code}")
            raise
    except Exception as e:
        logger.exception(f"Error running dbt {command}: {e}")
        raise

if __name__ == "__main__":
    run_dbt_command("debug")
    run_dbt_command("run")
    run_dbt_command("test")

    logger.info("dbt run completed successfully!")