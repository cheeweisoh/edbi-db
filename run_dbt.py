import subprocess, sys, os

subprocess.run([sys.executable, "-m", "pip", "install", "dbt-databricks>=1.7,<2.0", "--quiet"], check=True)

PROJECT_DIR = "/Workspace/Repos/soh_chee_wei@agc.gov.sg/edbi-db/"
PROFILES_DIR = PROJECT_DIR


def run(cmd):
    print(f"\n>>> dbt {cmd}")
    result = subprocess.run(
        [sys.executable, "-m", "dbt", cmd, "--project-dir", PROJECT_DIR, "--profiles-dir", PROFILES_DIR, "--no-use-colors"], text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT
    )
    print(result.stdout)
    if result.returncode != 0:
        raise SystemExit(f"dbt {cmd} failed (exit {result.returncode})")


run("run")
run("test")