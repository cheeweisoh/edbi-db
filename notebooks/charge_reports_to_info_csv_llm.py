"""Convert ChargeReports markdown to Info to Extract CSV using LiteLLM.

Usage:
  python charge_reports_to_info_csv_llm.py
  python charge_reports_to_info_csv_llm.py --sample 50
  python charge_reports_to_info_csv_llm.py --charge DAC-000001-2023

Secrets Setup:
  Store credentials in a Databricks secret scope (e.g. "llm-secrets"):
    databricks secrets put-secret llm-secrets LLM_API_KEY
    databricks secrets put-secret llm-secrets LLM_BASE_URL
    databricks secrets put-secret llm-secrets LLM_MODEL
  Or set environment variables: LLM_API_KEY, LLM_BASE_URL, LLM_MODEL
"""

import argparse
import csv
import json
import os
import re
import sys
from pathlib import Path

try:
    from openai import OpenAI
except ImportError:
    print("ERROR: openai package not installed. Run: pip install openai", file=sys.stderr)
    sys.exit(1)

BASE_DIR = Path("/Volumes/edbi_teamg01/landing/statecourts/charge_reports/")
CHARGE_REPORTS_DIR = BASE_DIR / "ChargeReports"

from datetime import datetime, timedelta, timezone
OUTPUT_DIR = Path("/Volumes/edbi_teamg01/landing/statecourts/info_extracted") 
tz = timezone(timedelta(hours=8))
DEFAULT_OUT = OUTPUT_DIR / f"Info_Extracted_{datetime.now(tz).strftime('%Y%m%d_%H%M')}.csv"

SECRET_SCOPE = "llm-secrets"  # Change to your Databricks secret scope name


def _get_secret(key: str, default: str | None = None) -> str:
    """Retrieve a secret from Databricks secret scope, falling back to env var."""
    # Try Databricks secrets first
    try:
        from pyspark.dbutils import DBUtils
        from pyspark.sql import SparkSession
        spark = SparkSession.builder.getOrCreate()
        dbutils = DBUtils(spark)
        value = dbutils.secrets.get(scope=SECRET_SCOPE, key=key)
        if value:
            return value
    except Exception:
        pass  # Not running in Databricks or scope not configured

    # Fall back to environment variable
    value = os.environ.get(key)
    if value:
        return value

    if default is not None:
        return default

    print(f"ERROR: Secret '{key}' not found. Set it in Databricks secret scope "
          f"'{SECRET_SCOPE}' or as an environment variable.", file=sys.stderr)
    sys.exit(1)


LLM_API_KEY = _get_secret("LLM_API_KEY")
LLM_BASE_URL = _get_secret("LLM_BASE_URL")
LLM_MODEL = _get_secret("LLM_MODEL", default="agc-llmaas-claude-4-5-dev")

LLM_PROMPT = """\
You are a legal data extraction assistant. Given a Singapore charge report document,
extract these fields as JSON. Return only valid JSON with keys exactly as listed.
If a field is absent, set it to null for strings or null for numbers.
Only extract 'victim_*' from 'Victim Particulars'. Do not infer victim data from the statement.

Fields:
- case
- charge
- accused_name
- accused_age
- accused_gender
- accused_relationship_to_victim
- victim_name
- victim_age
- victim_gender
- victim_relationship_to_accused
- offence_group
- special_type

Document:
{document}
"""


def call_llm(text: str, client) -> dict:
    res = client.chat.completions.create(
        model=LLM_MODEL,
        messages=[{"role": "user", "content": LLM_PROMPT.format(document=text)}],
        temperature=0,
    )
    content = res.choices[0].message.content
    if "```json" in content:
        content = content.split("```json")[1].split("```")[0]
    elif "```" in content:
        content = content.split("```")[1].split("```")[0]
    data = json.loads(content.strip())
    return data


def to_csv_rows(extracted: dict) -> list[dict]:
    rows = []
    case = extracted.get("case") or ""
    charge = extracted.get("charge") or ""
    rows.append({
        "case_no": case,
        "charge_no": charge,
        "entity_name": (extracted.get("accused_name") or "").title(),
        "entity_type": "Accused Person",
        "entity_age": extracted.get("accused_age") or "",
        "entity_gender": extracted.get("accused_gender") or "",
        "relationship_to_victim": extracted.get("accused_relationship_to_victim") or "",
        "offence_group": extracted.get("offence_group") or "",
        "special_type": extracted.get("special_type") or "",
    })
    if extracted.get("victim_name"):
        rows.append({
            "case_no": case,
            "charge_no": charge,
            "entity_name": (extracted.get("victim_name") or "").title(),
            "entity_type": "Victim",
            "entity_age": extracted.get("victim_age") or "",
            "entity_gender": extracted.get("victim_gender") or "",
            "relationship_to_victim": extracted.get("victim_relationship_to_accused") or "",
            "offence_group": extracted.get("offence_group") or "",
            "special_type": extracted.get("special_type") or "",
        })
    return rows


def main():
    parser = argparse.ArgumentParser(description="Extract charge reports to Info to Extract CSV via LiteLLM")
    parser.add_argument("--charge", type=str, default=None, help="Single charge file name without extension")
    parser.add_argument("--sample", type=int, default=0, help="Random sample count")
    parser.add_argument("--output", type=str, default=str(DEFAULT_OUT), help="Output CSV path")
    args = parser.parse_args()

    if args.charge:
        files = [CHARGE_REPORTS_DIR / f"{args.charge}.md"]
    else:
        files = sorted(CHARGE_REPORTS_DIR.glob("*.md"))
        if args.sample > 0:
            import random
            random.seed(42)
            files = random.sample(files, min(args.sample, len(files)))

    if not files:
        print("No charge report files found.")
        return

    client = OpenAI(api_key=LLM_API_KEY, base_url=LLM_BASE_URL)

    out_rows = []
    issues = []
    for doc in files:
        text = doc.read_text(encoding="utf-8")
        try:
            ext = call_llm(text, client)
            if not isinstance(ext, dict):
                raise ValueError("LLM output is not a JSON object")
            if "charge" not in ext:
                ext["charge"] = doc.stem
            rows = to_csv_rows(ext)
            out_rows.extend(rows)
        except Exception as e:
            issues.append(f"{doc.name}: {e}")
            print(f"Error processing {doc.name}: {e}", file=sys.stderr)

    header = ["case_no", "charge_no", "entity_name", "entity_type", "entity_age", "entity_gender", "relationship_to_victim", "offence_group", "special_type"]
    with open(args.output, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=header)
        writer.writeheader()
        writer.writerows(out_rows)

    print(f"Wrote {len(out_rows)} rows to {args.output}")
    if issues:
        print(f"{len(issues)} issues encountered. See stderr for details.")


if __name__ == "__main__":
    main()