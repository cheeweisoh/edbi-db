"""
QA Validation Script for Charge Report Data Extraction
=======================================================
Scans charge report markdown documents, extracts structured fields, validates
data quality/completeness, and produces a CSV output matching the "Info to
Extract" schema. Does NOT require an existing ground-truth CSV.

This supports QA of the data extraction pipeline in the Databricks medallion
framework (bronze → silver layer).

Two extraction modes:
  1. Regex-based (default) — fast, no API cost, catches structural issues
  2. LLM-based (optional)  — uses OpenAI/Azure OpenAI to extract fields via
     prompt, better at inferring Offence Groups and Relationship to Victim

Usage:
  python qa_charge_report_extraction.py                          # extract & validate all
  python qa_charge_report_extraction.py --output-csv extracted.csv  # save extracted data
  python qa_charge_report_extraction.py --mode llm --sample 50   # LLM on 50 charges
  python qa_charge_report_extraction.py --charge DAC-000001-2023 # single charge
"""

import argparse
import csv
import json
import os
import re
import sys
from collections import defaultdict
from pathlib import Path

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
BASE_DIR = Path("/Volumes/edbi_teamg01/landing/statecourts/charge_reports")
CHARGE_REPORTS_DIR = BASE_DIR / "ChargeReports"

# LiteLLM configuration
LLM_API_KEY = os.environ.get("LLM_API_KEY", "")
LLM_BASE_URL = os.environ.get("LLM_BASE_URL", "https://litellm.<dns>/")
LLM_MODEL = os.environ.get("LLM_MODEL", "agc-llmaas-claude-4-5-dev")

# Required fields that every charge report must produce
REQUIRED_FIELDS = ["case", "charge", "accused_name", "accused_age", "accused_gender", "offence_group"]
# Fields that should be present when a victim exists
VICTIM_FIELDS = ["victim_name", "victim_age", "victim_gender"]

# Offence group mapping: statute/section keywords → offence group
OFFENCE_GROUP_MAP = {
    "Road Traffic Act": "Traffic Offences",
    "Misuse of Drugs Act": "Drug Offences",
    "Prevention of Corruption Act": "Corruption",
    "Women's Charter": "Vice Activities",
    "Arms Offences Act": "Weapons Offences",
    "Corrosive and Explosive Substances": "Weapons Offences",
    "Moneylenders Act": "Moneylending Offences",
    "Immigration Act": "Immigration Offences",
    "Computer Misuse Act": "Computer Crimes",
    "Protection from Harassment Act": "Harassment",
    "Massage Establishments Act": "Vice Activities in Massage Parlours",
    "Employment of Foreign Manpower Act": "Immigration Offences",
    "Miscellaneous Offences": "Public Order Offences",
    "Public Order Act": "Public Order Offences",
    "Societies Act": "Public Order Offences",
    "Vandalism Act": "Mischief",
    "Films Act": "Vice Activities",
}

# Penal Code section → offence group
PENAL_CODE_SECTION_MAP = {
    "323": "Hurt", "324": "Hurt", "325": "Hurt", "326": "Hurt",
    "334": "Hurt", "335": "Hurt",
    "379": "Theft", "380": "Theft", "381": "Theft",
    "392": "Robbery", "393": "Robbery", "394": "Robbery",
    "395": "Robbery", "397": "Robbery",
    "384": "Extortion", "385": "Extortion", "386": "Extortion",
    "387": "Extortion", "388": "Extortion", "389": "Extortion",
    "403": "Criminal Misappropriation",
    "406": "Criminal Breach of Trust", "407": "Criminal Breach of Trust",
    "408": "Criminal Breach of Trust", "409": "Criminal Breach of Trust",
    "411": "Receiving Stolen Property",
    "420": "Cheating", "417": "Cheating", "418": "Cheating", "419": "Cheating",
    "427": "Mischief", "426": "Mischief", "425": "Mischief",
    "447": "Housebreaking/Theft", "449": "Housebreaking/Theft",
    "451": "Housebreaking/Theft", "453": "Housebreaking/Theft",
    "454": "Housebreaking/Theft", "456": "Housebreaking/Theft",
    "457": "Housebreaking/Theft", "458": "Housebreaking/Theft",
    "461": "Housebreaking/Theft", "462": "Housebreaking/Theft",
    "463": "Forgery", "465": "Forgery", "468": "Forgery", "471": "Forgery",
    "472": "Forgery", "473": "Forgery", "474": "Forgery",
    "376": "Sexual Assault", "375": "Sexual Assault",
    "354": "Outrage of Modesty",
    "377BB": "Voyeurism/Sexual Exposure", "377BD": "Voyeurism/Sexual Exposure",
    "377BF": "Voyeurism/Sexual Exposure",
    "504": "Criminal Intimidation", "506": "Criminal Intimidation",
    "507": "Criminal Intimidation", "503": "Criminal Intimidation",
    "267": "Public Order Offences", "268": "Public Order Offences",
    "143": "Public Order Offences", "147": "Public Order Offences",
    "148": "Public Order Offences", "151": "Public Order Offences",
    "186": "Public Order Offences", "353": "Public Order Offences",
    "363": "Kidnapping", "364": "Kidnapping", "365": "Kidnapping",
    "312": "Hurt", "313": "Hurt", "314": "Hurt",
    "304": "Hurt", "304A": "Hurt",
    "509": "Outrage of Modesty",
    "489A": "Forgery", "489B": "Forgery", "489C": "Forgery",
    "477A": "Forgery",
    "182": "Public Order Offences", "204": "Public Order Offences",
    "193": "Public Order Offences", "228": "Public Order Offences",
}


# ---------------------------------------------------------------------------
# Regex-based extraction
# ---------------------------------------------------------------------------
def parse_charge_report(filepath: Path) -> dict:
    """Parse a charge report markdown file and extract structured fields."""
    text = filepath.read_text(encoding="utf-8")
    result = {
        "charge": filepath.stem,
        "case": None,
        "accused_name": None,
        "accused_age": None,
        "accused_gender": None,
        "accused_relationship": None,
        "victim_name": None,
        "victim_age": None,
        "victim_gender": None,
        "victim_relationship": None,
        "offence_group": None,
        "special_type": None,
        "statute": None,
        "offence_section": None,
    }

    # Case & Charge from "Charge Details" line
    m = re.search(r"Charge Details.*?\((SC-\d+-\d+)\s*/\s*(\S+)\)", text)
    if m:
        result["case"] = m.group(1)

    # Accused name
    m = re.search(r"\*\*Name:\*\*\s*(.+)", text)
    if m:
        result["accused_name"] = m.group(1).strip()

    # Accused gender/age
    m = re.search(r"\*\*Gender/Age:\*\*\s*(MALE|FEMALE)\s*/\s*(\d+)\s*YEARS", text)
    if m:
        result["accused_gender"] = "M" if m.group(1) == "MALE" else "F"
        result["accused_age"] = int(m.group(2))

    # Statute
    m = re.search(r"\*\*Statute:\*\*\s*(.+)", text)
    if m:
        result["statute"] = m.group(1).strip()

    # Offence section
    m = re.search(r"\*\*Offence:\*\*\s*Section\s+([\w()]+)", text)
    if m:
        result["offence_section"] = m.group(1)

    # Statement of offence (full text block)
    stmt_match = re.search(
        r"### \*\*Statement of Offence\*\*\s*\n(.+?)(?=\n\*\s\*\*Offence:|\Z)",
        text, re.DOTALL,
    )
    statement_text = stmt_match.group(1).strip() if stmt_match else None

    # Special Type
    m = re.search(r"\*\*Special Type:\*\*\s*(.+)", text)
    if m:
        result["special_type"] = m.group(1).strip()

    # Victim section
    victim_section = re.search(
        r"###\s*\*\*Victim Particulars\*\*(.+?)(?=###|\Z)", text, re.DOTALL
    )
    if victim_section:
        vs = victim_section.group(1)
        vm = re.search(r"\*\*Name:\*\*\s*(.+)", vs)
        if vm:
            result["victim_name"] = vm.group(1).strip()
        vm = re.search(r"\*\*Gender/Age:\*\*\s*(MALE|FEMALE)\s*/\s*(\d+)\s*YEARS", vs)
        if vm:
            result["victim_gender"] = "M" if vm.group(1) == "MALE" else "F"
            result["victim_age"] = int(vm.group(2))
        vm = re.search(r"\*\*Relationship to Accused:\*\*\s*(.+)", vs)
        if vm:
            result["victim_relationship"] = vm.group(1).strip()

    # Derive offence group
    result["offence_group"] = derive_offence_group(
        result["statute"], result["offence_section"], statement_text
    )

    return result


def derive_offence_group(
    statute: str | None, section: str | None, statement: str | None = None
) -> str | None:
    """Map statute/section to an offence group using keyword rules."""
    if statute:
        for keyword, group in OFFENCE_GROUP_MAP.items():
            if keyword.lower() in statute.lower():
                # Distinguish massage parlour vice from general vice
                if group == "Vice Activities" and statement:
                    if "massage" in statement.lower():
                        return "Vice Activities in Massage Parlours"
                return group
    if section and statute and "penal code" in statute.lower():
        # Strip parenthetical parts: "377BB(7)" → "377BB"
        base = re.sub(r"\(.*\)", "", section)
        if base in PENAL_CODE_SECTION_MAP:
            return PENAL_CODE_SECTION_MAP[base]
    return None


# ---------------------------------------------------------------------------
# LLM-based extraction (optional)
# ---------------------------------------------------------------------------
LLM_EXTRACTION_PROMPT = """\
You are a legal data extraction assistant. Given a Singapore charge report document,
extract the following fields as JSON. Return ONLY valid JSON, no explanation.

IMPORTANT: Only extract victim information if there is an explicit "Victim Particulars"
section in the document. Do NOT infer a victim from the Statement of Offence or other
sections. If there is no "Victim Particulars" section, set all victim fields to null.

Fields to extract:
- case: The SC case number (e.g. SC-000001-2023). If blank/missing, set to null.
- charge: The charge number (e.g. DAC-000001-2023)
- accused_name: Full name of the accused person (title case)
- accused_age: Age as integer
- accused_gender: "M" or "F"
- victim_name: Full name of victim from "Victim Particulars" section (title case), or null if no such section
- victim_age: Victim age as integer from "Victim Particulars" section, or null
- victim_gender: "M" or "F" from "Victim Particulars" section, or null
- victim_relationship_to_accused: From "Relationship to Accused" in Victim Particulars (e.g. "Self"), or null
- accused_relationship_to_victim: The accused's relationship/role relative to the victim
  (e.g. "Coach", "Other Driver", "Neighbour", "Stranger", "Parent", etc.), or empty string
- offence_group: Classify into one of these categories:
  Traffic Offences, Drug Offences, Corruption, Theft, Hurt, Robbery, Cheating,
  Mischief, Forgery, Sexual Assault, Outrage of Modesty, Voyeurism/Sexual Exposure,
  Criminal Breach of Trust, Criminal Intimidation, Criminal Misappropriation,
  Housebreaking/Theft, Public Order Offences, Harassment, Moneylending Offences,
  Immigration Offences, Weapons Offences, Computer Crimes, Vice Activities,
  Vice Activities in Massage Parlours, Receiving Stolen Property, Extortion, Kidnapping
- special_type: One of "Offences involving Vulnerable Victims", "Family Violence",
  "LT1 Offences", or empty string

Charge Report:
{document}
"""


def extract_with_llm(filepath: Path, client) -> dict:
    """Use LiteLLM-compatible API to extract fields from a charge report."""
    text = filepath.read_text(encoding="utf-8")
    response = client.chat.completions.create(
        model=LLM_MODEL,
        messages=[
            {"role": "user", "content": LLM_EXTRACTION_PROMPT.format(document=text)}
        ],
        temperature=0,
    )
    # Parse JSON from response (handle both raw JSON and markdown-wrapped JSON)
    content = response.choices[0].message.content
    # Strip markdown code fences if present
    if "```json" in content:
        content = content.split("```json")[1].split("```")[0]
    elif "```" in content:
        content = content.split("```")[1].split("```")[0]
    return json.loads(content.strip())



# ---------------------------------------------------------------------------
# Validation (no ground truth needed)
# ---------------------------------------------------------------------------
def validate_extracted(extracted: dict) -> list[dict]:
    """Validate completeness and data quality of extracted fields.
    Returns a list of issue dicts."""
    issues = []
    charge = extracted.get("charge", "UNKNOWN")

    # Required field checks
    for field in REQUIRED_FIELDS:
        val = extracted.get(field)
        if val is None or (isinstance(val, str) and val.strip() == ""):
            issues.append({
                "charge": charge,
                "field": field,
                "severity": "ERROR",
                "message": f"Missing required field: {field}",
            })

    # Format checks
    case = extracted.get("case")
    if case and not re.match(r"^SC-\d{6,7}-\d{4}$", case):
        issues.append({
            "charge": charge,
            "field": "case",
            "severity": "WARNING",
            "message": f"Case number format unexpected: {case}",
        })

    charge_val = extracted.get("charge", "")
    if not re.match(r"^(DAC|MAC|MCN)-\d{6}-\d{4}$", charge_val):
        issues.append({
            "charge": charge,
            "field": "charge",
            "severity": "WARNING",
            "message": f"Charge number format unexpected: {charge_val}",
        })

    age = extracted.get("accused_age")
    if age is not None and (not isinstance(age, int) or age < 7 or age > 120):
        issues.append({
            "charge": charge,
            "field": "accused_age",
            "severity": "WARNING",
            "message": f"Accused age looks unusual: {age}",
        })

    gender = extracted.get("accused_gender")
    if gender and gender not in ("M", "F"):
        issues.append({
            "charge": charge,
            "field": "accused_gender",
            "severity": "ERROR",
            "message": f"Invalid gender value: {gender}",
        })

    # Victim field consistency
    has_victim = extracted.get("victim_name") is not None
    if has_victim:
        for field in VICTIM_FIELDS:
            val = extracted.get(field)
            if val is None or (isinstance(val, str) and val.strip() == ""):
                issues.append({
                    "charge": charge,
                    "field": field,
                    "severity": "ERROR",
                    "message": f"Victim exists but missing field: {field}",
                })
        v_age = extracted.get("victim_age")
        if v_age is not None and (not isinstance(v_age, int) or v_age < 0 or v_age > 120):
            issues.append({
                "charge": charge,
                "field": "victim_age",
                "severity": "WARNING",
                "message": f"Victim age looks unusual: {v_age}",
            })
        v_gender = extracted.get("victim_gender")
        if v_gender and v_gender not in ("M", "F"):
            issues.append({
                "charge": charge,
                "field": "victim_gender",
                "severity": "ERROR",
                "message": f"Invalid victim gender value: {v_gender}",
            })

    return issues


def extracted_to_csv_rows(extracted: dict) -> list[dict]:
    """Convert extracted data to CSV rows in 'Info to Extract' format.
    Each charge produces 1 row for the accused and optionally 1 for the victim."""
    rows = []
    charge = extracted.get("charge", "")
    case = extracted.get("case", "")

    # Accused row
    rows.append({
        "Case": case,
        "Charge": charge,
        "Entity": (extracted.get("accused_name") or "").title(),
        "Entity Type": "Accused Person",
        "Age": extracted.get("accused_age", ""),
        "Gender": extracted.get("accused_gender", ""),
        "Relationship to Victim": extracted.get("accused_relationship", ""),
        "Offence Groups": extracted.get("offence_group", ""),
        "Special Type": extracted.get("special_type", ""),
    })

    # Victim row (if present)
    if extracted.get("victim_name"):
        rows.append({
            "Case": case,
            "Charge": charge,
            "Entity": (extracted.get("victim_name") or "").title(),
            "Entity Type": "Victim",
            "Age": extracted.get("victim_age", ""),
            "Gender": extracted.get("victim_gender", ""),
            "Relationship to Victim": "Self",
            "Offence Groups": extracted.get("offence_group", ""),
            "Special Type": extracted.get("special_type", ""),
        })

    return rows


# ---------------------------------------------------------------------------
# Report generation
# ---------------------------------------------------------------------------
def generate_report(
    all_issues: list[dict],
    total_reports: int,
    checked_reports: int,
    extraction_failures: list[str],
    total_csv_rows: int,
) -> str:
    """Generate a human-readable QA summary report."""
    error_issues = [i for i in all_issues if i["severity"] == "ERROR"]
    warning_issues = [i for i in all_issues if i["severity"] == "WARNING"]
    charges_with_issues = len(set(i["charge"] for i in all_issues))

    lines = []
    lines.append("=" * 70)
    lines.append("  CHARGE REPORT EXTRACTION QA REPORT")
    lines.append("=" * 70)
    lines.append(f"Total report files found: {total_reports}")
    lines.append(f"Reports processed:        {checked_reports}")
    lines.append(f"Extraction failures:      {len(extraction_failures)}")
    lines.append(f"Total CSV rows generated: {total_csv_rows}")
    lines.append(f"Reports with issues:      {charges_with_issues}")
    lines.append(f"  Errors:                 {len(error_issues)}")
    lines.append(f"  Warnings:               {len(warning_issues)}")
    lines.append("")

    # Issue summary by field
    field_counts = defaultdict(lambda: {"ERROR": 0, "WARNING": 0})
    for i in all_issues:
        field_counts[i["field"]][i["severity"]] += 1
    if field_counts:
        lines.append("--- Issue Summary by Field ---")
        lines.append(f"  {'Field':<25s}  {'Errors':>7s}  {'Warnings':>8s}")
        lines.append(f"  {'-'*25}  {'-'*7}  {'-'*8}")
        for field, counts in sorted(field_counts.items(), key=lambda x: -(x[1]["ERROR"] + x[1]["WARNING"])):
            lines.append(f"  {field:<25s}  {counts['ERROR']:>7d}  {counts['WARNING']:>8d}")
        lines.append("")

    # Extraction failures
    if extraction_failures:
        lines.append("--- Extraction Failures (first 20) ---")
        for ef in extraction_failures[:20]:
            lines.append(f"  {ef}")
        if len(extraction_failures) > 20:
            lines.append(f"  ... and {len(extraction_failures) - 20} more")
        lines.append("")

    # Detailed issues
    if all_issues:
        lines.append("--- Detailed Issues ---")
        for issue in all_issues:
            lines.append(f"  [{issue['severity']}] {issue['charge']} — {issue['message']}")

    if not all_issues and not extraction_failures:
        lines.append("All reports passed validation. No issues found.")

    lines.append("\n" + "=" * 70)
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(
        description="Extract & validate data from charge report documents"
    )
    parser.add_argument(
        "--mode", choices=["regex", "llm"], default="regex",
        help="Extraction mode: regex (default) or llm",
    )
    parser.add_argument(
        "--sample", type=int, default=0,
        help="If > 0, only process this many reports (random sample)",
    )
    parser.add_argument(
        "--charge", type=str, default=None,
        help="Process a single charge (e.g. DAC-000001-2023)",
    )
    parser.add_argument(
        "--output", type=str, default=None,
        help="Write QA report to file (default: stdout)",
    )
    parser.add_argument(
        "--output-csv", type=str, default=None,
        help="Write extracted data to CSV (Info to Extract format)",
    )
    parser.add_argument(
        "--issues-csv", type=str, default=None,
        help="Write validation issues to CSV for analysis",
    )
    args = parser.parse_args()

    # Discover charge report files
    if args.charge:
        report_file = CHARGE_REPORTS_DIR / f"{args.charge}.md"
        if not report_file.exists():
            print(f"ERROR: Report file not found: {report_file}", file=sys.stderr)
            sys.exit(1)
        report_files = [report_file]
    else:
        report_files = sorted(CHARGE_REPORTS_DIR.glob("*.md"))

    total_reports = len(report_files)

    # Sample
    if args.sample > 0 and not args.charge:
        import random
        random.seed(42)
        report_files = random.sample(report_files, min(args.sample, len(report_files)))

    # Optional LLM client
    llm_client = None
    if args.mode == "llm":
        try:
            from openai import OpenAI
            llm_client = OpenAI(
                api_key=LLM_API_KEY,
                base_url=LLM_BASE_URL,
            )
            print(f"Using LLM mode with model: {LLM_MODEL}")
            print(f"Endpoint: {LLM_BASE_URL}")
        except ImportError:
            print("ERROR: openai package not installed. Run: pip install openai", file=sys.stderr)
            sys.exit(1)

    # Run extraction & validation
    all_issues = []
    all_csv_rows = []
    extraction_failures = []
    checked = 0

    for i, report_file in enumerate(report_files):
        try:
            if args.mode == "llm" and llm_client:
                extracted = extract_with_llm(report_file, llm_client)
                extracted.setdefault("charge", report_file.stem)
            else:
                extracted = parse_charge_report(report_file)

            # Validate
            issues = validate_extracted(extracted)
            all_issues.extend(issues)

            # Convert to CSV rows
            csv_rows = extracted_to_csv_rows(extracted)
            all_csv_rows.extend(csv_rows)

            checked += 1

            if (i + 1) % 500 == 0:
                print(f"  Processed {i + 1}/{len(report_files)} reports...", file=sys.stderr)

        except Exception as e:
            extraction_failures.append(f"{report_file.stem}: {e}")
            checked += 1

    # Generate QA report
    report = generate_report(
        all_issues, total_reports, checked, extraction_failures, len(all_csv_rows)
    )

    if args.output:
        Path(args.output).write_text(report, encoding="utf-8")
        print(f"QA report written to {args.output}")
    else:
        print(report)

    # Write extracted data CSV
    if args.output_csv:
        csv_header = ["Case", "Charge", "Entity", "Entity Type", "Age", "Gender",
                       "Relationship to Victim", "Offence Groups", "Special Type"]
        with open(args.output_csv, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=csv_header)
            writer.writeheader()
            writer.writerows(all_csv_rows)
        print(f"Extracted data CSV written to {args.output_csv} ({len(all_csv_rows)} rows)")

    # Write issues CSV
    if args.issues_csv:
        with open(args.issues_csv, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(["Charge", "Field", "Severity", "Message"])
            for issue in all_issues:
                writer.writerow([
                    issue["charge"], issue["field"],
                    issue["severity"], issue["message"],
                ])
        print(f"Issues CSV written to {args.issues_csv}")

    # Exit code: 1 if there are errors
    has_errors = any(i["severity"] == "ERROR" for i in all_issues) or extraction_failures
    sys.exit(1 if has_errors else 0)


if __name__ == "__main__":
    main()
