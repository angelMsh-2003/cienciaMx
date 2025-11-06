import requests
import xml.etree.ElementTree as ET
import pandas as pd
import json
from pathlib import Path
from time import sleep

# ==========================================================
# Configuration
# ==========================================================
INPUT_FILE = Path("csv/dataversion5.0.csv")
OUTPUT_DIR = Path("json/repositories")

NAMESPACES = {
    "oai": "http://www.openarchives.org/OAI/2.0/",
    "dc": "http://purl.org/dc/elements/1.1/",
    "oai_dc": "http://www.openarchives.org/OAI/2.0/oai_dc/"
}

TIMEOUT = 60
SLEEP_BETWEEN_REQUESTS = 2
MAX_RECORDS = 100  # For test mode (None = all records)

# ==========================================================
# Metadata field categories
# ==========================================================
FIELDS_OBLIGATORY = {
    "dc:title",
    "dc:creator",
    "dc:identifier",
    "dc:type",
    "dc:rights",
    "dc:date",
    "dc:publisher",
    "dc:publicationYear",
    "dc:relatedIdentifier"
}

FIELDS_OBLIGATORY_WHEN_APPLICABLE = {
    "dc:source",
    "dc:coverage",
    "dc:relation",
    "dc:contributor"
}

FIELDS_RECOMMENDED = {
    "dc:subject",
    "dc:description",
    "dc:language",
    "dc:format"
}

# ==========================================================
# OAI-PMH helper functions
# ==========================================================
def get_records(oai_url, max_records=None):
    """Fetch records from an OAI-PMH endpoint using resumptionToken if available."""
    records = []
    params = {"verb": "ListRecords", "metadataPrefix": "oai_dc"}

    while True:
        try:
            response = requests.get(oai_url, params=params, timeout=TIMEOUT)
            response.raise_for_status()
            root = ET.fromstring(response.text)

            batch = root.findall(".//oai:record", NAMESPACES)
            records.extend(batch)

            token_element = root.find(".//oai:resumptionToken", NAMESPACES)
            if token_element is not None and token_element.text:
                params = {"verb": "ListRecords", "resumptionToken": token_element.text}
                if max_records and len(records) >= max_records:
                    break
                sleep(SLEEP_BETWEEN_REQUESTS)
            else:
                break

        except Exception as e:
            print(f"Error accessing {oai_url}: {e}")
            break

    return records


def extract_metadata_fields(record):
    """Extract metadata fields from one record as dictionary."""
    metadata = {}
    for element in record.findall(".//dc:*", NAMESPACES):
        tag = element.tag.split("}", 1)[1]
        value = element.text.strip() if element.text else ""
        metadata.setdefault(f"dc:{tag}", []).append(value)
    return metadata


# ==========================================================
# Repository analysis
# ==========================================================
def analyze_repository(name, oai_url, max_records=None):
    """Extract and classify all metadata for a repository."""
    print(f"\nAnalyzing repository: {name}")
    records = get_records(oai_url, max_records=max_records)
    if not records:
        print(f" - No records retrieved or connection failed for: {name}")
        return None

    repo_data = {
        "repository_name": name,
        "url": oai_url,
        "records": []
    }

    all_detected_fields = set()
    record_count = 0
    complete_obligatory = 0
    missing_records = []

    for rec in records:
        header = rec.find(".//oai:header", NAMESPACES)
        meta_block = extract_metadata_fields(rec)

        # Categorize fields
        obligatory = {k: v for k, v in meta_block.items() if k in FIELDS_OBLIGATORY}
        obligatory_when_applicable = {k: v for k, v in meta_block.items() if k in FIELDS_OBLIGATORY_WHEN_APPLICABLE}
        recommended = {k: v for k, v in meta_block.items() if k in FIELDS_RECOMMENDED}
        optional = {
            k: v for k, v in meta_block.items()
            if k not in FIELDS_OBLIGATORY
            and k not in FIELDS_OBLIGATORY_WHEN_APPLICABLE
            and k not in FIELDS_RECOMMENDED
            and k.startswith("dc:")
        }
        others = {
            k: v for k, v in meta_block.items() if not k.startswith("dc:")
        }

        all_detected_fields.update(meta_block.keys())
        record_count += 1

        missing_fields = sorted(FIELDS_OBLIGATORY - meta_block.keys())
        if not missing_fields:
            complete_obligatory += 1
        else:
            missing_records.append({
                "identifier": header.findtext("oai:identifier", default="", namespaces=NAMESPACES),
                "missing_fields": missing_fields
            })

        record_data = {
            "identifier": header.findtext("oai:identifier", default="", namespaces=NAMESPACES),
            "datestamp": header.findtext("oai:datestamp", default="", namespaces=NAMESPACES),
            "metadata": {
                "obligatory": obligatory,
                "obligatory_when_applicable": obligatory_when_applicable,
                "recommended": recommended,
                "optional": optional,
                "others": others
            }
        }

        repo_data["records"].append(record_data)

    # Save records JSON
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    repo_filename = OUTPUT_DIR / f"{name.replace(' ', '_').lower()}_items.json"
    with open(repo_filename, "w", encoding="utf-8") as f:
        json.dump(repo_data, f, ensure_ascii=False, indent=4)

    # Build repository summary
    summary_data = {
        "repository_name": name,
        "total_records": record_count,
        "records_with_all_obligatory": complete_obligatory,
        "records_missing_obligatory": len(missing_records),
        "has_all_obligatory": len(missing_records) == 0,
        "missing_records": missing_records
    }

    summary_filename = OUTPUT_DIR / f"{name.replace(' ', '_').lower()}_summary.json"
    with open(summary_filename, "w", encoding="utf-8") as f:
        json.dump(summary_data, f, ensure_ascii=False, indent=4)

    print(f" - Saved: {repo_filename}")
    print(f" - Summary saved: {summary_filename}")
    print(f" - Records with all obligatory fields: {complete_obligatory}/{record_count}")

    return summary_data


# ==========================================================
# Execution modes
# ==========================================================
def run_test_mode():
    """Run test mode: analyze one repository only."""
    TEST_NAME = "Repositorio Digital CIDE"
    TEST_URL = "https://cide.repositorioinstitucional.mx/oai/request"
    analyze_repository(TEST_NAME, TEST_URL, max_records=MAX_RECORDS)


def run_full_mode():
    """Run full mode: analyze all repositories in CSV."""
    df = pd.read_csv(INPUT_FILE)
    for index, row in df.iterrows():
        name = row.get("NAME") or row.get("name") or row.get("nombre") or "Unknown"
        oai_url = row.get("oai_url") or row.get("link")
        if not isinstance(oai_url, str) or not oai_url.startswith("http"):
            continue
        print(f"[{index+1}/{len(df)}] Processing: {name}")
        analyze_repository(name.strip(), oai_url.strip(), max_records=None)
        sleep(SLEEP_BETWEEN_REQUESTS)


# MAIN

if __name__ == "__main__":
  
    run_test_mode()   # <-- TEST mode (single repository)
    # run_full_mode()  # <-- FULL mode (all repositories from CSV)
