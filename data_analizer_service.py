import csv
import json
from pathlib import Path
from typing import Dict, List, Set, Tuple

from src.stats.stats import Stats

BASE_DIR = Path(__file__).resolve().parent
DATA_FOLDER = BASE_DIR / "data"
OUTPUT_CSV = BASE_DIR / "MetaValidatorReport.csv"

# Mandatory fields
MANDATORY_FIELDS = [
    "dc:title",
    "dc:creator",
    "dc:rights",
    "dc:date",
    "dc:type",
    "dc:identifier",
]


# ----------------------------------------------------------------------
# Helpers
# ----------------------------------------------------------------------
def extract_records(payload: Dict) -> List[Dict]:
    """Return the list of records, supporting both plain and namespaced keys."""
    candidates = [
        ("OAI-PMH", "ListRecords", "record"),
        ("ns0:OAI-PMH", "ns0:ListRecords", "ns0:record"),
    ]
    for root_key, list_key, record_key in candidates:
        root = payload.get(root_key)
        if not isinstance(root, dict):
            continue
        list_records = root.get(list_key)
        if not isinstance(list_records, dict):
            continue
        record_list = list_records.get(record_key)
        if isinstance(record_list, list):
            return record_list
        if isinstance(record_list, dict):
            return [record_list]
    return []


def get_header(item: Dict) -> Dict:
    return item.get("header") or item.get("ns0:header") or {}


def get_metadata(item: Dict) -> Dict:
    metadata_block = item.get("metadata") or item.get("ns0:metadata") or {}
    return metadata_block.get("oai_dc:dc") or metadata_block.get("ns2:dc") or {}


def normalize_identifier(value) -> str:
    if isinstance(value, list):
        for item in value:
            if isinstance(item, str) and item.strip():
                return item.strip()
    elif isinstance(value, str) and value.strip():
        return value.strip()
    return "no_id"


def has_metadata_value(value) -> bool:
    if isinstance(value, list):
        return any(isinstance(element, str) and element.strip() for element in value)
    if isinstance(value, str):
        return bool(value.strip())
    return bool(value)


def validate_record(item: Dict) -> Tuple[List[str], List[str], str, Set[str], bool, bool]:
    issues: List[str] = []
    missing: List[str] = []
    metadata = get_metadata(item)

    for field in MANDATORY_FIELDS:
        value = metadata.get(field)
        if not value:
            issues.append(f"Missing {field}")
            missing.append(field)

    item_header = get_header(item)
    identifier = normalize_identifier(
        metadata.get("dc:identifier")
        or item_header.get("identifier")
        or item_header.get("ns0:identifier")
    )

    title_has_value = has_metadata_value(metadata.get("dc:title"))
    description_has_value = has_metadata_value(metadata.get("dc:description"))

    return (
        issues,
        missing,
        identifier,
        set(metadata.keys()),
        title_has_value,
        description_has_value,
    )


def is_deleted(header_block: Dict) -> bool:
    status = (
        header_block.get("@status")
        or header_block.get("status")
        or header_block.get("ns0:status")
    )
    return isinstance(status, str) and status.lower() == "deleted"


def analyze_all_repositories() -> List[Dict]:
    combined_rows: List[Dict] = []

    for json_path in sorted(DATA_FOLDER.glob("*.json")):
        try:
            with json_path.open("r", encoding="utf-8") as f:
                data = json.load(f)
        except (FileNotFoundError, json.JSONDecodeError):
            continue

        records = extract_records(data)
        total_records = len(records)
        deleted_records = 0
        records_with_errors = 0
        records_complete = 0
        all_fields: Set[str] = set()
        missing_fields_info: List[str] = []
        title_present = 0
        description_present = 0

        for record in records:
            header = get_header(record)
            if is_deleted(header):
                deleted_records += 1
                continue

            (
                errors,
                missing_fields,
                record_id,
                fields,
                title_has_value,
                description_has_value,
            ) = validate_record(record)
            all_fields.update(fields)

            if title_has_value:
                title_present += 1
            if description_has_value:
                description_present += 1

            if errors:
                records_with_errors += 1
                missing_fields_info.append(f"{record_id}: {', '.join(missing_fields)}")
            else:
                records_complete += 1

        # Use the Stats helper for additional insights
        stats = Stats(json_path)
        data_loaded = stats.load_data()

        oldest_item = stats.get_oldest_item(data_loaded)
        newest_item = stats.get_newest_item(data_loaded)
        language_summary = stats.get_item_language(data_loaded)

        combined_rows.append(
            {
                "json_file": json_path.name,
                "oldest_item": oldest_item or "",
                "newest_item": newest_item or "",
                "language_summary": json.dumps(
                    language_summary, ensure_ascii=False
                ),
                "total_records": total_records,
                "deleted_records": deleted_records,
                "records_with_errors": records_with_errors,
                "records_complete": records_complete,
                "metadata_fields": ", ".join(sorted(all_fields)),
                "mandatory_fields": " | ".join(missing_fields_info)
                if missing_fields_info
                else "All mandatory fields present",
                "dc:tittle": (
                    f"{title_present}/{total_records}" if total_records else "0/0"
                ),
                "dc:escription": (
                    f"{description_present}/{total_records}"
                    if total_records
                    else "0/0"
                ),
            }
        )

    with OUTPUT_CSV.open("w", newline="", encoding="utf-8") as f:
        fieldnames = [
            "json_file",
            "oldest_item",
            "newest_item",
            "language_summary",
            "total_records",
            "deleted_records",
            "records_with_errors",
            "records_complete",
            "metadata_fields",
            "mandatory_fields",
            "dc:tittle",
            "dc:escription",
        ]
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(combined_rows)

    print(f"CSV report generated successfully: {OUTPUT_CSV}")
    return combined_rows


if __name__ == "__main__":
    analyze_all_repositories()
