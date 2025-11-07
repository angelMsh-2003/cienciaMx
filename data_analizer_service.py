import os
import json
import csv

DATA_FOLDER = "data"
OUTPUT_CSV = "MetaValidatorReport.csv"

# Mandatory fields
MANDATORY_FIELDS = ['dc:title', 'dc:creator', 'dc:rights', 'dc:date', 'dc:type', 'dc:identifier']

def validate_record(record):
    errors = []
    metadata = record.get('metadata', {}).get('oai_dc:dc', {})
    missing_fields = []

    for field in MANDATORY_FIELDS:
        if field not in metadata:
            errors.append(f"Missing {field}")
            missing_fields.append(field)

    # Use dc:identifier if exists, else header identifier
    record_id = metadata.get('dc:identifier', record.get('header', {}).get('identifier', 'no_id'))

    return errors, missing_fields, record_id, metadata.keys()

combined_rows = []

for filename in os.listdir(DATA_FOLDER):
    if not filename.endswith(".json"):
        continue

    file_path = os.path.join(DATA_FOLDER, filename)
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        continue

    records = data.get('OAI-PMH', {}).get('ListRecords', {}).get('record', [])
    total_records = len(records)
    deleted_records = 0
    records_with_errors = 0
    records_complete = 0
    all_fields = set()
    missing_fields_info = []

    for record in records:
        header = record.get('header', {})
        if header.get('@status') == 'deleted':
            deleted_records += 1
            continue

        errors, missing_fields, record_id, fields = validate_record(record)
        all_fields.update(fields)

        if errors:
            records_with_errors += 1
            # Store missing field info per record
            missing_fields_info.append(f"{record_id}: {', '.join(missing_fields)}")
        else:
            records_complete += 1

    combined_rows.append({
        "json_file": filename,
        "total_records": total_records,
        "deleted_records": deleted_records,
        "records_with_errors": records_with_errors,
        "records_complete": records_complete,
        "metadata_fields": ", ".join(sorted(all_fields)),
        "mandatory_fields": " | ".join(missing_fields_info) if missing_fields_info else "All mandatory fields present"
    })

# Write CSV
with open(OUTPUT_CSV, "w", newline="", encoding="utf-8") as f:
    fieldnames = ["json_file","total_records","deleted_records","records_with_errors","records_complete","metadata_fields","mandatory_fields"]
    writer = csv.DictWriter(f, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(combined_rows)

print(f"CSV report generated: {OUTPUT_CSV}")
