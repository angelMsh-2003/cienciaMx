import requests
import xml.etree.ElementTree as ET
import pandas as pd
import json
from pathlib import Path
from time import sleep

# ==========================================================
# CONFIGURATION
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
    "dc:date"
}

FIELDS_OBLIGATORY_WHEN_APPLICABLE = {
    "dc:source",
    "dc:coverage",
    "dc:relation",
    "dc:contributor",
    "dc:language"
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

# ==========================================================
# Metadata extraction
# ==========================================================
def extract_metadata_fields(record):
    """
    Extract metadata fields from one record.
    - Single values: stored as string or dict
    - Multiple values: stored as list
    - dc:rights is ALWAYS a list of dictionaries
    """
    # Temporary storage with lists
    temp_metadata = {
        "obligatory": {},
        "obligatory_when_applicable": {},
        "recommended": {},
        "optional": {},
        "others": {}
    }

    access_levels = ["closedAccess", "openAccess", "restrictedAccess", "embargoedAccess"]

    for element in record.findall(".//dc:*", NAMESPACES):
        tag = element.tag.split("}", 1)[1]
        value = element.text.strip() if element.text else ""
        attrs = element.attrib
        key = f"dc:{tag}"

        # Determine category
        if key in FIELDS_OBLIGATORY:
            category = "obligatory"
        elif key in FIELDS_OBLIGATORY_WHEN_APPLICABLE:
            category = "obligatory_when_applicable"
        elif key in FIELDS_RECOMMENDED:
            category = "recommended"
        elif key.startswith("dc:"):
            category = "optional"
        else:
            category = "others"

        # Special handling by field type
        if tag in ["creator", "contributor"]:
            # Dictionary format
            entry = {"name": value}
            # Extraer TODOS los atributos sin filtrar
            for attr_key, attr_value in attrs.items():
                # Limpiar el namespace del nombre del atributo si existe
                clean_key = attr_key.split("}", 1)[-1] if "}" in attr_key else attr_key
                entry[clean_key] = attr_value
            
            # Validar si el ID es real o es un placeholder vacío
            if "id" in entry:
                id_value = entry["id"].lower()
                # Detectar placeholders vacíos
                invalid_patterns = [
                    "sin_identificador",
                    "sin identificador", 
                    "without_identifier",
                    "no_identifier",
                    "unknown",
                    "n/a",
                    "null"
                ]
                if any(pattern in id_value for pattern in invalid_patterns):
                    entry["id"] = "unknown"
            else:
                entry["id"] = "unknown"

        elif tag == "rights":
            # Dictionary format with access_level and licence_condition
            rights_entry = {}
            
            # Check value for access level and licence
            if value:
                for level in access_levels:
                    if level.lower() in value.lower():
                        rights_entry["access_level"] = level
                        break
                if value.startswith("http"):
                    rights_entry["licence_condition"] = value
            
            # Check attributes (limpiando namespaces)
            for attr_key, attr_value in attrs.items():
                # Limpiar namespace del atributo
                clean_key = attr_key.split("}", 1)[-1] if "}" in attr_key else attr_key
                
                for level in access_levels:
                    if level.lower() in attr_value.lower():
                        rights_entry["access_level"] = level
                        break
                if attr_value.startswith("http"):
                    rights_entry["licence_condition"] = attr_value
            
            entry = rights_entry

        else:
            # Regular string value
            entry = value

        # Add to temp storage (always as list)
        if key not in temp_metadata[category]:
            temp_metadata[category][key] = []
        temp_metadata[category][key].append(entry)

    # Convert to final format: single items as direct values, multiple as lists
    # EXCEPTION: dc:rights is ALWAYS a list
    metadata = {
        "obligatory": {},
        "obligatory_when_applicable": {},
        "recommended": {},
        "optional": {},
        "others": {}
    }

    for category in temp_metadata:
        for key, values in temp_metadata[category].items():
            if key == "dc:rights":
                # dc:rights is ALWAYS a list
                metadata[category][key] = values
            elif len(values) == 1:
                # Single value: store directly
                metadata[category][key] = values[0]
            else:
                # Multiple values: keep as list
                metadata[category][key] = values

    return metadata

# ==========================================================
# Repository analysis
# ==========================================================
def analyze_repository(name, oai_url, max_records=None):
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

    record_count = 0
    complete_obligatory = 0
    missing_records = []
    
    # Track all metadata fields found across all records
    all_fields_found = {
        "obligatory": set(),
        "obligatory_when_applicable": set(),
        "recommended": set(),
        "optional": set(),
        "others": set()
    }
    
    # NUEVO: Capturar TODOS los campos directamente del XML
    all_metadata_fields_raw = set()

    for rec in records:
        header = rec.find(".//oai:header", NAMESPACES)
        meta_block = extract_metadata_fields(rec)

        # Collect all fields found in this record (para clasificación)
        for category in all_fields_found:
            all_fields_found[category].update(meta_block[category].keys())
        
        # NUEVO: Capturar TODOS los elementos dc:* directamente del XML
        for element in rec.findall(".//dc:*", NAMESPACES):
            tag = element.tag.split("}", 1)[1]
            all_metadata_fields_raw.add(f"dc:{tag}")

        record_count += 1
        missing_fields = sorted(FIELDS_OBLIGATORY - meta_block["obligatory"].keys())
        
        # Validación adicional: verificar si dc:creator o dc:contributor tienen id válido
        validation_failed = False
        
        # Verificar dc:creator
        if "dc:creator" in meta_block["obligatory"]:
            creator = meta_block["obligatory"]["dc:creator"]
            # Si es un dict (un solo creator)
            if isinstance(creator, dict) and creator.get("id") == "unknown":
                validation_failed = True
                if "dc:creator (id missing)" not in missing_fields:
                    missing_fields.append("dc:creator (id missing)")
            # Si es una lista (múltiples creators)
            elif isinstance(creator, list):
                if all(c.get("id") == "unknown" for c in creator if isinstance(c, dict)):
                    validation_failed = True
                    if "dc:creator (id missing)" not in missing_fields:
                        missing_fields.append("dc:creator (id missing)")
        
        # Verificar dc:contributor si existe
        if "dc:contributor" in meta_block["obligatory_when_applicable"]:
            contributor = meta_block["obligatory_when_applicable"]["dc:contributor"]
            # Si es un dict
            if isinstance(contributor, dict) and contributor.get("id") == "unknown":
                validation_failed = True
                if "dc:contributor (id missing)" not in missing_fields:
                    missing_fields.append("dc:contributor (id missing)")
            # Si es una lista
            elif isinstance(contributor, list):
                if all(c.get("id") == "unknown" for c in contributor if isinstance(c, dict)):
                    validation_failed = True
                    if "dc:contributor (id missing)" not in missing_fields:
                        missing_fields.append("dc:contributor (id missing)")
        
        missing_fields = sorted(missing_fields)
        
        if not missing_fields and not validation_failed:
            complete_obligatory += 1
        else:
            missing_records.append({
                "identifier": header.findtext("oai:identifier", default="", namespaces=NAMESPACES),
                "missing_fields": missing_fields
            })

        record_data = {
            "identifier": header.findtext("oai:identifier", default="", namespaces=NAMESPACES),
            "datestamp": header.findtext("oai:datestamp", default="", namespaces=NAMESPACES),
            "metadata": meta_block
        }

        repo_data["records"].append(record_data)

    # Save JSON
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    repo_filename = OUTPUT_DIR / f"{name.replace(' ', '_').lower()}_items.json"
    with open(repo_filename, "w", encoding="utf-8") as f:
        json.dump(repo_data, f, ensure_ascii=False, indent=4)

    # Build metadata fields catalog - CAPTURA TODOS LOS CAMPOS
    fields_catalog = {
        "repository_name": name,
        "url": oai_url,
        "total_records_analyzed": record_count,
        "metadata_fields_found": sorted(list(all_metadata_fields_raw)),  # TODOS los campos del XML
        "obligatory_fields_status": {
            "required": sorted(list(FIELDS_OBLIGATORY)),
            "found": sorted(list(all_fields_found["obligatory"])),
            "missing": sorted(list(FIELDS_OBLIGATORY - all_fields_found["obligatory"]))
        }
    }
    
    fields_filename = OUTPUT_DIR / f"{name.replace(' ', '_').lower()}_metadata_fields.json"
    with open(fields_filename, "w", encoding="utf-8") as f:
        json.dump(fields_catalog, f, ensure_ascii=False, indent=4)

    # Build summary
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
    print(f" - Metadata fields catalog saved: {fields_filename}")
    print(f" - Summary saved: {summary_filename}")
    print(f" - Records with all obligatory fields: {complete_obligatory}/{record_count}")

    return summary_data

# ==========================================================
# Execution modes
# ==========================================================
def run_test_mode():
    TEST_NAME = "CIDE Digital Repository"
    TEST_URL = "https://cide.repositorioinstitucional.mx/oai/request"
    analyze_repository(TEST_NAME, TEST_URL, max_records=MAX_RECORDS)

def run_full_mode():
    df = pd.read_csv(INPUT_FILE)
    for index, row in df.iterrows():
        name = row.get("NAME") or row.get("name") or row.get("nombre") or "Unknown"
        oai_url = row.get("oai_url") or row.get("link")
        if not isinstance(oai_url, str) or not oai_url.startswith("http"):
            continue
        print(f"[{index+1}/{len(df)}] Processing: {name}")
        analyze_repository(name.strip(), oai_url.strip(), max_records=None)
        sleep(SLEEP_BETWEEN_REQUESTS)

# ==========================================================
# MAIN
# ==========================================================
if __name__ == "__main__":
    # run_test_mode()   # TEST mode
    run_full_mode()  # FULL mode