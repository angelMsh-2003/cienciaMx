import os
import json
import re

INPUT_DIR = "data/output/data"
OUTPUT_DIR = "data/output/normalized"
os.makedirs(OUTPUT_DIR, exist_ok=True)

def safe_get(obj, key, default=None):
    """Safely get a key from a dict."""
    if isinstance(obj, dict):
        return obj.get(key, default)
    return default

def ensure_list(value):
    """Ensure output is always a list."""
    if value is None:
        return []
    if isinstance(value, list):
        return value
    return [value]

def normalize_rights(rights_list):
    """
    Normalize rights values from 'info:eu-repo/semantics/openAccess' 
    to just 'openAccess', etc.
    """
    normalized = []
    valid_rights = {
        'openaccess': 'openAccess',
        'embargoedaccess': 'embargoedAccess',
        'restrictedaccess': 'restrictedAccess',
        'closedaccess': 'closedAccess'
    }
    
    for right in rights_list:
        if not right or not isinstance(right, str):
            continue
        
        # Clean the string
        clean_right = right.strip()
        
        # Check if it's already a valid value (exact match)
        if clean_right in valid_rights.values():
            normalized.append(clean_right)
            continue
        
        # Try to find any of the valid rights in the string (case-insensitive)
        found = False
        for key, value in valid_rights.items():
            if key in clean_right.lower().replace('-', '').replace('_', ''):
                normalized.append(value)
                found = True
                break
        
        if not found:
            # If no match found, keep the original value
            normalized.append(clean_right)
    
    # Remove duplicates while preserving order
    seen = set()
    result = []
    for item in normalized:
        if item not in seen:
            seen.add(item)
            result.append(item)
    
    return result if result else None

def normalize_record(record, repository_name):
    """Extract and normalize a single OAI-PMH record safely."""
    header = safe_get(record, "header", {})
    metadata = safe_get(record, "metadata", {})
    dc = safe_get(metadata, "oai_dc:dc", {})
    
    # Extract metadata fields safely
    title = safe_get(dc, "dc:title")
    creator = safe_get(dc, "dc:creator")
    description = safe_get(dc, "dc:description")
    subject = ensure_list(safe_get(dc, "dc:subject"))
    rights = ensure_list(safe_get(dc, "dc:rights"))
    fmt = ensure_list(safe_get(dc, "dc:format"))
    type_ = safe_get(dc, "dc:type")
    lang = safe_get(dc, "dc:language")
    date = ensure_list(safe_get(dc, "dc:date"))
    identifier = ensure_list(safe_get(dc, "dc:identifier"))
    
    # Normalize rights values
    rights = normalize_rights(rights)
    
    # Build normalized object
    normalized = {
        "id": safe_get(header, "identifier"),
        "repository": repository_name,
        "identifier": identifier if identifier else None,
        "datestamp": safe_get(header, "datestamp"),
        "setSpec": ensure_list(safe_get(header, "setSpec")) or None,
        "title": title if title else None,
        "creator": creator if creator else None,
        "date": date if date else None,
        "description": description if description else None,
        "subject": subject if subject else None,
        "rights": rights if rights else None,
        "format": fmt if fmt else None,
        "type": type_ if type_ else None,
        "language": lang if lang else None
    }
    
    return normalized

def process_file(path):
    """Load OAI-PMH JSON, extract records, and write normalized output safely."""
    try:
        with open(path, "r", encoding="utf-8") as f:
            raw = json.load(f)
    except Exception as e:
        print(f"[Error] reading file: {path}")
        print("   →", e)
        return
    
    root = safe_get(raw, "OAI-PMH", {})
    list_records = safe_get(root, "ListRecords", {})
    records = ensure_list(safe_get(list_records, "record"))
    
    repository_name = os.path.basename(path).replace(".json", "")
    normalized_list = []
    
    for index, record in enumerate(records):
        try:
            normalized = normalize_record(record, repository_name)
            normalized_list.append(normalized)
        except Exception as e:
            print(f" [Error] processing record #{index} in file: {path}")
            print("   →", e)
            continue
    
    output_path = os.path.join(
        OUTPUT_DIR,
        os.path.basename(path).replace(".json", "_normalized.json")
    )
    
    try:
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(normalized_list, f, ensure_ascii=False, indent=2)
        print(f"[OK] Normalized: {output_path} ({len(normalized_list)} records)")
    except Exception as e:
        print(f" [Error] writing output file: {output_path}")
        print("   →", e)

def process_all_files():
    print("=" * 60)
    print("Iniciando normalización de archivos OAI-PMH")
    print("=" * 60)
    
    file_count = 0
    for root, _, files in os.walk(INPUT_DIR):
        for file in files:
            if file.endswith(".json"):
                file_count += 1
                full_path = os.path.join(root, file)
                print(f"\n[{file_count}] Procesando: {file}")
                process_file(full_path)
    
    print("\n" + "=" * 60)
    print(f" Proceso completado: {file_count} archivos procesados")
    print("=" * 60)

if __name__ == "__main__":
    process_all_files()