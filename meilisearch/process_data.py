import json
import os
from pathlib import Path

def extract_records_from_json(json_file_path):
    """Extract records from a page JSON file."""
    try:
        with open(json_file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        # Navigate to ListRecords.record
        records = data.get('OAI-PMH', {}).get('ListRecords', {}).get('record', [])
        return records if isinstance(records, list) else [records]
    except Exception as e:
        print(f"Error processing {json_file_path}: {e}")
        return []

def flatten_record(record, repo_name):
    """Transform a record into a flat structure for MeiliSearch."""
    try:
        header = record.get('header', {})
        metadata = record.get('metadata', {}).get('oai_dc:dc', {})
        
        # Extract identifier as primary key
        identifier = header.get('identifier', '')
        
        # Helper function to extract text from various formats
        def extract_text(value):
            if isinstance(value, str):
                return value
            elif isinstance(value, dict):
                return value.get('#text', str(value))
            elif isinstance(value, list):
                return [extract_text(v) for v in value]
            return str(value)
        
        # Build flat document
        doc = {
            'id': identifier,
            'repository': repo_name,
            'identifier': identifier,
            'datestamp': header.get('datestamp', ''),
            'setSpec': header.get('setSpec', []),
        }
        
        # Add metadata fields
        for key, value in metadata.items():
            if key.startswith('@'):
                continue  # Skip XML attributes
            
            # Clean field name (remove namespace prefix)
            field_name = key.replace('dc:', '')
            doc[field_name] = extract_text(value)
        
        return doc
    except Exception as e:
        print(f"Error flattening record: {e}")
        return None

def process_all_repositories(data_dir, output_dir):
    """Process all repositories and consolidate records."""
    data_path = Path(data_dir)
    all_documents = []
    
    # Find all page_*.json files
    for repo_dir in data_path.iterdir():
        if not repo_dir.is_dir():
            continue
        
        repo_name = repo_dir.name
        print(f"Processing repository: {repo_name}")
        
        # Process all page files in this repository
        for page_file in repo_dir.glob('page_*.json'):
            print(f"  Reading {page_file.name}")
            records = extract_records_from_json(page_file)
            
            for record in records:
                doc = flatten_record(record, repo_name)
                if doc and doc.get('setSpec'):
                    all_documents.append(doc)
    
    # Create output directory if it doesn't exist
    output_path = Path(output_dir)
    output_path.mkdir(exist_ok=True)
    
    # Save consolidated data
    output_file = output_path / 'consolidated_data.json'
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(all_documents, f, ensure_ascii=False, indent=2)
    
    print(f"\nProcessing complete!")
    print(f"Total documents: {len(all_documents)}")
    print(f"Output file: {output_file}")
    
    return len(all_documents)

if __name__ == '__main__':
    # Configuration
    DATA_DIR = 'data/20251113_200813'
    OUTPUT_DIR = 'prod_data'
    
    # Process all data
    total = process_all_repositories(DATA_DIR, OUTPUT_DIR)
    print(f"\n✓ Successfully processed {total} documents")