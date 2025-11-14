#!/usr/bin/env python3
"""
Script to load documents from prod_data/consolidated_data.json into MeiliSearch.
Generates incremental IDs as primary identifiers, keeping original IDs for informational purposes.
"""

import json
import os
from meilisearch import Client

# Configuration
MEILISEARCH_URL = 'http://localhost:7700'
INDEX_NAME = 'documents'
DATA_PATH = 'prod_data/consolidated_data.json'
BATCH_SIZE = 10

def load_and_modify_data():
    """Load data and modify IDs."""
    if not os.path.exists(DATA_PATH):
        print(f"Error: {DATA_PATH} not found.")
        return []

    print(f"Loading data from {DATA_PATH}...")
    with open(DATA_PATH, 'r', encoding='utf-8') as f:
        documents = json.load(f)

    print(f"Total documents loaded: {len(documents)}")

    # Modify documents: generate incremental IDs, keep original as 'original_id'
    modified_documents = []
    for i, doc in enumerate(documents, start=1):
        new_doc = doc.copy()
        new_doc['original_id'] = new_doc.pop('id')  # Rename 'id' to 'original_id'
        new_doc['id'] = i  # Set new incremental ID
        modified_documents.append(new_doc)

    print(f"Modified {len(modified_documents)} documents with incremental IDs.")
    return modified_documents

def load_data_to_meilisearch(documents, client, index_name):
    """Load modified documents into MeiliSearch."""
    # Create index if it doesn't exist
    try:
        task_info = client.create_index(index_name, {'primaryKey': 'id'})
        client.wait_for_task(task_info.task_uid)
        print(f"✓ Created index '{index_name}' with primary key 'id'")
    except Exception as e:
        print(f"Index '{index_name}' might already exist: {e}")

    # Get the index
    index = client.index(index_name)

    # Load documents in batches
    total_batches = (len(documents) + BATCH_SIZE - 1) // BATCH_SIZE
    failed_batches = []
    for i in range(0, len(documents), BATCH_SIZE):
        batch = documents[i:i + BATCH_SIZE]
        batch_num = i // BATCH_SIZE + 1
        try:
            task_info = index.add_documents(batch)
            client.wait_for_task(task_info.task_uid, timeout_in_ms=60000)  # Increased timeout to 60 seconds
            print(f"✓ Loaded batch {batch_num}/{total_batches}: {len(batch)} documents")
        except Exception as e:
            print(f"✗ Failed to load batch {batch_num}/{total_batches}: {len(batch)} documents - Error: {e}")
            print(f"  Error type: {type(e).__name__}")
            if hasattr(e, 'code'):
                print(f"  Error code: {e.code}")
            if hasattr(e, 'message'):
                print(f"  Error message: {e.message}")
            failed_batches.append(batch_num)
            continue  # Continue with next batch

    # Verify
    try:
        stats = index.get_stats()
        print(f"✓ Index stats: {stats.number_of_documents} documents indexed")
    except Exception as e:
        print(f"✗ Error getting index stats: {e}")

    # Report failed batches
    if failed_batches:
        print(f"\n⚠ Warning: {len(failed_batches)} batches failed to load: {failed_batches}")
    else:
        print("\n✓ All batches loaded successfully!")

if __name__ == "__main__":
    meili_client = Client(MEILISEARCH_URL)
    docs = load_and_modify_data()
    if docs:
        load_data_to_meilisearch(docs, meili_client, INDEX_NAME)
        print("✓ Data loading complete!")
    else:
        print("No documents to load.")