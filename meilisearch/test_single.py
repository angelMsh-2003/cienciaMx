#!/usr/bin/env python3
"""
Test script to load a single document into MeiliSearch.
"""

import json
from meilisearch import Client

# Configuration
MEILISEARCH_URL = 'http://demos-meilisearch-6q6cgd-4f9541-217-196-48-210.traefik.me/'
INDEX_NAME = 'documents'

def test_single_document():
    # Load first document
    with open('prod_data/consolidated_data.json', 'r', encoding='utf-8') as f:
        documents = json.load(f)

    # Modify first document like the script does
    doc = documents[0].copy()
    doc['original_id'] = doc.pop('id')
    doc['id'] = 1

    print(f"Loading document: {doc['id']}")
    print(f"Original ID: {doc['original_id']}")
    print(f"Title: {doc.get('title', 'No title')[:100]}...")

    client = Client(MEILISEARCH_URL)
    index = client.index(INDEX_NAME)

    try:
        task_info = index.add_documents([doc])
        client.wait_for_task(task_info.task_uid, timeout_in_ms=60000)
        print("✓ Successfully loaded single document")
    except Exception as e:
        print(f"✗ Failed to load single document: {e}")
        print(f"Error type: {type(e).__name__}")
        if hasattr(e, 'code'):
            print(f"Error code: {e.code}")
        if hasattr(e, 'message'):
            print(f"Error message: {e.message}")

if __name__ == "__main__":
    test_single_document()