#!/usr/bin/env python3
"""
Test script to debug MeiliSearch indexing issues
"""

import json
import time
from meilisearch import Client

# Configuration
MEILISEARCH_URL = 'http://localhost:7700'
INDEX_NAME = 'documents'

def test_single_document():
    """Test adding a single document to debug indexing issues."""
    print(f"Connecting to MeiliSearch at {MEILISEARCH_URL}...")
    client = Client(MEILISEARCH_URL)
    
    # Get or create index
    try:
        index = client.index(INDEX_NAME)
        print("Index exists")
        settings = index.get_settings()
        print(f"Current settings: {settings}")
        
        # Try to delete the index and recreate
        print("Deleting index...")
        task_info = index.delete()
        client.wait_for_task(task_info.task_uid)
        print("Index deleted")
        
    except Exception as e:
        print(f"Index doesn't exist: {e}")
    
    # Create fresh index
    print("Creating fresh index with primary key 'id'...")
    task_info = client.create_index(INDEX_NAME, {'primaryKey': 'id'})
    client.wait_for_task(task_info.task_uid)
    print("Index created")
    
    # Test document
    test_doc = {
        "id": "test-123",
        "title": "Test Document",
        "content": "This is a test document to verify indexing works"
    }
    
    print("Adding test document...")
    try:
        task_info = index.add_documents([test_doc])
        client.wait_for_task(task_info.task_uid)
        print("Test document added")
        
        # Check stats
        stats = index.get_stats()
        print(f"Documents after test: {stats.number_of_documents}")
        
        # Try search
        results = index.search('test')
        print(f"Search results: {len(results['hits'])}")
        if results['hits']:
            print(f"Found: {results['hits'][0]['title']}")
            
    except Exception as e:
        print(f"Error adding test document: {e}")
        import traceback
        traceback.print_exc()

if __name__ == '__main__':
    test_single_document()