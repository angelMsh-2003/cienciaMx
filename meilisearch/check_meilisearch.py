#!/usr/bin/env python3
"""
Script to check MeiliSearch status and index information.
"""

from meilisearch import Client
import json

# Configuration
MEILISEARCH_URL = 'http://localhost:7700'
INDEX_NAME = 'documents'

def check_meilisearch():
    """Check MeiliSearch connection and index status."""
    print(f"Connecting to MeiliSearch at {MEILISEARCH_URL}...")
    
    try:
        client = Client(MEILISEARCH_URL)
        
        # Check health
        health = client.health()
        print(f"✓ MeiliSearch is healthy: {health}")
        
        # Get version
        version = client.get_version()
        print(f"✓ MeiliSearch version: {version}")
        
        # List all indexes
        print("\nIndexes:")
        indexes = client.get_indexes()
        for idx in indexes['results']:
            print(f"  - {idx.uid}: {idx.primary_key}")
        
        # Check specific index
        print(f"\nChecking index '{INDEX_NAME}':")
        try:
            index = client.index(INDEX_NAME)
            stats = index.get_stats()
            
            print(f"  Primary key: {index.primary_key}")
            print(f"  Documents: {stats.number_of_documents}")
            print(f"  Is indexing: {stats.is_indexing}")
            
            # Get settings
            settings = index.get_settings()
            print(f"\n  Searchable attributes: {settings.get('searchableAttributes', 'default')}")
            print(f"  Filterable attributes: {settings.get('filterableAttributes', [])}")
            print(f"  Sortable attributes: {settings.get('sortableAttributes', [])}")
            
            # Try a sample search
            if stats.number_of_documents > 0:
                print("\n  Testing search with query 'universidad':")
                results = index.search('universidad', {'limit': 3})
                print(f"  Found {results['estimatedTotalHits']} results")
                print(f"  Showing {len(results['hits'])} results:")
                for i, hit in enumerate(results['hits'], 1):
                    title = hit.get('title', 'No title')
                    if isinstance(title, list):
                        title = title[0] if title else 'No title'
                    print(f"    {i}. {title[:80]}...")
            
        except Exception as e:
            print(f"  ✗ Error accessing index: {e}")
        
    except Exception as e:
        print(f"✗ Error connecting to MeiliSearch: {e}")
        print("\nMake sure MeiliSearch is running:")
        print("  - With Docker: docker-compose up")
        print("  - Standalone: docker run -p 7700:7700 getmeili/meilisearch:latest")

if __name__ == '__main__':
    check_meilisearch()