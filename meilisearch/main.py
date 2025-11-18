from fastapi import FastAPI, HTTPException
from meilisearch import Client
import json
import os
from contextlib import asynccontextmanager
import load_data

# MeiliSearch client
client = Client('http://localhost:7700')

# Index name
INDEX_NAME = 'documents'

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    # Create index if it doesn't exist
    try:
        task_info = client.create_index(INDEX_NAME, {'primaryKey': 'id'})
        client.wait_for_task(task_info.task_uid)
    except Exception as e:
        # Index might already exist
        pass
    
    # Get the index
    index = client.index(INDEX_NAME)

    # Load and modify production data
    documents = load_data.load_and_modify_data()
    if documents:
        load_data.load_data_to_meilisearch(documents, client, INDEX_NAME)
        print("✓ Data loading complete!")
    else:
        print("No documents to load.")
    yield
    # Shutdown (if needed)

app = FastAPI(lifespan=lifespan)

@app.get("/search/partial")
async def partial_search(q: str):
    try:
        results = client.index(INDEX_NAME).search(q, {'limit': 3})
        return {"results": results['hits']}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/search/total")
async def total_search(q: str):
    try:
        results = client.index(INDEX_NAME).search(q)
        return {"results": results['hits']}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))