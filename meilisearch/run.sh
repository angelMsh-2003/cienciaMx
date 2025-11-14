#!/bin/bash

# Script to run the FastAPI server with MeiliSearch integration

echo "Starting FastAPI server..."
uvicorn main:app --reload --host 0.0.0.0 --port 8000