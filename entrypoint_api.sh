#!/bin/sh
# Entrypoint for the FastAPI backend (port 8088)
exec python -m uvicorn web.api:app --host 0.0.0.0 --port 8088 --log-level warning
