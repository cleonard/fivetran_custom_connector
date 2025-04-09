#!/usr/bin/env bash

PYTHON_PATH=./.venv/bin/python

# Export env vars from .env
export $(grep -v '^#' .env | xargs)

# Create Fivetran's configuration.json file from .env
"${PYTHON_PATH}" gen_config.py

fivetran deploy \
  --api-key "${FIVETRAN_API_KEY}" \
  --destination "${FIVETRAN_DESTINATION_NAME}" \
  --connection "${FIVETRAN_CONNECTION_NAME}" \
  --configuration configuration.json


