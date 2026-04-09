#!/usr/bin/env bash
set -euo pipefail

# Usage:
# ./scripts/run_radar_ingest.sh data/radar_input.md you@example.com [drive_folder_id] [oauth_client_secret]

RADAR_INPUT="${1:-data/radar_sample.md}"
EMAIL="${2:-you@example.com}"
DRIVE_FOLDER_ID="${3:-1_ogEIopbmPtwfjkyl4qgt64Lob0Bld64}"
OAUTH_CLIENT_SECRET="${4:-client_secret.json}"

python3 src/paper_ingest.py \
  --radar-input "$RADAR_INPUT" \
  --parsed-csv-out logs/parsed_from_radar.csv \
  --radar-default-topic tissue-culture \
  --email "$EMAIL" \
  --drive-folder-id "$DRIVE_FOLDER_ID" \
  --oauth-client-secret "$OAUTH_CLIENT_SECRET"
