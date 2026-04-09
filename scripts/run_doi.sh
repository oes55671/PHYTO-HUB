#!/usr/bin/env bash
set -euo pipefail

# Usage:
# ./scripts/run_doi.sh "10.7717/peerj.4375" "organoid" "your_email@example.com"

DOI="${1:-}"
TOPIC="${2:-manual}"
EMAIL="${3:-your_email@example.com}"
DRIVE_FOLDER_ID="${DRIVE_FOLDER_ID:-1_ogEIopbmPtwfjkyl4qgt64Lob0Bld64}"
OAUTH_JSON="${OAUTH_JSON:-}"

if [[ -z "$DOI" ]]; then
  echo "[error] DOI is required."
  echo "Example: ./scripts/run_doi.sh \"10.7717/peerj.4375\" \"organoid\" \"your_email@example.com\""
  exit 1
fi

if [[ -z "$OAUTH_JSON" ]]; then
  echo "[error] OAUTH_JSON is not set."
  echo "Set it first: export OAUTH_JSON=\"/Users/oh/Downloads/client_secret_xxx.json\""
  exit 1
fi

TMP_CSV="/tmp/doi_single_input.csv"
cat > "$TMP_CSV" <<CSV
doi,pmid,title,topic
$DOI,,,$TOPIC
CSV

python src/paper_ingest.py \
  --input "$TMP_CSV" \
  --email "$EMAIL" \
  --drive-folder-id "$DRIVE_FOLDER_ID" \
  --oauth-client-secret "$OAUTH_JSON" \
  --oauth-token token.json
