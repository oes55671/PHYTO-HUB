# Ops Max Setup (Notion + GitHub)

## 1) Notion Database Schema (Recommended)
Create one database named `Paper Ops` with these properties:
- `Title` (title)
- `DOI` (rich text)
- `Status` (select)
- `Topic` (rich text)
- `Year` (number)
- `Source URL` (url)

Optional select values for `Status`:
- `DOWNLOADED`
- `SKIPPED_NOT_OA`
- `SKIPPED_DUPLICATE`
- `SKIPPED_FILE_EXISTS`
- `SKIPPED_NO_METADATA`
- `SKIPPED_NO_DOI`
- `ERROR`

## 2) GitHub Secrets
In repo settings, add:
- `CONTACT_EMAIL`
- `DRIVE_FOLDER_ID`
- `GOOGLE_OAUTH_CLIENT_SECRET_JSON` (optional)
- `GOOGLE_OAUTH_TOKEN_JSON` (optional)
- `GOOGLE_APPLICATION_CREDENTIALS_JSON` (optional)
- `NOTION_TOKEN` (for Notion sync workflow if enabled)
- `NOTION_DATABASE_ID` (for Notion sync workflow if enabled)

## 3) Weekly Cadence
- Mon/Wed/Fri 09:00 KST: `Research Radar Ingest` workflow runs.
- If manual queue exists, issue is auto-created with `manual-collection` label.

## 4) Local Fast Commands
Run single DOI:
```bash
./scripts/run_doi.sh "10.1186/s12870-026-08246-x" "manual" "your_email@example.com"
```

Sync latest rows to Notion:
```bash
NOTION_TOKEN="..." NOTION_DATABASE_ID="..." \
python scripts/sync_to_notion.py --input logs/paper_index.csv --limit 100
```

Only sync successful rows:
```bash
NOTION_TOKEN="..." NOTION_DATABASE_ID="..." \
python scripts/sync_to_notion.py --only-status DOWNLOADED --limit 200
```

## 5) Default Operating Rule
- Use Notion for planning/priorities
- Use GitHub Issues/PR for execution
- Use `paper_index.csv` as source-of-truth event log
