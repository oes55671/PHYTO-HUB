#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import os
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional

import requests

NOTION_API = "https://api.notion.com/v1"
NOTION_VERSION = "2022-06-28"


@dataclass
class NotionConfig:
    token: str
    database_id: str
    title_property: str
    doi_property: str
    status_property: str
    topic_property: str
    year_property: str
    source_property: str


def notion_headers(token: str) -> Dict[str, str]:
    return {
        "Authorization": f"Bearer {token}",
        "Notion-Version": NOTION_VERSION,
        "Content-Type": "application/json",
    }


def parse_rows(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8", newline="") as f:
        return list(csv.DictReader(f))


def query_page_by_doi(cfg: NotionConfig, doi: str) -> Optional[str]:
    if not doi:
        return None
    url = f"{NOTION_API}/databases/{cfg.database_id}/query"
    payload = {
        "filter": {
            "property": cfg.doi_property,
            "rich_text": {"equals": doi},
        },
        "page_size": 1,
    }
    res = requests.post(url, headers=notion_headers(cfg.token), json=payload, timeout=30)
    res.raise_for_status()
    data = res.json()
    results = data.get("results", [])
    if not results:
        return None
    return results[0].get("id")


def build_properties(cfg: NotionConfig, row: dict[str, str]) -> dict[str, Any]:
    title = (row.get("title") or "Untitled").strip()
    doi = (row.get("doi") or "").strip()
    status = (row.get("status") or "").strip()
    topic = (row.get("topic") or "").strip()
    year = (row.get("year") or "").strip()
    source = (row.get("source_url") or "").strip()

    props: dict[str, Any] = {
        cfg.title_property: {"title": [{"text": {"content": title[:1900]}}]},
        cfg.doi_property: {"rich_text": [{"text": {"content": doi[:1900]}}]},
        cfg.status_property: {"select": {"name": status or "UNKNOWN"}},
        cfg.topic_property: {"rich_text": [{"text": {"content": topic[:1900]}}]},
        cfg.year_property: {"number": int(year) if year.isdigit() else None},
        cfg.source_property: {"url": source or None},
    }
    return props


def upsert_row(cfg: NotionConfig, row: dict[str, str], dry_run: bool) -> tuple[str, str]:
    doi = (row.get("doi") or "").strip()
    page_id = query_page_by_doi(cfg, doi) if doi else None
    props = build_properties(cfg, row)

    if dry_run:
        return ("DRYRUN_UPDATE" if page_id else "DRYRUN_CREATE", doi)

    if page_id:
        url = f"{NOTION_API}/pages/{page_id}"
        res = requests.patch(url, headers=notion_headers(cfg.token), json={"properties": props}, timeout=30)
        res.raise_for_status()
        return ("UPDATED", doi)

    url = f"{NOTION_API}/pages"
    payload = {
        "parent": {"database_id": cfg.database_id},
        "properties": props,
    }
    res = requests.post(url, headers=notion_headers(cfg.token), json=payload, timeout=30)
    res.raise_for_status()
    return ("CREATED", doi)


def main() -> int:
    parser = argparse.ArgumentParser(description="Sync paper_index.csv rows to Notion database")
    parser.add_argument("--input", default="logs/paper_index.csv", help="CSV path")
    parser.add_argument("--limit", type=int, default=50, help="Sync latest N rows")
    parser.add_argument("--only-status", default="", help="Comma-separated statuses filter")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    cfg = NotionConfig(
        token=os.environ.get("NOTION_TOKEN", ""),
        database_id=os.environ.get("NOTION_DATABASE_ID", ""),
        title_property=os.environ.get("NOTION_TITLE_PROPERTY", "Title"),
        doi_property=os.environ.get("NOTION_DOI_PROPERTY", "DOI"),
        status_property=os.environ.get("NOTION_STATUS_PROPERTY", "Status"),
        topic_property=os.environ.get("NOTION_TOPIC_PROPERTY", "Topic"),
        year_property=os.environ.get("NOTION_YEAR_PROPERTY", "Year"),
        source_property=os.environ.get("NOTION_SOURCE_PROPERTY", "Source URL"),
    )

    if not cfg.token or not cfg.database_id:
        raise SystemExit("Set NOTION_TOKEN and NOTION_DATABASE_ID first")

    rows = parse_rows(Path(args.input))
    if args.only_status:
        allowed = {x.strip().upper() for x in args.only_status.split(",") if x.strip()}
        rows = [r for r in rows if (r.get("status") or "").upper() in allowed]

    rows = rows[-args.limit :] if args.limit > 0 else rows
    if not rows:
        print("No rows to sync")
        return 0

    created = 0
    updated = 0
    for row in rows:
        outcome, doi = upsert_row(cfg, row, dry_run=args.dry_run)
        print(f"[{outcome}] {doi or row.get('title','')}")
        if outcome == "CREATED":
            created += 1
        elif outcome == "UPDATED":
            updated += 1

    print(f"Done. created={created}, updated={updated}, total={len(rows)} at {datetime.now().isoformat(timespec='seconds')}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
