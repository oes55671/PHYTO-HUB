#!/usr/bin/env python3
"""Ingest open-access papers from CSV or radar text/json and optionally upload to Google Drive.

Safety policy:
- Downloads only papers with a clear open-access PDF URL (primarily via Unpaywall).
- Skips paywalled or unknown-license content.
"""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import json
import os
import re
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any, Dict, Iterable, List, Optional, Tuple

if TYPE_CHECKING:
    import requests

CROSSREF_WORKS = "https://api.crossref.org/works"
UNPAYWALL_WORKS = "https://api.unpaywall.org/v2"
PUBMED_SUMMARY = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esummary.fcgi"
USER_AGENT = "paper-ingest-bot/1.1 (mailto:{email})"
DOI_REGEX = re.compile(r"10\.\d{4,9}/[-._;()/:A-Za-z0-9]+", re.IGNORECASE)
PMID_REGEX = re.compile(r"\bPMID\s*[:#]?\s*(\d{5,9})\b", re.IGNORECASE)
URL_REGEX = re.compile(r"https?://\S+", re.IGNORECASE)


@dataclass
class PaperMetadata:
    doi: Optional[str]
    pmid: Optional[str]
    title: Optional[str]
    authors: List[str]
    year: Optional[int]
    journal: Optional[str]


def sanitize_component(value: str, max_len: int = 80) -> str:
    value = (value or "").strip()
    value = re.sub(r"[\\/:*?\"<>|]+", "_", value)
    value = re.sub(r"\s+", " ", value)
    value = value.replace(".", "")
    if len(value) > max_len:
        value = value[:max_len].rstrip()
    return value or "Unknown"


def pick_first_author(authors: List[str]) -> str:
    if not authors:
        return "UnknownAuthor"
    first = authors[0].split(",")[0].strip()
    return sanitize_component(first, 40)


def ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def normalize_doi(doi: str) -> str:
    doi = (doi or "").strip()
    doi = re.sub(r"^https?://(dx\.)?doi\.org/", "", doi, flags=re.IGNORECASE)
    return doi.strip().strip(".")


def clean_title(text: str) -> str:
    text = (text or "").strip()
    text = re.sub(r"^[-*\d.)\s]+", "", text)
    text = URL_REGEX.sub("", text)
    text = DOI_REGEX.sub("", text)
    text = PMID_REGEX.sub("", text)
    text = re.sub(r"\s+", " ", text).strip(" -|:")
    return text


def read_csv_rows(path: Path) -> List[Dict[str, str]]:
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        return list(csv.DictReader(f))


def write_rows_to_csv(path: Path, rows: List[Dict[str, str]]) -> None:
    ensure_dir(path.parent)
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["doi", "pmid", "title", "topic"])
        writer.writeheader()
        for row in rows:
            writer.writerow(
                {
                    "doi": row.get("doi", ""),
                    "pmid": row.get("pmid", ""),
                    "title": row.get("title", ""),
                    "topic": row.get("topic", ""),
                }
            )


def load_index(path: Path) -> Dict[str, Dict[str, str]]:
    if not path.exists():
        return {}
    result: Dict[str, Dict[str, str]] = {}
    success_statuses = {"DOWNLOADED", "SKIPPED_FILE_EXISTS"}
    with path.open("r", encoding="utf-8", newline="") as f:
        for row in csv.DictReader(f):
            status = (row.get("status") or "").strip().upper()
            if status not in success_statuses:
                continue
            key = normalize_doi(row.get("doi", "")).lower()
            if key:
                result[key] = row
    return result


def append_index(path: Path, row: Dict[str, str]) -> None:
    exists = path.exists()
    with path.open("a", encoding="utf-8", newline="") as f:
        fieldnames = [
            "timestamp",
            "status",
            "doi",
            "pmid",
            "title",
            "journal",
            "year",
            "topic",
            "pdf_path",
            "source_url",
            "error",
            "drive_file_id",
        ]
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        if not exists:
            writer.writeheader()
        writer.writerow({k: row.get(k, "") for k in fieldnames})


def append_manual_collect(path: Path, row: Dict[str, str]) -> None:
    exists = path.exists()
    ensure_dir(path.parent)
    with path.open("a", encoding="utf-8", newline="") as f:
        fieldnames = [
            "timestamp",
            "reason",
            "doi",
            "pmid",
            "title",
            "journal",
            "year",
            "topic",
            "note",
        ]
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        if not exists:
            writer.writeheader()
        writer.writerow({k: row.get(k, "") for k in fieldnames})


def build_session(email: str) -> "requests.Session":
    import requests
    from requests.adapters import HTTPAdapter
    from urllib3.util.retry import Retry

    clean_email = " ".join((email or "").split())
    if not clean_email:
        clean_email = "unknown@example.com"

    session = requests.Session()
    retry = Retry(
        total=4,
        read=4,
        connect=4,
        backoff_factor=0.7,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=("GET", "HEAD", "OPTIONS"),
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    session.headers.update({"User-Agent": USER_AGENT.format(email=clean_email)})
    return session


def parse_crossref_message(msg: Dict[str, Any]) -> PaperMetadata:
    doi = normalize_doi(msg.get("DOI") or "") or None
    title = (msg.get("title") or [None])[0]
    journal = (msg.get("container-title") or [None])[0]

    year = None
    issued = msg.get("issued", {}).get("date-parts", [])
    if issued and issued[0]:
        try:
            year = int(issued[0][0])
        except Exception:
            year = None

    authors = []
    for author in msg.get("author", []) or []:
        family = author.get("family")
        given = author.get("given")
        if family and given:
            authors.append(f"{family}, {given}")
        elif family:
            authors.append(family)

    return PaperMetadata(
        doi=doi,
        pmid=None,
        title=title,
        authors=authors,
        year=year,
        journal=journal,
    )


def crossref_by_doi(session: "requests.Session", doi: str) -> Optional[PaperMetadata]:
    doi = normalize_doi(doi)
    if not doi:
        return None
    response = session.get(f"{CROSSREF_WORKS}/{doi}", timeout=30)
    if response.status_code != 200:
        return None
    msg = response.json().get("message", {})
    return parse_crossref_message(msg)


def crossref_by_title(session: "requests.Session", title: str) -> Optional[PaperMetadata]:
    response = session.get(CROSSREF_WORKS, params={"query.title": title, "rows": 1}, timeout=30)
    if response.status_code != 200:
        return None
    items = response.json().get("message", {}).get("items", [])
    if not items:
        return None
    return parse_crossref_message(items[0])


def pubmed_by_pmid(session: "requests.Session", pmid: str) -> Optional[PaperMetadata]:
    response = session.get(
        PUBMED_SUMMARY,
        params={"db": "pubmed", "id": pmid, "retmode": "json"},
        timeout=30,
    )
    if response.status_code != 200:
        return None
    data = response.json().get("result", {})
    item = data.get(str(pmid))
    if not item:
        return None

    doi = None
    for article_id in item.get("articleids", []) or []:
        if article_id.get("idtype") == "doi":
            doi = normalize_doi(article_id.get("value") or "") or None
            break

    authors = []
    for author in item.get("authors", []) or []:
        name = author.get("name")
        if name:
            authors.append(name)

    year = None
    pubdate = item.get("pubdate") or ""
    match = re.search(r"(19|20)\d{2}", pubdate)
    if match:
        year = int(match.group(0))

    return PaperMetadata(
        doi=doi,
        pmid=str(pmid),
        title=item.get("title"),
        authors=authors,
        year=year,
        journal=item.get("fulljournalname") or item.get("source"),
    )


def find_oa_pdf_url(session: "requests.Session", doi: str, email: str) -> Optional[str]:
    doi = normalize_doi(doi)
    if not doi:
        return None

    response = session.get(f"{UNPAYWALL_WORKS}/{doi}", params={"email": email}, timeout=30)
    if response.status_code != 200:
        return None

    payload = response.json()
    best = payload.get("best_oa_location") or {}
    if best.get("url_for_pdf"):
        return best["url_for_pdf"]

    for loc in payload.get("oa_locations", []) or []:
        pdf_url = loc.get("url_for_pdf")
        if pdf_url:
            return pdf_url
    return None


def find_pdf_url_from_crossref(session: "requests.Session", doi: str) -> Optional[str]:
    doi = normalize_doi(doi)
    if not doi:
        return None

    response = session.get(f"{CROSSREF_WORKS}/{doi}", timeout=30)
    if response.status_code != 200:
        return None

    message = response.json().get("message", {})
    links = message.get("link") or []
    for link in links:
        content_type = (link.get("content-type") or "").lower()
        url = (link.get("URL") or "").strip()
        if not url:
            continue
        if "pdf" in content_type or url.lower().endswith(".pdf"):
            return url
    return None


def download_pdf(session: "requests.Session", url: str, output: Path) -> None:
    with session.get(url, timeout=90, stream=True, allow_redirects=True) as response:
        response.raise_for_status()
        content_type = (response.headers.get("Content-Type") or "").lower()
        final_url = str(response.url)
        if "pdf" not in content_type and not final_url.lower().endswith(".pdf"):
            raise RuntimeError(
                f"Not a PDF response (content-type={content_type}, final_url={final_url})"
            )

        with output.open("wb") as f:
            for chunk in response.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    f.write(chunk)


def build_filename(meta: PaperMetadata) -> str:
    author = pick_first_author(meta.authors)
    year = str(meta.year or "UnknownYear")
    journal = sanitize_component(meta.journal or "UnknownJournal", 30)
    title = sanitize_component(meta.title or "Untitled", 70)
    return f"{author}_{year}_{journal}_{title}.pdf"


def get_drive_service(
    oauth_client_secret_path: Optional[Path],
    oauth_token_path: Optional[Path],
):
    try:
        from google.oauth2.service_account import Credentials
        from google.auth.transport.requests import Request
        from google.oauth2.credentials import Credentials as UserCredentials
        from googleapiclient.discovery import build
        from google_auth_oauthlib.flow import InstalledAppFlow
    except Exception as e:
        return None, f"Drive libs missing: {e}"

    scopes = ["https://www.googleapis.com/auth/drive"]
    creds_path = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
    if creds_path and Path(creds_path).exists():
        creds = Credentials.from_service_account_file(creds_path, scopes=scopes)
        service = build("drive", "v3", credentials=creds)
        return service, None

    client_secret = oauth_client_secret_path
    if not client_secret:
        env_secret = os.environ.get("GOOGLE_OAUTH_CLIENT_SECRET")
        if env_secret:
            client_secret = Path(env_secret)
        elif Path("client_secret.json").exists():
            client_secret = Path("client_secret.json")

    token_path = oauth_token_path or Path(os.environ.get("GOOGLE_OAUTH_TOKEN_PATH", "token.json"))
    creds = None
    if token_path.exists():
        creds = UserCredentials.from_authorized_user_file(str(token_path), scopes=scopes)

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            if not client_secret or not client_secret.exists():
                return (
                    None,
                    "No auth available. Set GOOGLE_APPLICATION_CREDENTIALS or provide --oauth-client-secret",
                )
            flow = InstalledAppFlow.from_client_secrets_file(str(client_secret), scopes)
            creds = flow.run_local_server(port=0)
        ensure_dir(token_path.parent)
        token_path.write_text(creds.to_json(), encoding="utf-8")

    service = build("drive", "v3", credentials=creds)
    return service, None


def maybe_upload_to_drive(
    pdf_path: Path,
    folder_id: str,
    topic: str,
    year: str,
    oauth_client_secret_path: Optional[Path],
    oauth_token_path: Optional[Path],
) -> Tuple[Optional[str], Optional[str]]:
    try:
        from googleapiclient.errors import HttpError
        from googleapiclient.http import MediaFileUpload
    except Exception as e:
        return None, f"Drive libs missing: {e}"

    service, service_err = get_drive_service(oauth_client_secret_path, oauth_token_path)
    if service_err:
        return None, service_err

    try:
        service.files().get(
            fileId=folder_id,
            fields="id,name,mimeType,driveId",
            supportsAllDrives=True,
        ).execute()
    except HttpError as e:
        return None, f"Folder not accessible: {e}"

    def find_or_create_subfolder(parent_id: str, name: str) -> Tuple[Optional[str], Optional[str]]:
        escaped = name.replace("'", "\\'")
        found = (
            service.files()
            .list(
                q=(
                    f"'{parent_id}' in parents and name='{escaped}' and "
                    "mimeType='application/vnd.google-apps.folder' and trashed=false"
                ),
                fields="files(id,name)",
                pageSize=1,
                supportsAllDrives=True,
                includeItemsFromAllDrives=True,
            )
            .execute()
            .get("files", [])
        )
        if found:
            return found[0].get("id"), None

        created = (
            service.files()
            .create(
                body={
                    "name": name,
                    "mimeType": "application/vnd.google-apps.folder",
                    "parents": [parent_id],
                },
                fields="id,name",
                supportsAllDrives=True,
            )
            .execute()
        )
        return created.get("id"), None

    topic_folder_id, topic_err = find_or_create_subfolder(folder_id, topic)
    if topic_err or not topic_folder_id:
        return None, topic_err or "Failed to create/find topic folder"

    year_folder_id, year_err = find_or_create_subfolder(topic_folder_id, year)
    if year_err or not year_folder_id:
        return None, year_err or "Failed to create/find year folder"

    escaped_name = pdf_path.name.replace("'", "\\'")
    existing = (
        service.files()
        .list(
            q=f"'{year_folder_id}' in parents and name='{escaped_name}' and trashed=false",
            fields="files(id,name)",
            pageSize=1,
            supportsAllDrives=True,
            includeItemsFromAllDrives=True,
        )
        .execute()
        .get("files", [])
    )
    if existing:
        return existing[0].get("id"), None

    body = {"name": pdf_path.name, "parents": [year_folder_id]}
    media = MediaFileUpload(str(pdf_path), mimetype="application/pdf", resumable=False)
    file_obj = (
        service.files()
        .create(
            body=body,
            media_body=media,
            fields="id,name",
            supportsAllDrives=True,
        )
        .execute()
    )
    return file_obj.get("id"), None


def pick_field(record: Dict[str, Any], aliases: Iterable[str]) -> str:
    for key in aliases:
        if key in record and record[key] is not None:
            value = str(record[key]).strip()
            if value:
                return value
    return ""


def parse_structured_records(obj: Any, default_topic: str) -> List[Dict[str, str]]:
    rows: List[Dict[str, str]] = []

    def add_record(record: Dict[str, Any]) -> None:
        doi_raw = pick_field(record, ["doi", "DOI", "doi_url", "doiUrl", "url_doi"])
        doi_match = DOI_REGEX.search(doi_raw)
        doi = normalize_doi(doi_match.group(0) if doi_match else doi_raw)

        pmid_raw = pick_field(record, ["pmid", "PMID", "pubmed_id", "pubmedId"])
        pmid_match = re.search(r"\d{5,9}", pmid_raw)
        pmid = pmid_match.group(0) if pmid_match else ""

        title = pick_field(record, ["title", "paper_title", "paperTitle", "name"])
        topic = pick_field(record, ["topic", "category", "tag", "area"]) or default_topic

        if doi or pmid or title:
            rows.append({"doi": doi, "pmid": pmid, "title": clean_title(title), "topic": topic})

    if isinstance(obj, list):
        for item in obj:
            if isinstance(item, dict):
                add_record(item)
            elif isinstance(item, str):
                rows.extend(parse_radar_text(item, default_topic))
        return rows

    if isinstance(obj, dict):
        containers = ["papers", "results", "items", "articles", "data", "entries"]
        for key in containers:
            if isinstance(obj.get(key), list):
                rows.extend(parse_structured_records(obj[key], default_topic))

        # Single-record dict fallback
        if any(k in obj for k in ["doi", "DOI", "pmid", "PMID", "title", "paper_title"]):
            add_record(obj)

    return rows


def parse_block_key_values(block_text: str, default_topic: str) -> Optional[Dict[str, str]]:
    doi = ""
    pmid = ""
    title = ""
    topic = default_topic

    for line in block_text.splitlines():
        raw = line.strip()
        if not raw:
            continue
        lowered = raw.lower()
        if lowered.startswith("doi:"):
            doi_candidate = raw.split(":", 1)[1].strip()
            match = DOI_REGEX.search(doi_candidate)
            doi = normalize_doi(match.group(0) if match else doi_candidate)
        elif lowered.startswith("pmid:"):
            m = re.search(r"\d{5,9}", raw)
            if m:
                pmid = m.group(0)
        elif lowered.startswith("title:"):
            title = clean_title(raw.split(":", 1)[1].strip())
        elif lowered.startswith("topic:"):
            topic = clean_title(raw.split(":", 1)[1].strip()) or default_topic

    if doi or pmid or title:
        return {"doi": doi, "pmid": pmid, "title": title, "topic": topic}
    return None


def parse_radar_text(text: str, default_topic: str) -> List[Dict[str, str]]:
    rows: List[Dict[str, str]] = []

    # Try key:value blocks first.
    blocks = [b.strip() for b in re.split(r"\n\s*\n", text) if b.strip()]
    for block in blocks:
        parsed = parse_block_key_values(block, default_topic)
        if parsed:
            rows.append(parsed)

    # Line-based DOI extraction fallback.
    for line in text.splitlines():
        stripped = line.strip()
        if not stripped:
            continue
        lowered = stripped.lower()
        if lowered.startswith("doi:") or lowered.startswith("pmid:") or lowered.startswith("topic:"):
            continue

        doi_match = DOI_REGEX.search(stripped)
        pmid_match = PMID_REGEX.search(stripped)
        if not doi_match and not pmid_match:
            continue

        doi = normalize_doi(doi_match.group(0)) if doi_match else ""
        pmid = pmid_match.group(1) if pmid_match else ""
        title = clean_title(stripped)
        if title.lower() in {"doi", "pmid", "topic", "title"}:
            title = ""
        rows.append({"doi": doi, "pmid": pmid, "title": title, "topic": default_topic})

    # If no DOI/PMID found, fall back to bullet titles.
    if not rows:
        for line in text.splitlines():
            candidate = clean_title(line)
            if len(candidate) >= 12:
                rows.append({"doi": "", "pmid": "", "title": candidate, "topic": default_topic})

    dedup: Dict[Tuple[str, str, str, str], Dict[str, str]] = {}
    for row in rows:
        key = (
            normalize_doi(row.get("doi", "")).lower(),
            (row.get("pmid") or "").strip(),
            (row.get("title") or "").strip().lower(),
            (row.get("topic") or default_topic).strip().lower(),
        )
        dedup[key] = row
    return list(dedup.values())


def parse_radar_file(path: Path, default_topic: str) -> List[Dict[str, str]]:
    text = path.read_text(encoding="utf-8")
    rows: List[Dict[str, str]] = []

    try:
        parsed_json = json.loads(text)
        rows.extend(parse_structured_records(parsed_json, default_topic))
    except json.JSONDecodeError:
        rows.extend(parse_radar_text(text, default_topic))

    deduped: Dict[Tuple[str, str, str], Dict[str, str]] = {}
    for row in rows:
        doi = normalize_doi(row.get("doi", ""))
        pmid = (row.get("pmid") or "").strip()
        title = clean_title(row.get("title", ""))
        topic = sanitize_component(row.get("topic") or default_topic, 40)

        if not (doi or pmid or title):
            continue

        key = (doi.lower(), pmid, title.lower())
        deduped[key] = {"doi": doi, "pmid": pmid, "title": title, "topic": topic}

    return list(deduped.values())


def resolve_metadata(session: "requests.Session", row: Dict[str, str]) -> Optional[PaperMetadata]:
    doi = normalize_doi(row.get("doi", ""))
    pmid = (row.get("pmid") or "").strip()
    title = (row.get("title") or "").strip()

    if doi:
        meta = crossref_by_doi(session, doi)
        if meta:
            meta.pmid = pmid or meta.pmid
            return meta

    if pmid:
        meta = pubmed_by_pmid(session, pmid)
        if meta:
            if not meta.doi and doi:
                meta.doi = doi
            return meta

    if title:
        return crossref_by_title(session, title)

    return None


def process_row(
    session: "requests.Session",
    row: Dict[str, str],
    email: str,
    out_root: Path,
    dry_run: bool,
    drive_folder_id: Optional[str],
    index_by_doi: Dict[str, Dict[str, str]],
    index_path: Path,
    manual_collect_path: Optional[Path],
    oauth_client_secret_path: Optional[Path],
    oauth_token_path: Optional[Path],
) -> None:
    topic = sanitize_component((row.get("topic") or "Unsorted"), 40)
    ts = dt.datetime.now().isoformat(timespec="seconds")

    try:
        meta = resolve_metadata(session, row)
        if not meta:
            append_index(
                index_path,
                {
                    "timestamp": ts,
                    "status": "SKIPPED_NO_METADATA",
                    "doi": row.get("doi", ""),
                    "pmid": row.get("pmid", ""),
                    "title": row.get("title", ""),
                    "topic": topic,
                    "error": "Could not resolve metadata",
                },
            )
            if manual_collect_path:
                append_manual_collect(
                    manual_collect_path,
                    {
                        "timestamp": ts,
                        "reason": "NO_METADATA",
                        "doi": row.get("doi", ""),
                        "pmid": row.get("pmid", ""),
                        "title": row.get("title", ""),
                        "topic": topic,
                        "note": "Could not resolve metadata",
                    },
                )
            print(f"[skip] metadata unresolved: {row}")
            return

        doi_key = normalize_doi(meta.doi or "").lower()
        if doi_key and doi_key in index_by_doi:
            append_index(
                index_path,
                {
                    "timestamp": ts,
                    "status": "SKIPPED_DUPLICATE",
                    "doi": meta.doi or "",
                    "pmid": meta.pmid or "",
                    "title": meta.title or "",
                    "journal": meta.journal or "",
                    "year": str(meta.year or ""),
                    "topic": topic,
                },
            )
            print(f"[skip] duplicate DOI from index: {meta.doi}")
            return

        if not meta.doi:
            append_index(
                index_path,
                {
                    "timestamp": ts,
                    "status": "SKIPPED_NO_DOI",
                    "doi": "",
                    "pmid": meta.pmid or "",
                    "title": meta.title or "",
                    "journal": meta.journal or "",
                    "year": str(meta.year or ""),
                    "topic": topic,
                    "error": "No DOI; OA lookup skipped",
                },
            )
            if manual_collect_path:
                append_manual_collect(
                    manual_collect_path,
                    {
                        "timestamp": ts,
                        "reason": "NO_DOI",
                        "doi": "",
                        "pmid": meta.pmid or "",
                        "title": meta.title or "",
                        "journal": meta.journal or "",
                        "year": str(meta.year or ""),
                        "topic": topic,
                        "note": "No DOI; OA lookup skipped",
                    },
                )
            print(f"[skip] no DOI for OA check: {meta.title}")
            return

        pdf_url = find_oa_pdf_url(session, meta.doi, email)
        if not pdf_url:
            pdf_url = find_pdf_url_from_crossref(session, meta.doi)
        if not pdf_url:
            append_index(
                index_path,
                {
                    "timestamp": ts,
                    "status": "SKIPPED_NOT_OA",
                    "doi": meta.doi or "",
                    "pmid": meta.pmid or "",
                    "title": meta.title or "",
                    "journal": meta.journal or "",
                    "year": str(meta.year or ""),
                    "topic": topic,
                    "error": "No PDF URL from Unpaywall/Crossref",
                },
            )
            if manual_collect_path:
                append_manual_collect(
                    manual_collect_path,
                    {
                        "timestamp": ts,
                        "reason": "NOT_OA",
                        "doi": meta.doi or "",
                        "pmid": meta.pmid or "",
                        "title": meta.title or "",
                        "journal": meta.journal or "",
                        "year": str(meta.year or ""),
                        "topic": topic,
                        "note": "No PDF URL from Unpaywall/Crossref",
                    },
                )
            print(f"[skip] paywalled/unknown OA: {meta.doi}")
            return

        year = str(meta.year or "UnknownYear")
        target_dir = out_root / topic / year
        ensure_dir(target_dir)
        filename = build_filename(meta)
        output_path = target_dir / filename

        if output_path.exists():
            append_index(
                index_path,
                {
                    "timestamp": ts,
                    "status": "SKIPPED_FILE_EXISTS",
                    "doi": meta.doi,
                    "pmid": meta.pmid or "",
                    "title": meta.title or "",
                    "journal": meta.journal or "",
                    "year": str(meta.year or ""),
                    "topic": topic,
                    "pdf_path": str(output_path),
                    "source_url": pdf_url,
                },
            )
            print(f"[skip] file exists: {output_path}")
            return

        if dry_run:
            append_index(
                index_path,
                {
                    "timestamp": ts,
                    "status": "DRYRUN_READY",
                    "doi": meta.doi,
                    "pmid": meta.pmid or "",
                    "title": meta.title or "",
                    "journal": meta.journal or "",
                    "year": str(meta.year or ""),
                    "topic": topic,
                    "pdf_path": str(output_path),
                    "source_url": pdf_url,
                },
            )
            print(f"[dry-run] would download {meta.doi} -> {output_path}")
            return

        try:
            download_pdf(session, pdf_url, output_path)
        except RuntimeError as e:
            if "Not a PDF response" in str(e):
                append_index(
                    index_path,
                    {
                        "timestamp": ts,
                        "status": "SKIPPED_NOT_OA",
                        "doi": meta.doi or "",
                        "pmid": meta.pmid or "",
                        "title": meta.title or "",
                        "journal": meta.journal or "",
                        "year": str(meta.year or ""),
                        "topic": topic,
                        "source_url": pdf_url,
                        "error": str(e),
                    },
                )
                if manual_collect_path:
                    append_manual_collect(
                        manual_collect_path,
                        {
                            "timestamp": ts,
                            "reason": "NOT_PDF",
                            "doi": meta.doi or "",
                            "pmid": meta.pmid or "",
                            "title": meta.title or "",
                            "journal": meta.journal or "",
                            "year": str(meta.year or ""),
                            "topic": topic,
                            "note": str(e),
                        },
                    )
                print(f"[skip] non-pdf landing page: {meta.doi}")
                return
            raise
        drive_file_id = ""

        if drive_folder_id:
            file_id, drive_err = maybe_upload_to_drive(
                output_path,
                drive_folder_id,
                topic,
                year,
                oauth_client_secret_path,
                oauth_token_path,
            )
            if drive_err:
                print(f"[warn] Drive upload skipped: {drive_err}")
            else:
                drive_file_id = file_id or ""
                print(f"[ok] uploaded to Drive: {drive_file_id}")

        append_index(
            index_path,
            {
                "timestamp": ts,
                "status": "DOWNLOADED",
                "doi": meta.doi,
                "pmid": meta.pmid or "",
                "title": meta.title or "",
                "journal": meta.journal or "",
                "year": str(meta.year or ""),
                "topic": topic,
                "pdf_path": str(output_path),
                "source_url": pdf_url,
                "drive_file_id": drive_file_id,
            },
        )

        if doi_key:
            index_by_doi[doi_key] = {"doi": meta.doi}

        print(f"[ok] downloaded: {output_path}")

    except Exception as exc:
        append_index(
            index_path,
            {
                "timestamp": ts,
                "status": "ERROR",
                "doi": row.get("doi", ""),
                "pmid": row.get("pmid", ""),
                "title": row.get("title", ""),
                "topic": topic,
                "error": str(exc),
            },
        )
        print(f"[error] {row.get('doi') or row.get('title')}: {exc}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Open-access paper ingester")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--input", help="CSV path with columns: doi,pmid,title,topic")
    group.add_argument("--radar-input", help="TXT/MD/JSON file from GPT radar output")

    parser.add_argument("--parsed-csv-out", default="", help="Optional CSV path to export parsed radar rows")
    parser.add_argument("--radar-default-topic", default="Unsorted", help="Default topic for radar parsing")
    parser.add_argument("--output-dir", default="downloads", help="Local output root directory")
    parser.add_argument("--index", default="logs/paper_index.csv", help="Index CSV log path")
    parser.add_argument(
        "--manual-collect",
        default="logs/manual_collect.csv",
        help="CSV path for papers that need manual collection",
    )
    parser.add_argument("--email", required=True, help="Contact email for Unpaywall/Crossref user agent")
    parser.add_argument("--drive-folder-id", default="", help="Optional Google Drive folder ID")
    parser.add_argument(
        "--oauth-client-secret",
        default="",
        help="Path to OAuth client secret JSON (Desktop app). Fallback: GOOGLE_OAUTH_CLIENT_SECRET or ./client_secret.json",
    )
    parser.add_argument(
        "--oauth-token",
        default="token.json",
        help="Path to cached OAuth token file (default: token.json)",
    )
    parser.add_argument("--max", type=int, default=0, help="Process first N rows only (0=all)")
    parser.add_argument("--sleep", type=float, default=0.3, help="Sleep seconds between rows")
    parser.add_argument("--dry-run", action="store_true", help="Resolve and validate without downloading")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    args.email = " ".join((args.email or "").split())

    out_root = Path(args.output_dir)
    index_path = Path(args.index)
    manual_collect_path = Path(args.manual_collect) if args.manual_collect else None
    oauth_client_secret_path = Path(args.oauth_client_secret) if args.oauth_client_secret else None
    oauth_token_path = Path(args.oauth_token) if args.oauth_token else None
    ensure_dir(out_root)
    ensure_dir(index_path.parent)
    if manual_collect_path:
        ensure_dir(manual_collect_path.parent)
    if oauth_token_path:
        ensure_dir(oauth_token_path.parent)

    rows: List[Dict[str, str]]
    if args.input:
        rows = read_csv_rows(Path(args.input))
    else:
        rows = parse_radar_file(Path(args.radar_input), args.radar_default_topic)
        print(f"[info] parsed {len(rows)} candidate rows from radar input")
        if args.parsed_csv_out:
            parsed_out_path = Path(args.parsed_csv_out)
            write_rows_to_csv(parsed_out_path, rows)
            print(f"[info] wrote parsed rows csv: {parsed_out_path}")

    if args.max > 0:
        rows = rows[: args.max]

    if not rows:
        print("No rows to process. Check input format.")
        return 1

    index_by_doi = load_index(index_path)

    try:
        session = build_session(args.email)
    except ImportError as exc:
        print(f"Missing dependency: {exc}. Run: pip install -r requirements.txt")
        return 2
    for idx, row in enumerate(rows, start=1):
        print(f"\n[{idx}/{len(rows)}] processing")
        process_row(
            session=session,
            row=row,
            email=args.email,
            out_root=out_root,
            dry_run=args.dry_run,
            drive_folder_id=(args.drive_folder_id or None),
            index_by_doi=index_by_doi,
            index_path=index_path,
            manual_collect_path=manual_collect_path,
            oauth_client_secret_path=oauth_client_secret_path,
            oauth_token_path=oauth_token_path,
        )
        time.sleep(max(args.sleep, 0.0))

    print("\nDone.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
