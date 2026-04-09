#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
from collections import Counter
from pathlib import Path


def build_report(rows: list[dict[str, str]]) -> str:
    if not rows:
        return "# Manual Collection Queue\n\nNo items."

    reason_counts = Counter((r.get("reason") or "UNKNOWN") for r in rows)
    lines = [
        "# Manual Collection Queue",
        "",
        f"Total items: **{len(rows)}**",
        "",
        "## By Reason",
    ]
    for reason, count in sorted(reason_counts.items()):
        lines.append(f"- `{reason}`: {count}")

    lines.append("")
    lines.append("## Top Items")

    preview = rows[-20:]
    for r in reversed(preview):
        doi = r.get("doi", "")
        title = r.get("title", "")
        topic = r.get("topic", "")
        note = r.get("note", "")
        lines.append(f"- DOI: `{doi or '-'}` | Topic: `{topic or '-'}` | {title or '-'}")
        if note:
            lines.append(f"  - Note: {note}")

    return "\n".join(lines)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("input", help="Path to manual_collect.csv")
    parser.add_argument("--out", default="", help="Output markdown path")
    args = parser.parse_args()

    p = Path(args.input)
    rows: list[dict[str, str]] = []
    if p.exists():
        with p.open("r", encoding="utf-8", newline="") as f:
            rows = list(csv.DictReader(f))

    report = build_report(rows)
    if args.out:
        out = Path(args.out)
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(report, encoding="utf-8")
    else:
        print(report)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
