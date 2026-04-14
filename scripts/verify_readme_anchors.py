#!/usr/bin/env python3
"""Verify README TOC links: each anchor matches a heading slug or explicit HTML id."""

from __future__ import annotations

import re
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
README = REPO_ROOT / "README.md"

# TOC block: from "## Table of contents" through the closing `---` before "## Quick start"
TOC_START = "## Table of contents"


def github_heading_slug(title: str) -> str:
    """Approximate GitHub-rendered slug for a markdown heading line (no leading #)."""
    t = title.strip()
    t = re.sub(r"\*\*([^*]+)\*\*", r"\1", t)
    t = t.lower()
    t = t.replace(" & ", "--")
    t = re.sub(r"[^\w\s-]", "", t, flags=re.UNICODE)
    t = re.sub(r"\s+", "-", t.strip())
    return t


def extract_toc_anchors(text: str) -> list[str]:
    start = text.find(TOC_START)
    if start == -1:
        raise SystemExit(f"Missing {TOC_START!r} in README")
    rest = text[start:]
    end = rest.find("\n---\n", 1)
    if end == -1:
        raise SystemExit("Missing TOC end delimiter (---) after Table of contents")
    toc_block = rest[:end]
    return re.findall(r"\(#([a-z0-9-]+)\)", toc_block)


def extract_explicit_html_ids(text: str) -> set[str]:
    return set(re.findall(r'\bid="([^"]+)"', text))


def extract_markdown_heading_slugs(text: str) -> set[str]:
    slugs: set[str] = set()
    for m in re.finditer(r"^(#{2,3})\s+(.+)$", text, re.MULTILINE):
        title = m.group(2).strip()
        slugs.add(github_heading_slug(title))
    return slugs


def main() -> int:
    text = README.read_text(encoding="utf-8")
    toc = extract_toc_anchors(text)
    if not toc:
        print("No TOC anchors found.", file=sys.stderr)
        return 1

    valid = extract_explicit_html_ids(text) | extract_markdown_heading_slugs(text)
    missing = [a for a in toc if a not in valid]
    if missing:
        print("TOC anchors with no matching heading id/slug:", file=sys.stderr)
        for a in missing:
            print(f"  #{a}", file=sys.stderr)
        return 1

    print(f"OK: {len(toc)} TOC anchors match headings ({README.relative_to(REPO_ROOT)})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
