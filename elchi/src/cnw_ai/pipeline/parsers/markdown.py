"""Markdown / RST parser: split by headings into sections."""

from __future__ import annotations

import re
from pathlib import Path

from cnw_ai.pipeline.models import ParsedDocument, SourceConfig


_HEADING_RE = re.compile(r"^(#{1,6})\s+(.+)$", re.MULTILINE)

# RST underline characters (in priority order)
_RST_UNDERLINE_CHARS = set("=-~^`_*+#")


def _split_markdown_sections(text: str) -> list[tuple[str, str]]:
    """Split markdown text into (heading, body) sections."""
    sections: list[tuple[str, str]] = []
    matches = list(_HEADING_RE.finditer(text))

    if not matches:
        # No headings, return entire text as one section
        return [("", text.strip())]

    # Text before first heading
    preamble = text[: matches[0].start()].strip()
    if preamble:
        sections.append(("", preamble))

    for i, match in enumerate(matches):
        heading = match.group(2).strip()
        start = match.end()
        end = matches[i + 1].start() if i + 1 < len(matches) else len(text)
        body = text[start:end].strip()
        if body:
            sections.append((heading, body))

    return sections


def _split_rst_sections(text: str) -> list[tuple[str, str]]:
    """Split RST text into (heading, body) sections."""
    lines = text.split("\n")
    sections: list[tuple[str, str]] = []
    current_heading = ""
    current_body: list[str] = []

    i = 0
    while i < len(lines):
        # Check if next line is an RST underline
        if (
            i + 1 < len(lines)
            and len(lines[i + 1]) >= 2
            and len(set(lines[i + 1].strip())) == 1
            and lines[i + 1].strip()[0] in _RST_UNDERLINE_CHARS
            and lines[i].strip()
        ):
            # Save previous section
            body = "\n".join(current_body).strip()
            if body:
                sections.append((current_heading, body))
            current_heading = lines[i].strip()
            current_body = []
            i += 2  # skip heading + underline
            continue

        current_body.append(lines[i])
        i += 1

    # Last section
    body = "\n".join(current_body).strip()
    if body:
        sections.append((current_heading, body))

    return sections if sections else [("", text.strip())]


def parse_markdown(file_path: Path, source: SourceConfig) -> list[ParsedDocument]:
    """Parse a markdown or RST file into section-based documents."""
    text = file_path.read_text(encoding="utf-8", errors="replace")
    if not text.strip():
        return []

    title = file_path.stem.replace("-", " ").replace("_", " ").title()

    if file_path.suffix.lower() == ".rst":
        sections = _split_rst_sections(text)
    else:
        sections = _split_markdown_sections(text)

    docs = []
    for heading, body in sections:
        if not body or len(body) < 20:
            continue
        docs.append(
            ParsedDocument(
                source_id=source.id,
                domain=source.domain,
                uri=str(file_path),
                title=title,
                section=heading,
                content=body,
                source_type=source.source_type,
                priority=source.priority,
                version=source.version,
                tags=list(source.tags),
                component=source.component,
                license=source.license,
                origin=source.location,
            )
        )
    return docs
