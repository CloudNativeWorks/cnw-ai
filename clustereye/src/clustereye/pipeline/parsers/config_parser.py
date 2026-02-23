"""Block-based config file parser.

Splits config files (postgresql.conf, my.cnf, elasticsearch.yml) by
[section] headers, comment dividers, or blank lines between parameter groups.
Each block = "config parameter group + comment explanation" as atomic unit.
"""

from __future__ import annotations

import re
from pathlib import Path

from clustereye.pipeline.models import ParsedDocument, SourceConfig

# Section header pattern: [section_name]
_SECTION_RE = re.compile(r"^\[([^\]]+)\]", re.MULTILINE)
# Comment divider: # ---
_COMMENT_DIVIDER_RE = re.compile(r"^#\s*-{3,}", re.MULTILINE)

_MIN_BLOCK_LEN = 30


def _split_config_blocks(text: str) -> list[tuple[str, str]]:
    """Split config text into (section_name, body) blocks."""
    # Try [section] headers first (INI-style)
    matches = list(_SECTION_RE.finditer(text))
    if matches:
        blocks = []
        # Text before first section
        preamble = text[: matches[0].start()].strip()
        if preamble:
            blocks.append(("global", preamble))

        for i, match in enumerate(matches):
            section_name = match.group(1).strip()
            start = match.end()
            end = matches[i + 1].start() if i + 1 < len(matches) else len(text)
            body = text[start:end].strip()
            if body:
                blocks.append((section_name, body))
        return blocks

    # Try comment dividers (# ---)
    parts = _COMMENT_DIVIDER_RE.split(text)
    if len(parts) > 1:
        blocks = []
        for i, part in enumerate(parts):
            stripped = part.strip()
            if stripped:
                # Extract section name from first comment line
                name = f"section-{i}"
                for line in stripped.split("\n"):
                    line = line.strip()
                    if line.startswith("#") and len(line) > 2:
                        name = line.lstrip("# ").strip()[:80]
                        break
                blocks.append((name, stripped))
        return blocks

    # Try blank-line separation (parameter groups)
    groups = re.split(r"\n\s*\n", text)
    if len(groups) > 1:
        blocks = []
        for i, group in enumerate(groups):
            stripped = group.strip()
            if stripped:
                name = f"group-{i}"
                for line in stripped.split("\n"):
                    line = line.strip()
                    if line.startswith("#") and len(line) > 2:
                        name = line.lstrip("# ").strip()[:80]
                        break
                blocks.append((name, stripped))
        return blocks

    # Single block
    return [("config", text.strip())] if text.strip() else []


def parse_config(file_path: Path, source: SourceConfig) -> list[ParsedDocument]:
    """Parse a config file into block-based documents."""
    text = file_path.read_text(encoding="utf-8", errors="replace")
    if not text.strip():
        return []

    blocks = _split_config_blocks(text)
    docs = []

    for i, (section_name, body) in enumerate(blocks):
        if len(body) < _MIN_BLOCK_LEN:
            continue

        docs.append(
            ParsedDocument(
                source_id=source.id,
                domain=source.domain,
                uri=f"{file_path}#section={i}",
                title=f"{file_path.name} - {section_name}",
                section=section_name,
                content=body,
                source_type=source.source_type,
                priority=source.priority,
                version=source.version,
                tags=list(source.tags),
                component=source.component,
                license=source.license,
                origin=source.location,
                db_engine=source.db_engine,
                topic=source.topic,
            )
        )

    return docs
