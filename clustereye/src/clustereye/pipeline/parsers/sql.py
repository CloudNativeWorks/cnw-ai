"""Block-based SQL file parser.

Splits SQL files by GO, semicolons, or comment dividers.
Each block = one ParsedDocument preserving "SQL snippet + comment" as atomic units.
"""

from __future__ import annotations

import re
from pathlib import Path

from clustereye.pipeline.models import ParsedDocument, SourceConfig

# Block separators in priority order
_GO_RE = re.compile(r"^\s*GO\s*$", re.MULTILINE | re.IGNORECASE)
_DIVIDER_RE = re.compile(r"^--\s*={3,}", re.MULTILINE)
_SEMI_BLANK_RE = re.compile(r";\s*\n\s*\n")

_MIN_BLOCK_LEN = 30


def _split_sql_blocks(text: str) -> list[str]:
    """Split SQL text into blocks using GO, comment dividers, or semicolons."""
    # Try GO separator first (T-SQL style)
    blocks = _GO_RE.split(text)
    if len(blocks) > 1:
        return [b.strip() for b in blocks if b.strip()]

    # Try comment dividers (-- ===)
    blocks = _DIVIDER_RE.split(text)
    if len(blocks) > 1:
        return [b.strip() for b in blocks if b.strip()]

    # Try semicolon + blank line
    blocks = _SEMI_BLANK_RE.split(text)
    if len(blocks) > 1:
        return [b.strip() for b in blocks if b.strip()]

    # No splitting possible, return as single block
    return [text.strip()] if text.strip() else []


def _extract_block_title(block: str) -> str:
    """Extract a title from the first comment or statement in a block."""
    for line in block.split("\n"):
        stripped = line.strip()
        if stripped.startswith("--"):
            title = stripped.lstrip("-").strip()
            if title:
                return title[:100]
        elif stripped and not stripped.startswith("/*"):
            # First non-comment SQL line
            return stripped[:100]
    return "SQL block"


def parse_sql(file_path: Path, source: SourceConfig) -> list[ParsedDocument]:
    """Parse a SQL file into block-based documents."""
    text = file_path.read_text(encoding="utf-8", errors="replace")
    if not text.strip():
        return []

    blocks = _split_sql_blocks(text)
    docs = []

    for i, block in enumerate(blocks):
        if len(block) < _MIN_BLOCK_LEN:
            continue

        title = _extract_block_title(block)

        docs.append(
            ParsedDocument(
                source_id=source.id,
                domain=source.domain,
                uri=f"{file_path}#block={i}",
                title=title,
                section=f"block-{i}",
                content=block,
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
