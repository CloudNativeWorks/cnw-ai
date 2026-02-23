"""JSONL dataset parser: each line becomes a document."""

from __future__ import annotations

import json
from pathlib import Path

from cnw_ai.pipeline.models import ParsedDocument, SourceConfig


def parse_jsonl(file_path: Path, source: SourceConfig) -> list[ParsedDocument]:
    """Parse a JSONL file into documents (Question/Answer format)."""
    docs = []

    with open(file_path, encoding="utf-8") as f:
        for i, line in enumerate(f):
            line = line.strip()
            if not line:
                continue
            try:
                entry = json.loads(line)
            except json.JSONDecodeError:
                continue

            instruction = entry.get("instruction", "")
            output = entry.get("output", "")
            resource = entry.get("resource", "")
            category = entry.get("category", "")

            content = f"Question: {instruction}\n\nAnswer: {output}"

            tags = list(source.tags)
            if category and category not in tags:
                tags.append(category)

            docs.append(
                ParsedDocument(
                    source_id=source.id,
                    domain=source.domain,
                    uri=f"{file_path}#{i}",
                    title=resource or file_path.stem,
                    section=category,
                    content=content,
                    source_type=source.source_type,
                    priority=source.priority,
                    version=source.version,
                    tags=tags,
                    component=source.component or resource.lower(),
                    license=source.license,
                    origin=source.location,
                )
            )

    return docs
