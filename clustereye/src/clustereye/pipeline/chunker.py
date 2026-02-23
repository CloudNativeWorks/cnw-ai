"""Heading-aware recursive chunker with code fence protection."""

from __future__ import annotations

import re

from clustereye.config import CHUNK_OVERLAP, CHUNK_SIZE, MIN_CHUNK_LENGTH
from clustereye.pipeline.models import ChunkDoc, ParsedDocument
from clustereye.utils.hashing import make_chunk_id, text_hash
from clustereye.utils.logging import get_logger

log = get_logger(__name__)

# Heading-aware separators (in priority order)
SEPARATORS = [
    "\n## ",
    "\n### ",
    "\n#### ",
    "\n\n",
    "\n",
    ". ",
    " ",
    "",
]

# DB-specific separators for SQL content
SQL_SEPARATORS = [
    "\nGO\n",
    ";\n\n",
    "\n-- ===",
    "\n## ",
    "\n### ",
    "\n\n",
    "\n",
    ". ",
    " ",
    "",
]

# DB-specific separators for config content
CONFIG_SEPARATORS = [
    "\n[",
    "\n# ---",
    "\n\n",
    "\n",
    ". ",
    " ",
    "",
]

# Code fence pattern
_CODE_FENCE_RE = re.compile(r"```[^\n]*\n.*?```", re.DOTALL)


def _protect_code_fences(text: str) -> tuple[str, dict[str, str]]:
    """Replace code fences with placeholders to prevent splitting inside them."""
    placeholders: dict[str, str] = {}
    counter = 0

    def replace(match: re.Match) -> str:
        nonlocal counter
        key = f"\x00CODEFENCE{counter}\x00"
        placeholders[key] = match.group(0)
        counter += 1
        return key

    protected = _CODE_FENCE_RE.sub(replace, text)
    return protected, placeholders


def _restore_code_fences(text: str, placeholders: dict[str, str]) -> str:
    """Restore code fences from placeholders."""
    for key, value in placeholders.items():
        text = text.replace(key, value)
    return text


def _split_text(text: str, chunk_size: int, chunk_overlap: int, separators: list[str] | None = None) -> list[str]:
    """Recursively split text using heading-aware separators."""
    if separators is None:
        separators = SEPARATORS

    if len(text) <= chunk_size:
        return [text]

    for sep in separators:
        if not sep:
            # Last resort: hard split
            chunks = []
            for i in range(0, len(text), chunk_size - chunk_overlap):
                chunk = text[i : i + chunk_size]
                if chunk.strip():
                    chunks.append(chunk)
            return chunks

        parts = text.split(sep)
        if len(parts) <= 1:
            continue

        chunks = []
        current = ""
        for part in parts:
            candidate = current + sep + part if current else part
            if len(candidate) <= chunk_size:
                current = candidate
            else:
                if current.strip():
                    chunks.append(current)
                if len(part) > chunk_size:
                    sub_chunks = _split_text(part, chunk_size, chunk_overlap, separators)
                    chunks.extend(sub_chunks)
                    current = ""
                else:
                    current = part

        if current.strip():
            chunks.append(current)

        if chunks:
            if chunk_overlap > 0 and len(chunks) > 1:
                overlapped = [chunks[0]]
                for i in range(1, len(chunks)):
                    prev_tail = chunks[i - 1][-chunk_overlap:]
                    overlapped.append(prev_tail + chunks[i])
                return overlapped
            return chunks

    return [text]


def _detect_separators(doc: ParsedDocument) -> list[str]:
    """Detect the best separator list based on document content type."""
    uri_lower = doc.uri.lower()
    if uri_lower.endswith(".sql"):
        return SQL_SEPARATORS
    if uri_lower.endswith((".conf", ".cnf", ".cfg", ".ini")):
        return CONFIG_SEPARATORS
    return SEPARATORS


def chunk_documents(
    documents: list[ParsedDocument],
    *,
    chunk_size: int = CHUNK_SIZE,
    chunk_overlap: int = CHUNK_OVERLAP,
    min_length: int = MIN_CHUNK_LENGTH,
) -> list[ChunkDoc]:
    """Split parsed documents into chunks."""
    all_chunks: list[ChunkDoc] = []

    for doc in documents:
        protected, placeholders = _protect_code_fences(doc.content)
        separators = _detect_separators(doc)

        raw_chunks = _split_text(protected, chunk_size, chunk_overlap, separators)

        for i, raw in enumerate(raw_chunks):
            text_content = _restore_code_fences(raw, placeholders).strip()

            if len(text_content) < min_length:
                continue

            # Re-split if restored code fences made the chunk too large
            if len(text_content) > chunk_size * 2:
                sub_chunks = _split_text(text_content, chunk_size, chunk_overlap, separators)
                for j, sub in enumerate(sub_chunks):
                    sub = sub.strip()
                    if len(sub) < min_length:
                        continue
                    chunk = ChunkDoc(
                        chunk_id=make_chunk_id(doc.source_id, doc.uri, doc.section, i * 1000 + j),
                        text=sub,
                        source_id=doc.source_id,
                        domain=doc.domain,
                        source_type=doc.source_type,
                        priority=doc.priority,
                        version=doc.version,
                        uri=doc.uri,
                        title=doc.title,
                        section=doc.section,
                        chunk_index=i * 1000 + j,
                        tags=list(doc.tags),
                        component=doc.component,
                        text_hash=text_hash(sub),
                        license=doc.license,
                        origin=doc.origin,
                        db_engine=doc.db_engine,
                        topic=doc.topic,
                    )
                    all_chunks.append(chunk)
                continue

            chunk = ChunkDoc(
                chunk_id=make_chunk_id(doc.source_id, doc.uri, doc.section, i),
                text=text_content,
                source_id=doc.source_id,
                domain=doc.domain,
                source_type=doc.source_type,
                priority=doc.priority,
                version=doc.version,
                uri=doc.uri,
                title=doc.title,
                section=doc.section,
                chunk_index=i,
                tags=list(doc.tags),
                component=doc.component,
                text_hash=text_hash(text_content),
                license=doc.license,
                origin=doc.origin,
                db_engine=doc.db_engine,
                topic=doc.topic,
            )
            all_chunks.append(chunk)

    log.debug("chunked", input_docs=len(documents), output_chunks=len(all_chunks))
    return all_chunks
