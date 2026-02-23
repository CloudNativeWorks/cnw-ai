"""Deterministic hashing for chunk IDs and dedup."""

import hashlib
import re


def make_chunk_id(source_id: str, uri: str, section: str, chunk_index: int) -> str:
    """Generate a deterministic chunk ID as UUID string.

    Qdrant requires point IDs to be unsigned int or UUID.
    We take the first 32 hex chars of sha256 and format as UUID.
    Section is included to avoid collisions when multiple ParsedDocuments
    share the same uri (e.g. markdown sections from one file).
    """
    key = f"{source_id}::{uri}::{section}::{chunk_index}"
    h = hashlib.sha256(key.encode()).hexdigest()[:32]
    return f"{h[:8]}-{h[8:12]}-{h[12:16]}-{h[16:20]}-{h[20:32]}"


def text_hash(text: str) -> str:
    """Generate a hash of normalized text for dedup."""
    normalized = re.sub(r"\s+", " ", text.strip().lower())
    return hashlib.sha256(normalized.encode()).hexdigest()[:16]
