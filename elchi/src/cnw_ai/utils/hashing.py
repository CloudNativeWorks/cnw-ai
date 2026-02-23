"""Deterministic hashing for chunk IDs and dedup."""

import hashlib
import re


def make_chunk_id(source_id: str, uri: str, chunk_index: int) -> str:
    """Generate a deterministic chunk ID.

    Format: sha256(source_id::uri::chunk_index)[:24]
    """
    key = f"{source_id}::{uri}::{chunk_index}"
    return hashlib.sha256(key.encode()).hexdigest()[:24]


def text_hash(text: str) -> str:
    """Generate a hash of normalized text for dedup."""
    normalized = re.sub(r"\s+", " ", text.strip().lower())
    return hashlib.sha256(normalized.encode()).hexdigest()[:16]
