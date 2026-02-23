"""Tests for chunker module."""

from __future__ import annotations

from clustereye.pipeline.models import ParsedDocument
from clustereye.pipeline.chunker import chunk_documents, _detect_separators, SQL_SEPARATORS, CONFIG_SEPARATORS, SEPARATORS
from clustereye.utils.logging import setup_logging


setup_logging(verbose=False)


def _make_doc(content: str, uri: str = "test.md", **kwargs) -> ParsedDocument:
    defaults = dict(
        source_id="test",
        domain="postgres",
        uri=uri,
        title="Test",
        section="",
        content=content,
        source_type="web",
        priority=1,
        db_engine="postgres",
        topic="monitoring",
    )
    defaults.update(kwargs)
    return ParsedDocument(**defaults)


def test_chunk_short_document():
    doc = _make_doc("Short text that fits in one chunk easily.")
    chunks = chunk_documents([doc], min_length=10)
    assert len(chunks) == 1
    assert chunks[0].db_engine == "postgres"
    assert chunks[0].topic == "monitoring"


def test_chunk_long_document():
    content = "A" * 5000  # Larger than CHUNK_SIZE
    doc = _make_doc(content)
    chunks = chunk_documents([doc])
    assert len(chunks) > 1


def test_chunk_preserves_metadata():
    doc = _make_doc("Content " * 50, db_engine="mongodb", topic="replication")
    chunks = chunk_documents([doc], min_length=10)
    assert len(chunks) >= 1
    for chunk in chunks:
        assert chunk.db_engine == "mongodb"
        assert chunk.topic == "replication"
        assert chunk.text_hash


def test_chunk_skips_short():
    doc = _make_doc("Too short")
    chunks = chunk_documents([doc], min_length=200)
    assert len(chunks) == 0


def test_chunk_code_fence_protection():
    content = """## Section 1

Some text here.

```sql
SELECT * FROM pg_stat_activity
WHERE state = 'active'
ORDER BY duration DESC;
```

More text after code fence.
"""
    doc = _make_doc(content)
    chunks = chunk_documents([doc], min_length=10)
    # Code fence should remain intact in at least one chunk
    all_text = " ".join(c.text for c in chunks)
    assert "```sql" in all_text
    assert "```" in all_text


def test_detect_separators_sql():
    doc = _make_doc("SELECT 1;", uri="test.sql")
    assert _detect_separators(doc) == SQL_SEPARATORS


def test_detect_separators_config():
    doc = _make_doc("key=value", uri="postgresql.conf")
    assert _detect_separators(doc) == CONFIG_SEPARATORS


def test_detect_separators_default():
    doc = _make_doc("hello", uri="readme.md")
    assert _detect_separators(doc) == SEPARATORS
