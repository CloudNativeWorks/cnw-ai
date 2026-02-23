"""Tests for the chunker module."""

from cnw_ai.pipeline.chunker import chunk_documents
from cnw_ai.pipeline.models import ParsedDocument


def _make_doc(content: str, source_id: str = "test") -> ParsedDocument:
    return ParsedDocument(
        source_id=source_id,
        domain="test",
        uri="test.md",
        title="Test",
        section="",
        content=content,
        source_type="local",
        priority=1,
    )


def test_short_content_single_chunk():
    doc = _make_doc("Short content that fits in one chunk.")
    chunks = chunk_documents([doc], min_length=10)
    assert len(chunks) == 1
    assert chunks[0].text == "Short content that fits in one chunk."


def test_very_short_content_skipped():
    doc = _make_doc("tiny")
    chunks = chunk_documents([doc], min_length=200)
    assert len(chunks) == 0


def test_heading_split():
    content = "## Section A\n\nContent of section A. " * 30
    content += "\n## Section B\n\nContent of section B. " * 30
    doc = _make_doc(content)
    chunks = chunk_documents([doc], chunk_size=500, chunk_overlap=0, min_length=10)
    assert len(chunks) >= 2


def test_code_fence_protection():
    content = "Some text before.\n\n```python\ndef hello():\n    print('hello world')\n```\n\nSome text after."
    doc = _make_doc(content)
    chunks = chunk_documents([doc], min_length=10)
    # Code fence should not be split
    assert any("```python" in c.text and "```" in c.text[c.text.index("```python") + 1:] for c in chunks)


def test_deterministic_chunk_ids():
    doc = _make_doc("Some content")
    chunks1 = chunk_documents([doc], min_length=10)
    chunks2 = chunk_documents([doc], min_length=10)
    assert chunks1[0].chunk_id == chunks2[0].chunk_id


def test_text_hash_populated():
    doc = _make_doc("Some content for hashing")
    chunks = chunk_documents([doc], min_length=10)
    assert chunks[0].text_hash
    assert len(chunks[0].text_hash) == 16
