"""Smoke tests: verify imports and basic module structure."""

from __future__ import annotations


def test_import_config():
    from clustereye.config import COLLECTION_NAME, EMBEDDING_DIM, LLM_MODEL
    assert COLLECTION_NAME == "clustereye_docs"
    assert EMBEDDING_DIM == 768
    assert LLM_MODEL == "deepseek-r1:8b"


def test_import_models():
    from clustereye.pipeline.models import (
        ChunkDoc,
        ParsedDocument,
        PipelineResult,
        SourceConfig,
    )
    s = SourceConfig(
        id="test", domain="postgres", priority=1,
        source_type="local", location="/tmp",
        db_engine="postgres", topic="monitoring",
    )
    assert s.db_engine == "postgres"
    assert s.topic == "monitoring"
    assert s.crawl_depth == 0
    assert s.rate_limit == 2.0


def test_pipeline_result_merge():
    from clustereye.pipeline.models import PipelineResult

    r1 = PipelineResult(sources_processed=1, files_fetched=5, chunks_created=10)
    r2 = PipelineResult(sources_processed=2, files_fetched=3, chunks_created=7, errors=["err1"])

    r1.merge(r2)
    assert r1.sources_processed == 3
    assert r1.files_fetched == 8
    assert r1.chunks_created == 17
    assert r1.errors == ["err1"]


def test_chunk_doc_payload():
    from clustereye.pipeline.models import ChunkDoc

    chunk = ChunkDoc(
        chunk_id="abc-123",
        text="test content",
        source_id="src1",
        domain="postgres",
        source_type="web",
        priority=1,
        version="1.0",
        uri="test.html",
        title="Test",
        section="section1",
        chunk_index=0,
        tags=["postgres"],
        component="pg",
        db_engine="postgres",
        topic="monitoring",
        text_hash="abcdef1234567890",
    )
    payload = chunk.to_payload()

    assert payload["text"] == "test content"
    assert payload["db_engine"] == "postgres"
    assert payload["topic"] == "monitoring"
    assert payload["metadata"]["db_engine"] == "postgres"
    assert payload["metadata"]["topic"] == "monitoring"


def test_import_utils():
    from clustereye.utils.hashing import make_chunk_id, text_hash
    from clustereye.utils.logging import get_logger, setup_logging

    cid = make_chunk_id("src", "uri", "sec", 0)
    assert len(cid) == 36  # UUID format

    h = text_hash("Hello World")
    assert len(h) == 16

    setup_logging(verbose=False)
    log = get_logger("test")
    assert log is not None


def test_import_parsers():
    from clustereye.pipeline.parsers import get_parser
    from pathlib import Path

    assert get_parser(Path("test.md")) is not None
    assert get_parser(Path("test.html")) is not None
    assert get_parser(Path("test.sql")) is not None
    assert get_parser(Path("test.conf")) is not None
    assert get_parser(Path("test.jsonl")) is not None
    assert get_parser(Path("test.pdf")) is not None
    assert get_parser(Path("test.unknown")) is None
