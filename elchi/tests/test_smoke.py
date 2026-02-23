"""Smoke tests: verify imports and basic wiring."""

import importlib


def test_import_pipeline():
    mod = importlib.import_module("cnw_ai.pipeline")
    assert hasattr(mod, "PipelineRunner")
    assert hasattr(mod, "load_sources")


def test_import_cli():
    from cnw_ai.cli import build_parser
    parser = build_parser()
    args = parser.parse_args(["--dry-run", "--verbose"])
    assert args.dry_run is True
    assert args.verbose is True


def test_import_models():
    from cnw_ai.pipeline.models import (
        ChunkDoc,
        ParsedDocument,
        PipelineResult,
        SourceConfig,
    )
    # Verify dataclasses are instantiable
    src = SourceConfig(
        id="t", domain="t", priority=1, source_type="local", location="/tmp"
    )
    assert src.id == "t"

    result = PipelineResult()
    assert result.sources_processed == 0
    assert "Sources processed: 0" in result.summary()


def test_import_parsers():
    from cnw_ai.pipeline.parsers import get_parser
    from pathlib import Path

    assert get_parser(Path("test.md")) is not None
    assert get_parser(Path("test.proto")) is not None
    assert get_parser(Path("test.jsonl")) is not None
    assert get_parser(Path("test.html")) is not None
    assert get_parser(Path("test.go")) is not None
    assert get_parser(Path("test.xyz")) is None


def test_hashing():
    from cnw_ai.utils.hashing import make_chunk_id, text_hash

    cid = make_chunk_id("src", "file.md", 0)
    assert len(cid) == 24

    # Deterministic
    assert make_chunk_id("src", "file.md", 0) == cid

    # Different inputs → different ids
    assert make_chunk_id("src", "file.md", 1) != cid

    h = text_hash("Hello World")
    assert len(h) == 16
    # Normalized: whitespace insensitive
    assert text_hash("  hello   world  ") == h


def test_config():
    from cnw_ai.config import (
        COLLECTION_NAME,
        QDRANT_URL,
        EMBEDDING_MODEL,
        CHUNK_SIZE,
    )
    assert COLLECTION_NAME == "elchi_docs"
    assert "6333" in QDRANT_URL
    assert EMBEDDING_MODEL == "nomic-embed-text"
    assert CHUNK_SIZE > 0
