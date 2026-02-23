"""Tests for export/import module."""

from __future__ import annotations

import json
from pathlib import Path

from clustereye.export_import import import_file


def test_import_file_unknown_format(tmp_path):
    """Should raise ValueError for unknown format."""
    bad_file = tmp_path / "data.zip"
    bad_file.write_text("dummy")

    import pytest
    with pytest.raises(ValueError, match="Unknown export format"):
        import_file(bad_file)


def test_import_file_detects_jsonl(tmp_path, monkeypatch):
    """Should detect .jsonl format and call import_jsonl."""
    jsonl_file = tmp_path / "data.jsonl"
    record = {"id": "test-id", "vector": [0.1] * 768, "payload": {"text": "hello"}}
    jsonl_file.write_text(json.dumps(record) + "\n")

    called_with = {}

    def mock_import_jsonl(path, **kwargs):
        called_with["path"] = path
        called_with.update(kwargs)

    monkeypatch.setattr("clustereye.export_import.import_jsonl", mock_import_jsonl)
    import_file(jsonl_file)
    assert called_with["path"] == jsonl_file


def test_import_file_detects_snapshot(tmp_path, monkeypatch):
    """Should detect .snapshot format and call import_snapshot."""
    snapshot_file = tmp_path / "data.snapshot"
    snapshot_file.write_bytes(b"binary snapshot data")

    called_with = {}

    def mock_import_snapshot(path, **kwargs):
        called_with["path"] = path
        called_with.update(kwargs)

    monkeypatch.setattr("clustereye.export_import.import_snapshot", mock_import_snapshot)
    import_file(snapshot_file)
    assert called_with["path"] == snapshot_file
