"""Tests for the config loader."""

from pathlib import Path

import pytest

from cnw_ai.pipeline.config_loader import filter_sources, load_sources


def _write_yaml(tmp_path: Path, content: str) -> Path:
    p = tmp_path / "sources.yaml"
    p.write_text(content)
    return p


def test_load_valid_config(tmp_path):
    p = _write_yaml(tmp_path, """
sources:
  - id: test-src
    domain: test
    priority: 1
    source_type: local
    location: /tmp/test
    tags: [a, b]
""")
    sources = load_sources(p)
    assert len(sources) == 1
    assert sources[0].id == "test-src"
    assert sources[0].domain == "test"
    assert sources[0].tags == ["a", "b"]


def test_missing_required_field(tmp_path):
    p = _write_yaml(tmp_path, """
sources:
  - id: bad
    domain: test
""")
    with pytest.raises(ValueError, match="missing required"):
        load_sources(p)


def test_invalid_source_type(tmp_path):
    p = _write_yaml(tmp_path, """
sources:
  - id: bad
    domain: test
    priority: 1
    source_type: ftp
    location: /tmp
""")
    with pytest.raises(ValueError, match="invalid source_type"):
        load_sources(p)


def test_file_not_found():
    with pytest.raises(FileNotFoundError):
        load_sources("/nonexistent/path.yaml")


def test_filter_by_domain(tmp_path):
    p = _write_yaml(tmp_path, """
sources:
  - id: a
    domain: elchi
    priority: 1
    source_type: local
    location: /tmp/a
  - id: b
    domain: envoy
    priority: 2
    source_type: local
    location: /tmp/b
""")
    sources = load_sources(p)
    filtered = filter_sources(sources, domains=["elchi"])
    assert len(filtered) == 1
    assert filtered[0].id == "a"


def test_filter_by_source_id(tmp_path):
    p = _write_yaml(tmp_path, """
sources:
  - id: a
    domain: elchi
    priority: 1
    source_type: local
    location: /tmp/a
  - id: b
    domain: elchi
    priority: 1
    source_type: local
    location: /tmp/b
""")
    sources = load_sources(p)
    filtered = filter_sources(sources, source_ids=["b"])
    assert len(filtered) == 1
    assert filtered[0].id == "b"
