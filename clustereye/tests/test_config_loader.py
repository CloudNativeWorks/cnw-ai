"""Tests for config_loader module."""

from __future__ import annotations

import pytest
import yaml


def test_load_sources(tmp_path):
    from clustereye.pipeline.config_loader import load_sources
    from clustereye.utils.logging import setup_logging

    setup_logging(verbose=False)

    config = {
        "sources": [
            {
                "id": "test-pg",
                "domain": "postgres",
                "priority": 1,
                "source_type": "web",
                "location": "https://example.com",
                "db_engine": "postgres",
                "topic": "monitoring",
                "crawl_depth": 2,
                "rate_limit": 5.0,
                "tags": ["postgres"],
            }
        ]
    }

    config_path = tmp_path / "sources.yaml"
    config_path.write_text(yaml.dump(config))

    sources = load_sources(config_path)
    assert len(sources) == 1
    assert sources[0].id == "test-pg"
    assert sources[0].db_engine == "postgres"
    assert sources[0].topic == "monitoring"
    assert sources[0].crawl_depth == 2
    assert sources[0].rate_limit == 5.0


def test_load_sources_missing_fields(tmp_path):
    from clustereye.pipeline.config_loader import load_sources
    from clustereye.utils.logging import setup_logging

    setup_logging(verbose=False)

    config = {"sources": [{"id": "incomplete"}]}
    config_path = tmp_path / "sources.yaml"
    config_path.write_text(yaml.dump(config))

    with pytest.raises(ValueError, match="missing required fields"):
        load_sources(config_path)


def test_load_sources_invalid_type(tmp_path):
    from clustereye.pipeline.config_loader import load_sources
    from clustereye.utils.logging import setup_logging

    setup_logging(verbose=False)

    config = {
        "sources": [
            {
                "id": "bad",
                "domain": "test",
                "priority": 1,
                "source_type": "invalid",
                "location": "/tmp",
            }
        ]
    }
    config_path = tmp_path / "sources.yaml"
    config_path.write_text(yaml.dump(config))

    with pytest.raises(ValueError, match="invalid source_type"):
        load_sources(config_path)


def test_filter_sources():
    from clustereye.pipeline.config_loader import filter_sources
    from clustereye.pipeline.models import SourceConfig

    sources = [
        SourceConfig(id="pg1", domain="postgres", priority=1, source_type="web", location="/"),
        SourceConfig(id="mongo1", domain="mongodb", priority=1, source_type="web", location="/"),
        SourceConfig(id="pg2", domain="postgres", priority=2, source_type="git", location="/"),
    ]

    filtered = filter_sources(sources, domains=["postgres"])
    assert len(filtered) == 2

    filtered = filter_sources(sources, source_ids=["pg1"])
    assert len(filtered) == 1
    assert filtered[0].id == "pg1"

    filtered = filter_sources(sources, domains=["postgres"], source_ids=["pg2"])
    assert len(filtered) == 1
    assert filtered[0].id == "pg2"
