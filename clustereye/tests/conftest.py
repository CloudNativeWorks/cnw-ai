"""Shared test fixtures for ClusterEye tests."""

from __future__ import annotations

import pytest

from clustereye.pipeline.models import SourceConfig


@pytest.fixture
def sample_source() -> SourceConfig:
    """A sample source config for testing."""
    return SourceConfig(
        id="test-source",
        domain="postgres",
        priority=1,
        source_type="local",
        location="/tmp/test",
        tags=["postgres", "test"],
        db_engine="postgres",
        topic="monitoring",
    )


@pytest.fixture
def sample_web_source() -> SourceConfig:
    """A sample web source config with crawl settings."""
    return SourceConfig(
        id="test-web",
        domain="postgres",
        priority=1,
        source_type="web",
        location="https://example.com/docs",
        tags=["postgres"],
        db_engine="postgres",
        topic="monitoring",
        crawl_depth=1,
        rate_limit=10.0,
    )


@pytest.fixture
def tmp_workdir(tmp_path):
    """Create a temporary workdir structure."""
    workdir = tmp_path / "workdir"
    workdir.mkdir()
    return workdir
