"""Shared fixtures for tests."""

import pytest

from cnw_ai.pipeline.models import SourceConfig


@pytest.fixture
def sample_source() -> SourceConfig:
    return SourceConfig(
        id="test-source",
        domain="test",
        priority=1,
        source_type="local",
        location="/tmp/test",
        tags=["test"],
        component="test",
    )


@pytest.fixture
def jsonl_source() -> SourceConfig:
    return SourceConfig(
        id="test-jsonl",
        domain="test",
        priority=1,
        source_type="jsonl",
        location="data/dataset.jsonl",
        tags=["qa"],
        component="general",
    )
