"""Tests for the FastAPI API."""

from __future__ import annotations

from unittest.mock import patch, MagicMock

import pytest
from fastapi.testclient import TestClient

from clustereye.api import app


@pytest.fixture
def client():
    return TestClient(app)


def test_health_degraded_when_qdrant_down(client):
    """Health endpoint should return degraded when Qdrant is not available."""
    with patch("qdrant_client.QdrantClient") as mock_cls:
        mock_cls.return_value.get_collection.side_effect = Exception("connection refused")
        resp = client.get("/health")
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "degraded"


def test_ask_endpoint_error_handling(client):
    """Ask endpoint should return 500 on error."""
    with patch("clustereye.rag.ask", side_effect=Exception("model not found")):
        resp = client.post("/ask", json={"question": "test?"})
        assert resp.status_code == 500


def test_ask_endpoint_success(client):
    """Ask endpoint should return answer with sources and timing."""
    mock_result = {
        "answer": "Check pg_stat_replication view.",
        "sources": [{"uri": "test.html", "title": "PG Docs", "section": "replication", "db_engine": "postgres", "topic": "replication"}],
        "timing_ms": 150,
    }
    with patch("clustereye.rag.ask", return_value=mock_result):
        resp = client.post("/ask", json={"question": "How to check replication lag?"})
        assert resp.status_code == 200
        data = resp.json()
        assert data["answer"] == "Check pg_stat_replication view."
        assert len(data["sources"]) == 1
        assert data["timing_ms"] == 150
