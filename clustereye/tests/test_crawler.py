"""Tests for the web crawler module."""

from __future__ import annotations

from clustereye.pipeline.crawler import _canonicalize_url, _is_same_domain


def test_canonicalize_strips_fragment():
    url = "https://example.com/docs/page#section"
    assert _canonicalize_url(url) == "https://example.com/docs/page"


def test_canonicalize_strips_trailing_slash():
    url = "https://example.com/docs/page/"
    assert _canonicalize_url(url) == "https://example.com/docs/page"


def test_canonicalize_keeps_root_slash():
    url = "https://example.com/"
    assert _canonicalize_url(url) == "https://example.com/"


def test_canonicalize_strips_tracking_params():
    url = "https://example.com/page?utm_source=twitter&ref=home&q=search"
    result = _canonicalize_url(url)
    assert "utm_source" not in result
    assert "ref" not in result
    assert "q=search" in result


def test_canonicalize_lowercases():
    url = "HTTPS://Example.COM/Docs"
    result = _canonicalize_url(url)
    assert result == "https://example.com/Docs"


def test_is_same_domain():
    assert _is_same_domain("https://example.com/page", "https://example.com/other")
    assert not _is_same_domain("https://other.com/page", "https://example.com/other")
    assert _is_same_domain("https://Example.COM/page", "https://example.com/other")


def test_canonicalize_idempotent():
    url = "https://example.com/docs/page"
    assert _canonicalize_url(url) == _canonicalize_url(_canonicalize_url(url))
