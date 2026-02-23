"""HTML parser: extract main content, convert to text."""

from __future__ import annotations

from pathlib import Path

from bs4 import BeautifulSoup
from markdownify import markdownify

from clustereye.pipeline.models import ParsedDocument, SourceConfig


# Tags to remove entirely
_REMOVE_TAGS = {"nav", "footer", "header", "aside", "script", "style", "noscript", "form"}

# Minimum length for a text block to be useful
_MIN_BLOCK_LEN = 50


def _extract_main_content(soup: BeautifulSoup) -> str:
    """Extract main content, preferring article/main tags."""
    for tag_name in _REMOVE_TAGS:
        for tag in soup.find_all(tag_name):
            tag.decompose()

    for selector in ["article", "main", '[role="main"]']:
        content = soup.find(selector)
        if content and len(content.get_text(strip=True)) > _MIN_BLOCK_LEN:
            return markdownify(str(content), heading_style="ATX", strip=["img"])

    body = soup.find("body")
    if body:
        return markdownify(str(body), heading_style="ATX", strip=["img"])

    return markdownify(str(soup), heading_style="ATX", strip=["img"])


def _filter_short_blocks(text: str) -> str:
    """Remove very short paragraphs (likely menu items / breadcrumbs)."""
    paragraphs = text.split("\n\n")
    filtered = []
    for p in paragraphs:
        stripped = p.strip()
        if not stripped:
            continue
        if stripped.startswith("#"):
            filtered.append(stripped)
            continue
        if len(stripped) >= _MIN_BLOCK_LEN:
            filtered.append(stripped)
    return "\n\n".join(filtered)


def parse_html(file_path: Path, source: SourceConfig) -> list[ParsedDocument]:
    """Parse an HTML file into a document."""
    text = file_path.read_text(encoding="utf-8", errors="replace")
    if not text.strip():
        return []

    soup = BeautifulSoup(text, "html.parser")

    title_tag = soup.find("title")
    title = title_tag.get_text(strip=True) if title_tag else file_path.stem

    content = _extract_main_content(soup)
    content = _filter_short_blocks(content)

    if not content or len(content) < _MIN_BLOCK_LEN:
        return []

    return [
        ParsedDocument(
            source_id=source.id,
            domain=source.domain,
            uri=str(file_path),
            title=title,
            section="",
            content=content,
            source_type=source.source_type,
            priority=source.priority,
            version=source.version,
            tags=list(source.tags),
            component=source.component,
            license=source.license,
            origin=source.location,
            db_engine=source.db_engine,
            topic=source.topic,
        )
    ]
