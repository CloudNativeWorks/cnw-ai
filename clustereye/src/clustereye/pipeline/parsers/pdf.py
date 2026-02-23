"""PDF parser: extract text using pymupdf (optional dependency)."""

from __future__ import annotations

from pathlib import Path

from clustereye.pipeline.models import ParsedDocument, SourceConfig
from clustereye.utils.logging import get_logger

log = get_logger(__name__)


def parse_pdf(file_path: Path, source: SourceConfig) -> list[ParsedDocument]:
    """Parse a PDF file into documents (one per page or group of pages)."""
    try:
        import pymupdf  # noqa: F811
    except ImportError:
        log.warning("pymupdf_not_installed", hint="Install with: pip install pymupdf")
        return []

    docs = []
    try:
        doc = pymupdf.open(str(file_path))
        for page_num in range(len(doc)):
            page = doc[page_num]
            text = page.get_text().strip()
            if len(text) < 50:
                continue
            docs.append(
                ParsedDocument(
                    source_id=source.id,
                    domain=source.domain,
                    uri=f"{file_path}#page={page_num + 1}",
                    title=file_path.stem,
                    section=f"Page {page_num + 1}",
                    content=text,
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
            )
        doc.close()
    except Exception as e:
        log.warning("pdf_parse_error", path=str(file_path), error=str(e))

    return docs
