"""Data models for the ingest pipeline."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone


@dataclass
class SourceConfig:
    """A single source definition from sources.yaml."""

    id: str
    domain: str
    priority: int
    source_type: str  # git, web, local, jsonl
    location: str
    branch: str = "main"
    include_globs: list[str] = field(default_factory=list)
    exclude_globs: list[str] = field(default_factory=list)
    parser_hint: str = "auto"
    tags: list[str] = field(default_factory=list)
    component: str = ""
    version: str = ""
    license: str = ""
    db_engine: str = ""
    topic: str = ""
    crawl_depth: int = 0
    rate_limit: float = 2.0


@dataclass
class ParsedDocument:
    """A single parsed unit (section, Q&A pair, config block, etc.)."""

    source_id: str
    domain: str
    uri: str
    title: str
    section: str
    content: str
    source_type: str
    priority: int
    version: str = ""
    tags: list[str] = field(default_factory=list)
    component: str = ""
    license: str = ""
    origin: str = ""
    db_engine: str = ""
    topic: str = ""


@dataclass
class ChunkDoc:
    """A chunk ready for embedding and upserting."""

    chunk_id: str
    text: str
    source_id: str
    domain: str
    source_type: str
    priority: int
    version: str
    uri: str
    title: str
    section: str
    chunk_index: int
    tags: list[str]
    component: str
    db_engine: str = ""
    topic: str = ""
    ingested_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    text_hash: str = ""
    license: str = ""
    origin: str = ""

    def to_payload(self) -> dict:
        """Convert to Qdrant payload dict.

        langchain-qdrant expects: {"text": ..., "metadata": {...}}
        Top-level keys also kept for Qdrant payload filtering.
        """
        meta = {
            "source_id": self.source_id,
            "domain": self.domain,
            "source_type": self.source_type,
            "priority": self.priority,
            "version": self.version,
            "uri": self.uri,
            "title": self.title,
            "section": self.section,
            "chunk_index": self.chunk_index,
            "tags": self.tags,
            "component": self.component,
            "ingested_at": self.ingested_at,
            "text_hash": self.text_hash,
            "db_engine": self.db_engine,
            "topic": self.topic,
        }
        if self.license:
            meta["license"] = self.license
        if self.origin:
            meta["origin"] = self.origin

        # text at top level for content_payload_key="text"
        # metadata nested for metadata_payload_key="metadata"
        # flat copies of filterable fields for Qdrant payload indexes
        payload = {
            "text": self.text,
            "metadata": meta,
            # Flat copies for Qdrant index/filter
            "source_id": self.source_id,
            "domain": self.domain,
            "source_type": self.source_type,
            "priority": self.priority,
            "component": self.component,
            "tags": self.tags,
            "text_hash": self.text_hash,
            "db_engine": self.db_engine,
            "topic": self.topic,
        }
        return payload


@dataclass
class PipelineResult:
    """Summary of a pipeline run."""

    sources_processed: int = 0
    files_fetched: int = 0
    documents_parsed: int = 0
    chunks_created: int = 0
    chunks_embedded: int = 0
    chunks_upserted: int = 0
    chunks_skipped_dedup: int = 0
    errors: list[str] = field(default_factory=list)

    def summary(self) -> str:
        lines = [
            f"Sources processed: {self.sources_processed}",
            f"Files fetched:     {self.files_fetched}",
            f"Documents parsed:  {self.documents_parsed}",
            f"Chunks created:    {self.chunks_created}",
            f"Chunks embedded:   {self.chunks_embedded}",
            f"Chunks upserted:   {self.chunks_upserted}",
            f"Chunks deduped:    {self.chunks_skipped_dedup}",
        ]
        if self.errors:
            lines.append(f"Errors:            {len(self.errors)}")
            for err in self.errors[:10]:
                lines.append(f"  - {err}")
        return "\n".join(lines)

    def merge(self, other: PipelineResult) -> None:
        """Merge another result into this one (for worker aggregation)."""
        self.sources_processed += other.sources_processed
        self.files_fetched += other.files_fetched
        self.documents_parsed += other.documents_parsed
        self.chunks_created += other.chunks_created
        self.chunks_embedded += other.chunks_embedded
        self.chunks_upserted += other.chunks_upserted
        self.chunks_skipped_dedup += other.chunks_skipped_dedup
        self.errors.extend(other.errors)
