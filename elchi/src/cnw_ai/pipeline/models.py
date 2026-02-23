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


@dataclass
class ParsedDocument:
    """A single parsed unit (section, Q&A pair, proto block, etc.)."""

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
    # Proto-specific
    gtype: str = ""
    message: str = ""
    proto_field: str = ""
    deprecated: bool = False
    oneof: str = ""


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
    ingested_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    text_hash: str = ""
    license: str = ""
    origin: str = ""
    # Proto-specific
    gtype: str = ""
    message: str = ""
    proto_field: str = ""
    deprecated: bool = False
    oneof: str = ""

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
        }
        if self.license:
            meta["license"] = self.license
        if self.origin:
            meta["origin"] = self.origin
        if self.gtype:
            meta["gtype"] = self.gtype
        if self.message:
            meta["message"] = self.message
        if self.proto_field:
            meta["proto_field"] = self.proto_field
        if self.deprecated:
            meta["deprecated"] = self.deprecated
        if self.oneof:
            meta["oneof"] = self.oneof

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
