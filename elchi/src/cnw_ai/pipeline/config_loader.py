"""Load and validate sources.yaml configuration."""

from __future__ import annotations

from pathlib import Path

import yaml

from cnw_ai.pipeline.models import SourceConfig
from cnw_ai.utils.logging import get_logger

log = get_logger(__name__)

REQUIRED_FIELDS = {"id", "domain", "priority", "source_type", "location"}
VALID_SOURCE_TYPES = {"git", "web", "local", "jsonl"}


def load_sources(config_path: str | Path) -> list[SourceConfig]:
    """Load and validate source configs from YAML file."""
    config_path = Path(config_path)
    if not config_path.exists():
        raise FileNotFoundError(f"Config file not found: {config_path}")

    with open(config_path, encoding="utf-8") as f:
        raw = yaml.safe_load(f)

    if not raw or "sources" not in raw:
        raise ValueError(f"Config file must contain 'sources' key: {config_path}")

    sources = []
    for i, entry in enumerate(raw["sources"]):
        missing = REQUIRED_FIELDS - set(entry.keys())
        if missing:
            raise ValueError(f"Source #{i} missing required fields: {missing}")

        if entry["source_type"] not in VALID_SOURCE_TYPES:
            raise ValueError(
                f"Source '{entry['id']}' has invalid source_type: {entry['source_type']}. "
                f"Valid types: {VALID_SOURCE_TYPES}"
            )

        source = SourceConfig(
            id=entry["id"],
            domain=entry["domain"],
            priority=entry["priority"],
            source_type=entry["source_type"],
            location=entry["location"],
            branch=entry.get("branch", "main"),
            include_globs=entry.get("include_globs", []),
            exclude_globs=entry.get("exclude_globs", []),
            parser_hint=entry.get("parser_hint", "auto"),
            tags=entry.get("tags", []),
            component=entry.get("component", ""),
            version=entry.get("version", ""),
            license=entry.get("license", ""),
        )
        sources.append(source)
        log.debug("loaded_source", source_id=source.id, source_type=source.source_type)

    log.info("sources_loaded", count=len(sources))
    return sources


def filter_sources(
    sources: list[SourceConfig],
    *,
    domains: list[str] | None = None,
    source_ids: list[str] | None = None,
) -> list[SourceConfig]:
    """Filter sources by domain and/or source_id."""
    filtered = sources
    if domains:
        filtered = [s for s in filtered if s.domain in domains]
    if source_ids:
        filtered = [s for s in filtered if s.id in source_ids]
    return filtered
