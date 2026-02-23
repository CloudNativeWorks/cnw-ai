"""CLI for the ingest pipeline."""

from __future__ import annotations

import argparse
import sys

from cnw_ai.config import DEFAULT_SOURCES_YAML
from cnw_ai.pipeline.config_loader import filter_sources, load_sources
from cnw_ai.pipeline.models import PipelineResult
from cnw_ai.pipeline.runner import PipelineRunner
from cnw_ai.utils.logging import setup_logging, get_logger


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="cnw-ingest",
        description="Elchi AI ingest pipeline - fetch, parse, chunk, embed, upsert",
    )
    parser.add_argument(
        "--config",
        default=str(DEFAULT_SOURCES_YAML),
        help="Path to sources.yaml config file",
    )
    parser.add_argument(
        "--domains",
        nargs="+",
        help="Filter by domain (e.g., elchi envoy frr)",
    )
    parser.add_argument(
        "--source-ids",
        nargs="+",
        help="Filter by source ID (e.g., elchi-dataset elchi-proto)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Fetch/parse/chunk only, skip embedding and upsert",
    )
    parser.add_argument(
        "--reindex",
        action="store_true",
        help="Delete existing data for selected sources before upserting",
    )
    parser.add_argument(
        "--max-items",
        type=int,
        default=0,
        help="Max files per source (0 = unlimited, useful for testing)",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Enable debug logging",
    )
    return parser


def main(argv: list[str] | None = None) -> None:
    parser = build_parser()
    args = parser.parse_args(argv)

    setup_logging(verbose=args.verbose)
    log = get_logger("cli")

    log.info("loading_config", path=args.config)
    sources = load_sources(args.config)

    sources = filter_sources(
        sources,
        domains=args.domains,
        source_ids=args.source_ids,
    )

    if not sources:
        log.error("no_sources_matched")
        sys.exit(1)

    log.info("running_pipeline", sources=len(sources), dry_run=args.dry_run)

    runner = PipelineRunner(
        sources,
        dry_run=args.dry_run,
        reindex=args.reindex,
        max_items=args.max_items,
    )
    result: PipelineResult = runner.run()

    print("\n" + "=" * 50)
    print("Pipeline Result")
    print("=" * 50)
    print(result.summary())

    if result.errors:
        sys.exit(1)
