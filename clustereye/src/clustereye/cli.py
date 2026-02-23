"""CLI: ingest, export, import, serve subcommands."""

from __future__ import annotations

import argparse
import sys

from clustereye.utils.logging import setup_logging, get_logger

log = get_logger(__name__)


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="clustereye",
        description="ClusterEye - Offline AI assistant for database monitoring",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    # ingest
    ingest = sub.add_parser("ingest", help="Ingest documentation sources")
    ingest.add_argument("--config", default=None, help="Path to sources.yaml")
    ingest.add_argument("--workers", type=int, default=1, help="Parallel workers for source processing")
    ingest.add_argument("--dry-run", action="store_true", help="Parse and chunk without embedding/upserting")
    ingest.add_argument("--reindex", action="store_true", help="Delete existing data before re-ingesting")
    ingest.add_argument("--verbose", action="store_true", help="Enable debug logging")
    ingest.add_argument("--domains", nargs="+", help="Only process these domains")
    ingest.add_argument("--sources", nargs="+", help="Only process these source IDs")

    # serve
    serve = sub.add_parser("serve", help="Start the FastAPI server")
    serve.add_argument("--host", default="0.0.0.0", help="Bind host")
    serve.add_argument("--port", type=int, default=8000, help="Bind port")
    serve.add_argument("--verbose", action="store_true", help="Enable debug logging")

    # export
    export = sub.add_parser("export", help="Export collection for air-gapped deployment")
    export.add_argument("--output", default=None, help="Output file path")
    export.add_argument("--method", choices=["snapshot", "jsonl"], default="snapshot", help="Export method")
    export.add_argument("--verbose", action="store_true", help="Enable debug logging")

    # import
    import_cmd = sub.add_parser("import", help="Import collection from export file")
    import_cmd.add_argument("file", help="Path to snapshot or JSONL file")
    import_cmd.add_argument("--verbose", action="store_true", help="Enable debug logging")

    return parser


def main_ingest():
    """Entry point for clustereye-ingest."""
    args = _build_parser().parse_args(["ingest"] + sys.argv[1:])
    _run_ingest(args)


def main_serve():
    """Entry point for clustereye-serve."""
    args = _build_parser().parse_args(["serve"] + sys.argv[1:])
    _run_serve(args)


def main_export():
    """Entry point for clustereye-export."""
    args = _build_parser().parse_args(["export"] + sys.argv[1:])
    _run_export(args)


def main_import():
    """Entry point for clustereye-import."""
    args = _build_parser().parse_args(["import"] + sys.argv[1:])
    _run_import(args)


def main(argv: list[str] | None = None):
    """Main CLI entry point."""
    parser = _build_parser()
    args = parser.parse_args(argv)

    if args.command == "ingest":
        _run_ingest(args)
    elif args.command == "serve":
        _run_serve(args)
    elif args.command == "export":
        _run_export(args)
    elif args.command == "import":
        _run_import(args)


def _run_ingest(args):
    setup_logging(verbose=args.verbose)

    from clustereye.config import DEFAULT_SOURCES_YAML
    from clustereye.pipeline.config_loader import filter_sources, load_sources
    from clustereye.pipeline.runner import PipelineRunner

    config_path = args.config or str(DEFAULT_SOURCES_YAML)
    sources = load_sources(config_path)
    sources = filter_sources(sources, domains=args.domains, source_ids=args.sources)

    if not sources:
        log.error("no_sources_matched")
        sys.exit(1)

    log.info("starting_ingest", sources=len(sources), dry_run=args.dry_run, workers=args.workers)

    runner = PipelineRunner(
        sources,
        dry_run=args.dry_run,
        reindex=args.reindex,
        workers=args.workers,
    )
    result = runner.run()
    print(result.summary())

    if result.errors:
        sys.exit(1)


def _run_serve(args):
    setup_logging(verbose=args.verbose)

    import uvicorn
    from clustereye.api import app

    log.info("starting_server", host=args.host, port=args.port)
    uvicorn.run(app, host=args.host, port=args.port)


def _run_export(args):
    setup_logging(verbose=args.verbose)

    from clustereye.export_import import export_snapshot, export_jsonl

    if args.method == "snapshot":
        path = export_snapshot(output_path=args.output)
    else:
        path = export_jsonl(output_path=args.output)

    print(f"Exported to: {path}")


def _run_import(args):
    setup_logging(verbose=args.verbose)

    from pathlib import Path
    from clustereye.export_import import import_file

    file_path = Path(args.file)
    if not file_path.exists():
        log.error("file_not_found", path=str(file_path))
        sys.exit(1)

    import_file(file_path)
    print(f"Imported from: {file_path}")
