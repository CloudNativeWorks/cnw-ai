.PHONY: setup ingest ingest-dry serve export export-jsonl import test clean

setup:
	bash scripts/setup.sh

ingest:
	uv run python -m clustereye ingest --config config/sources.yaml

ingest-dry:
	uv run python -m clustereye ingest --dry-run --verbose

serve:
	uv run python -m clustereye serve

export:
	uv run python -m clustereye export --method snapshot

export-jsonl:
	uv run python -m clustereye export --method jsonl

import:
	uv run python -m clustereye import data/export/clustereye_docs.snapshot

test:
	uv run pytest tests/ -v

clean:
	rm -rf data/workdir data/export
