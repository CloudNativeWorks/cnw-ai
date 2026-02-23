.PHONY: setup ingest ingest-dry serve serve-api serve-ui monitor export export-jsonl import test clean

setup:
	bash scripts/setup.sh

ingest:
	uv run python -m clustereye ingest --config config/sources.yaml

ingest-dry:
	uv run python -m clustereye ingest --dry-run --verbose

serve:
	@echo "Starting FastAPI (port 8000) + Streamlit (port 8501)..."
	uv run python -m clustereye serve & \
	sleep 2 && uv run streamlit run src/clustereye/ui.py --server.port 8501 --server.headless true; \
	kill %1 2>/dev/null

serve-api:
	uv run python -m clustereye serve

serve-ui:
	uv run streamlit run src/clustereye/ui.py --server.port 8501 --server.headless true

monitor:
	bash scripts/monitor.sh

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
