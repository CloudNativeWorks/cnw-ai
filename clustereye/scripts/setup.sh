#!/usr/bin/env bash
# Python-level setup: install deps, pull Ollama models, verify services
set -euo pipefail

cd "$(dirname "$0")/.."

uv sync

# Pull embedding model
ollama pull nomic-embed-text
# Pull LLM
ollama pull qwen2.5:14b

# Verify Qdrant
curl -sf http://localhost:6333/healthz > /dev/null && echo "Qdrant: OK" || echo "Qdrant: NOT RUNNING"
# Verify Ollama
curl -sf http://localhost:11434/api/tags > /dev/null && echo "Ollama: OK" || echo "Ollama: NOT RUNNING"

echo "Setup complete."
