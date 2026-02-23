#!/bin/bash
set -e

echo "=== cnw-ai Setup ==="

# Check Ollama
if ! command -v ollama &> /dev/null; then
    echo "Ollama bulunamadi. Lutfen yukleyin: https://ollama.com/download"
    exit 1
fi

# Check Qdrant
if ! curl -sf http://localhost:6333/healthz > /dev/null 2>&1; then
    echo "Qdrant calismiyor. Baslatmak icin:"
    echo "  brew services start qdrant"
    echo "  # veya: docker run -p 6333:6333 qdrant/qdrant"
    exit 1
fi
echo "Qdrant: OK"

echo "Ollama modelleri indiriliyor..."
ollama pull qwen3:latest
ollama pull nomic-embed-text

echo "Python bagimliliklari yukleniyor..."
if command -v uv &> /dev/null; then
    uv sync
else
    pip install -e .
fi

echo "=== Setup tamamlandi! ==="
echo ""
echo "Kullanim:"
echo "  1) Veriyi yukle:  python -m cnw_ai --config config/sources.yaml"
echo "  2) Dry run:       python -m cnw_ai --dry-run --verbose"
echo "  3) Chat baslat:   streamlit run src/cnw_ai/app.py"
