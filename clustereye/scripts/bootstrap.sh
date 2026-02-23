#!/usr/bin/env bash
# ClusterEye Bootstrap - Install OS-level prerequisites
# Supports: Ubuntu (apt) and macOS (brew)
# Usage: bash scripts/bootstrap.sh

set -euo pipefail

OS="$(uname -s)"

install_ubuntu() {
    sudo apt update && sudo apt install -y \
        curl git python3.11 python3.11-venv python3-pip build-essential

    # uv (Python package manager)
    if ! command -v uv &>/dev/null; then
        curl -LsSf https://astral.sh/uv/install.sh | sh
    fi

    # Ollama
    if ! command -v ollama &>/dev/null; then
        curl -fsSL https://ollama.com/install.sh | sh
    fi

    # Qdrant (Docker)
    if ! command -v docker &>/dev/null; then
        sudo apt install -y docker.io
        sudo systemctl enable --now docker
    fi
    docker pull qdrant/qdrant:latest
    # Start Qdrant if not running
    if ! docker ps | grep -q qdrant; then
        docker run -d --name qdrant -p 6333:6333 -p 6334:6334 \
            -v "$(pwd)/data/qdrant_storage:/qdrant/storage" \
            qdrant/qdrant:latest
    fi
}

install_macos() {
    # Homebrew
    if ! command -v brew &>/dev/null; then
        /bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"
    fi

    brew install python@3.11 git

    # uv
    if ! command -v uv &>/dev/null; then
        curl -LsSf https://astral.sh/uv/install.sh | sh
    fi

    # Ollama
    if ! command -v ollama &>/dev/null; then
        brew install ollama
    fi

    # Qdrant
    if ! brew services list | grep -q qdrant; then
        brew install qdrant
    fi
    brew services start qdrant
}

case "$OS" in
    Linux)  install_ubuntu ;;
    Darwin) install_macos ;;
    *)      echo "Unsupported OS: $OS"; exit 1 ;;
esac

echo "Bootstrap complete. Next: bash scripts/setup.sh"
