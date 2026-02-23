"""Embedding provider: Ollama via httpx - no langchain dependency."""

from __future__ import annotations

import httpx

from clustereye.config import EMBED_BATCH_SIZE, EMBEDDING_MODEL, OLLAMA_BASE_URL
from clustereye.pipeline.models import ChunkDoc
from clustereye.utils.logging import get_logger

log = get_logger(__name__)


def _embed_batch_ollama(texts: list[str], model: str, base_url: str) -> list[list[float]]:
    """Embed a batch of texts using Ollama API with automatic retry on smaller batches."""
    try:
        resp = httpx.post(
            f"{base_url}/api/embed",
            json={"model": model, "input": texts},
            timeout=120,
        )
        resp.raise_for_status()
        data = resp.json()
        return data["embeddings"]
    except httpx.HTTPStatusError as exc:
        if exc.response.status_code == 400 and len(texts) > 1:
            log.warning("embed_batch_too_large", count=len(texts), status="retrying_smaller")
            mid = len(texts) // 2
            left = _embed_batch_ollama(texts[:mid], model, base_url)
            right = _embed_batch_ollama(texts[mid:], model, base_url)
            return left + right
        if exc.response.status_code == 400 and len(texts) == 1:
            # Single text too large for model context - progressively truncate
            original_len = len(texts[0])
            for limit in [2000, 1000, 500]:
                truncated = texts[0][:limit]
                log.warning("embed_text_truncated", original_chars=original_len, truncated_to=limit)
                try:
                    resp = httpx.post(
                        f"{base_url}/api/embed",
                        json={"model": model, "input": [truncated]},
                        timeout=120,
                    )
                    resp.raise_for_status()
                    return resp.json()["embeddings"]
                except httpx.HTTPStatusError:
                    continue
            log.error("embed_text_failed", original_chars=original_len, status="skipped")
            raise
        raise


def embed_chunks(
    chunks: list[ChunkDoc],
    *,
    model: str = EMBEDDING_MODEL,
    base_url: str = OLLAMA_BASE_URL,
    batch_size: int = EMBED_BATCH_SIZE,
) -> list[list[float]]:
    """Embed all chunks and return vectors in the same order."""
    all_vectors: list[list[float]] = []
    total = len(chunks)

    for i in range(0, total, batch_size):
        batch = chunks[i : i + batch_size]
        texts = [f"search_document: {c.text}" for c in batch]

        log.info("embedding_batch", batch=f"{i + len(batch)}/{total}")
        vectors = _embed_batch_ollama(texts, model, base_url)
        all_vectors.extend(vectors)

    log.info("embedded", count=len(all_vectors), dim=len(all_vectors[0]) if all_vectors else 0)
    return all_vectors


def detect_embedding_dim(
    model: str = EMBEDDING_MODEL, base_url: str = OLLAMA_BASE_URL
) -> int:
    """Detect embedding dimension by encoding a test string."""
    vectors = _embed_batch_ollama(["test"], model, base_url)
    dim = len(vectors[0])
    log.info("detected_embedding_dim", model=model, dim=dim)
    return dim
