"""Export/import for air-gapped deployment.

Primary: Qdrant snapshot (fast, binary, compact)
Fallback: JSONL export/import (cross-version, debugging)
"""

from __future__ import annotations

import json
from pathlib import Path

import httpx
from qdrant_client import QdrantClient, models

from clustereye.config import COLLECTION_NAME, EXPORT_DIR, QDRANT_URL
from clustereye.utils.logging import get_logger

log = get_logger(__name__)


# --- Snapshot export/import (primary) ---

def export_snapshot(
    *,
    collection_name: str = COLLECTION_NAME,
    qdrant_url: str = QDRANT_URL,
    output_path: str | None = None,
) -> Path:
    """Export collection as Qdrant snapshot file."""
    EXPORT_DIR.mkdir(parents=True, exist_ok=True)

    # Create snapshot via REST API
    resp = httpx.post(
        f"{qdrant_url}/collections/{collection_name}/snapshots",
        timeout=300,
    )
    resp.raise_for_status()
    snapshot_name = resp.json()["result"]["name"]

    # Download snapshot
    download_resp = httpx.get(
        f"{qdrant_url}/collections/{collection_name}/snapshots/{snapshot_name}",
        timeout=600,
        follow_redirects=True,
    )
    download_resp.raise_for_status()

    if output_path:
        dest = Path(output_path)
    else:
        dest = EXPORT_DIR / f"{collection_name}.snapshot"

    dest.parent.mkdir(parents=True, exist_ok=True)
    dest.write_bytes(download_resp.content)

    log.info("snapshot_exported", path=str(dest), size_mb=round(len(download_resp.content) / 1024 / 1024, 2))
    return dest


def import_snapshot(
    snapshot_path: Path,
    *,
    collection_name: str = COLLECTION_NAME,
    qdrant_url: str = QDRANT_URL,
) -> None:
    """Import a Qdrant snapshot file."""
    with open(snapshot_path, "rb") as f:
        resp = httpx.post(
            f"{qdrant_url}/collections/{collection_name}/snapshots/upload",
            files={"snapshot": (snapshot_path.name, f, "application/octet-stream")},
            timeout=600,
        )
    resp.raise_for_status()
    log.info("snapshot_imported", path=str(snapshot_path), collection=collection_name)


# --- JSONL export/import (fallback) ---

def export_jsonl(
    *,
    collection_name: str = COLLECTION_NAME,
    qdrant_url: str = QDRANT_URL,
    output_path: str | None = None,
) -> Path:
    """Export collection as JSONL file (streaming, no full-file memory load)."""
    EXPORT_DIR.mkdir(parents=True, exist_ok=True)

    if output_path:
        dest = Path(output_path)
    else:
        dest = EXPORT_DIR / f"{collection_name}.jsonl"

    dest.parent.mkdir(parents=True, exist_ok=True)

    client = QdrantClient(url=qdrant_url)
    offset = None
    count = 0

    with open(dest, "w", encoding="utf-8") as f:
        while True:
            result = client.scroll(
                collection_name=collection_name,
                limit=100,
                offset=offset,
                with_payload=True,
                with_vectors=True,
            )
            points, next_offset = result

            for point in points:
                record = {
                    "id": point.id,
                    "vector": point.vector,
                    "payload": point.payload,
                }
                f.write(json.dumps(record) + "\n")
                count += 1

            if next_offset is None:
                break
            offset = next_offset

    log.info("jsonl_exported", path=str(dest), records=count)
    return dest


def import_jsonl(
    jsonl_path: Path,
    *,
    collection_name: str = COLLECTION_NAME,
    qdrant_url: str = QDRANT_URL,
) -> None:
    """Import collection from JSONL file."""
    client = QdrantClient(url=qdrant_url)

    # Read first line to detect vector dimension
    with open(jsonl_path, encoding="utf-8") as f:
        first_line = f.readline()
        if not first_line.strip():
            log.warning("empty_jsonl", path=str(jsonl_path))
            return
        first_record = json.loads(first_line)
        dim = len(first_record["vector"])

    # Ensure collection exists
    collections = [c.name for c in client.get_collections().collections]
    if collection_name not in collections:
        log.info("creating_collection_for_import", name=collection_name, dim=dim)
        client.create_collection(
            collection_name=collection_name,
            vectors_config=models.VectorParams(
                size=dim,
                distance=models.Distance.COSINE,
            ),
        )

    # Batch upsert
    batch_size = 100
    batch: list[models.PointStruct] = []
    count = 0

    with open(jsonl_path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            record = json.loads(line)
            batch.append(
                models.PointStruct(
                    id=record["id"],
                    vector=record["vector"],
                    payload=record["payload"],
                )
            )

            if len(batch) >= batch_size:
                client.upsert(collection_name=collection_name, points=batch)
                count += len(batch)
                batch.clear()

    if batch:
        client.upsert(collection_name=collection_name, points=batch)
        count += len(batch)

    log.info("jsonl_imported", path=str(jsonl_path), records=count)


def import_file(file_path: Path, **kwargs) -> None:
    """Auto-detect format and import."""
    suffix = file_path.suffix.lower()
    if suffix == ".snapshot":
        import_snapshot(file_path, **kwargs)
    elif suffix == ".jsonl":
        import_jsonl(file_path, **kwargs)
    else:
        raise ValueError(f"Unknown export format: {suffix}. Expected .snapshot or .jsonl")
