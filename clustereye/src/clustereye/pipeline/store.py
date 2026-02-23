"""Qdrant vector store: collection management and upsert."""

from __future__ import annotations

from qdrant_client import QdrantClient, models

from clustereye.config import COLLECTION_NAME, EMBEDDING_DIM, QDRANT_URL, UPSERT_BATCH_SIZE
from clustereye.pipeline.models import ChunkDoc
from clustereye.utils.logging import get_logger

log = get_logger(__name__)


def get_client(url: str = QDRANT_URL) -> QdrantClient:
    """Create a Qdrant client."""
    return QdrantClient(url=url)


def ensure_collection(
    client: QdrantClient,
    collection_name: str = COLLECTION_NAME,
    dim: int = EMBEDDING_DIM,
) -> None:
    """Create collection if it doesn't exist, with payload indexes."""
    collections = [c.name for c in client.get_collections().collections]

    if collection_name in collections:
        log.info("collection_exists", name=collection_name)
        return

    log.info("creating_collection", name=collection_name, dim=dim)
    client.create_collection(
        collection_name=collection_name,
        vectors_config=models.VectorParams(
            size=dim,
            distance=models.Distance.COSINE,
        ),
    )

    # Create payload indexes for filtering
    for field, schema in [
        ("domain", models.PayloadSchemaType.KEYWORD),
        ("source_id", models.PayloadSchemaType.KEYWORD),
        ("source_type", models.PayloadSchemaType.KEYWORD),
        ("component", models.PayloadSchemaType.KEYWORD),
        ("tags", models.PayloadSchemaType.KEYWORD),
        ("priority", models.PayloadSchemaType.INTEGER),
        ("text_hash", models.PayloadSchemaType.KEYWORD),
        ("db_engine", models.PayloadSchemaType.KEYWORD),
        ("topic", models.PayloadSchemaType.KEYWORD),
    ]:
        client.create_payload_index(
            collection_name=collection_name,
            field_name=field,
            field_schema=schema,
        )
    log.info("indexes_created", collection=collection_name)


def delete_by_source(
    client: QdrantClient,
    source_id: str,
    collection_name: str = COLLECTION_NAME,
) -> None:
    """Delete all points for a given source_id (for reindex)."""
    log.info("deleting_source", source_id=source_id)
    client.delete(
        collection_name=collection_name,
        points_selector=models.FilterSelector(
            filter=models.Filter(
                must=[
                    models.FieldCondition(
                        key="source_id",
                        match=models.MatchValue(value=source_id),
                    )
                ]
            )
        ),
    )


def get_existing_hashes(
    client: QdrantClient,
    source_id: str,
    collection_name: str = COLLECTION_NAME,
) -> set[str]:
    """Get existing text_hash values for a source (for dedup)."""
    hashes: set[str] = set()
    offset = None

    while True:
        result = client.scroll(
            collection_name=collection_name,
            scroll_filter=models.Filter(
                must=[
                    models.FieldCondition(
                        key="source_id",
                        match=models.MatchValue(value=source_id),
                    )
                ]
            ),
            limit=1000,
            offset=offset,
            with_payload=["text_hash"],
            with_vectors=False,
        )

        points, next_offset = result
        for point in points:
            h = point.payload.get("text_hash", "")
            if h:
                hashes.add(h)

        if next_offset is None:
            break
        offset = next_offset

    return hashes


def upsert_chunks(
    client: QdrantClient,
    chunks: list[ChunkDoc],
    vectors: list[list[float]],
    collection_name: str = COLLECTION_NAME,
    batch_size: int = UPSERT_BATCH_SIZE,
) -> int:
    """Upsert chunks with their vectors into Qdrant."""
    total = len(chunks)
    upserted = 0

    for i in range(0, total, batch_size):
        batch_chunks = chunks[i : i + batch_size]
        batch_vectors = vectors[i : i + batch_size]

        points = [
            models.PointStruct(
                id=chunk.chunk_id,
                vector=vector,
                payload=chunk.to_payload(),
            )
            for chunk, vector in zip(batch_chunks, batch_vectors)
        ]

        client.upsert(collection_name=collection_name, points=points)
        upserted += len(points)
        log.debug("upserted_batch", batch=f"{i}-{i + len(points)}/{total}")

    log.info("upserted", count=upserted)
    return upserted
