"""FastAPI REST API for ClusterEye."""

from __future__ import annotations

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

from clustereye.config import COLLECTION_NAME, QDRANT_URL
from clustereye.utils.logging import get_logger

log = get_logger(__name__)

app = FastAPI(
    title="ClusterEye",
    description="Offline AI assistant for database monitoring",
    version="0.1.0",
)


class AskRequest(BaseModel):
    question: str


class AskResponse(BaseModel):
    answer: str
    sources: list[dict]
    timing_ms: int
    stats: dict = {}


class HealthResponse(BaseModel):
    status: str
    qdrant: str
    points_count: int


class StatsResponse(BaseModel):
    collection: str
    points_count: int
    vectors_count: int
    segments_count: int


@app.post("/ask", response_model=AskResponse)
def ask_endpoint(request: AskRequest):
    """Ask a question about database monitoring."""
    from clustereye.rag import ask

    try:
        result = ask(request.question)
        return AskResponse(
            answer=result["answer"],
            sources=result["sources"],
            timing_ms=result["timing_ms"],
            stats=result.get("stats", {}),
        )
    except Exception as e:
        log.error("ask_error", error=str(e))
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/health", response_model=HealthResponse)
def health_endpoint():
    """Check Qdrant connection and collection status."""
    from qdrant_client import QdrantClient

    try:
        client = QdrantClient(url=QDRANT_URL)
        info = client.get_collection(COLLECTION_NAME)
        return HealthResponse(
            status="ok",
            qdrant="connected",
            points_count=info.points_count,
        )
    except Exception as e:
        return HealthResponse(
            status="degraded",
            qdrant=str(e),
            points_count=0,
        )


@app.get("/stats", response_model=StatsResponse)
def stats_endpoint():
    """Get collection statistics."""
    from qdrant_client import QdrantClient

    try:
        client = QdrantClient(url=QDRANT_URL)
        info = client.get_collection(COLLECTION_NAME)
        return StatsResponse(
            collection=COLLECTION_NAME,
            points_count=info.points_count,
            vectors_count=info.vectors_count,
            segments_count=len(info.segments or []),
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/sources")
def sources_endpoint():
    """List indexed sources from the collection."""
    from qdrant_client import QdrantClient

    try:
        client = QdrantClient(url=QDRANT_URL)
        # Scroll through unique source_ids
        sources: dict[str, dict] = {}
        offset = None

        while True:
            result = client.scroll(
                collection_name=COLLECTION_NAME,
                limit=100,
                offset=offset,
                with_payload=["source_id", "domain", "db_engine", "topic"],
                with_vectors=False,
            )
            points, next_offset = result
            for point in points:
                sid = point.payload.get("source_id", "")
                if sid and sid not in sources:
                    sources[sid] = {
                        "source_id": sid,
                        "domain": point.payload.get("domain", ""),
                        "db_engine": point.payload.get("db_engine", ""),
                        "topic": point.payload.get("topic", ""),
                    }
            if next_offset is None:
                break
            offset = next_offset

        return {"sources": list(sources.values())}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
