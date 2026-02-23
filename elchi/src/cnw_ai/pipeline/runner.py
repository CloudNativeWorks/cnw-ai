"""Pipeline orchestrator: fetch → parse → chunk → embed → upsert."""

from __future__ import annotations

from cnw_ai.pipeline.chunker import chunk_documents
from cnw_ai.pipeline.embedder import detect_embedding_dim, embed_chunks
from cnw_ai.pipeline.fetcher import fetch
from cnw_ai.pipeline.models import PipelineResult, SourceConfig
from cnw_ai.pipeline.parsers import parse_file
from cnw_ai.pipeline.store import (
    delete_by_source,
    ensure_collection,
    get_client,
    get_existing_hashes,
    upsert_chunks,
)
from cnw_ai.utils.logging import get_logger

log = get_logger(__name__)


class PipelineRunner:
    """Orchestrate the full ingest pipeline."""

    def __init__(
        self,
        sources: list[SourceConfig],
        *,
        dry_run: bool = False,
        reindex: bool = False,
        max_items: int = 0,
    ):
        self.sources = sources
        self.dry_run = dry_run
        self.reindex = reindex
        self.max_items = max_items

    def run(self) -> PipelineResult:
        result = PipelineResult()

        if not self.dry_run:
            client = get_client()
            dim = detect_embedding_dim()
            ensure_collection(client, dim=dim)

        for source in self.sources:
            log.info(
                "processing_source",
                source_id=source.id,
                domain=source.domain,
                source_type=source.source_type,
            )
            try:
                self._process_source(source, result, client if not self.dry_run else None)
            except Exception as e:
                msg = f"[{source.id}] {e}"
                log.error("source_error", source_id=source.id, error=str(e))
                result.errors.append(msg)

            result.sources_processed += 1

        return result

    def _process_source(
        self,
        source: SourceConfig,
        result: PipelineResult,
        client,
    ) -> None:
        # 1. Fetch
        files = fetch(source, max_items=self.max_items)
        result.files_fetched += len(files)
        log.info("files_fetched", source_id=source.id, count=len(files))

        # 2. Parse
        all_docs = []
        for file_path in files:
            docs = parse_file(file_path, source)
            all_docs.extend(docs)
        result.documents_parsed += len(all_docs)
        log.info("documents_parsed", source_id=source.id, count=len(all_docs))

        if not all_docs:
            log.warning("no_documents", source_id=source.id)
            return

        # 3. Chunk
        chunks = chunk_documents(all_docs)
        result.chunks_created += len(chunks)
        log.info("chunks_created", source_id=source.id, count=len(chunks))

        if self.dry_run:
            log.info("dry_run_skip", source_id=source.id, chunks=len(chunks))
            return

        # 4. Dedup: check existing hashes
        if not self.reindex:
            existing_hashes = get_existing_hashes(client, source.id)
            if existing_hashes:
                before = len(chunks)
                chunks = [c for c in chunks if c.text_hash not in existing_hashes]
                skipped = before - len(chunks)
                result.chunks_skipped_dedup += skipped
                if skipped:
                    log.info("dedup_skipped", source_id=source.id, skipped=skipped)
        else:
            # Reindex: delete all existing points for this source
            delete_by_source(client, source.id)

        if not chunks:
            log.info("no_new_chunks", source_id=source.id)
            return

        # 5. Embed
        vectors = embed_chunks(chunks)
        result.chunks_embedded += len(vectors)

        # 6. Upsert
        upserted = upsert_chunks(client, chunks, vectors)
        result.chunks_upserted += upserted
