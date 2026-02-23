"""Pipeline orchestrator: fetch -> parse -> chunk -> embed -> upsert.

Supports --workers for parallel source processing via ThreadPoolExecutor.
"""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed

from clustereye.config import DEFAULT_WORKERS
from clustereye.pipeline.chunker import chunk_documents
from clustereye.pipeline.embedder import detect_embedding_dim, embed_chunks
from clustereye.pipeline.fetcher import fetch
from clustereye.pipeline.models import PipelineResult, SourceConfig
from clustereye.pipeline.parsers import parse_file
from clustereye.pipeline.store import (
    delete_by_source,
    ensure_collection,
    get_client,
    get_existing_hashes,
    upsert_chunks,
)
from clustereye.utils.logging import get_logger

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
        workers: int = DEFAULT_WORKERS,
    ):
        self.sources = sources
        self.dry_run = dry_run
        self.reindex = reindex
        self.max_items = max_items
        self.workers = workers

    def run(self) -> PipelineResult:
        result = PipelineResult()

        if not self.dry_run:
            client = get_client()
            dim = detect_embedding_dim()
            ensure_collection(client, dim=dim)
        else:
            client = None

        if self.workers > 1 and len(self.sources) > 1:
            result = self._run_parallel(client)
        else:
            result = self._run_sequential(client)

        return result

    def _run_sequential(self, client) -> PipelineResult:
        """Process sources sequentially."""
        result = PipelineResult()

        for source in self.sources:
            log.info(
                "processing_source",
                source_id=source.id,
                domain=source.domain,
                source_type=source.source_type,
            )
            try:
                self._process_source(source, result, client)
            except Exception as e:
                msg = f"[{source.id}] {e}"
                log.error("source_error", source_id=source.id, error=str(e))
                result.errors.append(msg)

            result.sources_processed += 1

        return result

    def _run_parallel(self, client) -> PipelineResult:
        """Process sources in parallel using ThreadPoolExecutor."""
        result = PipelineResult()

        def process_one(source: SourceConfig) -> PipelineResult:
            r = PipelineResult()
            try:
                self._process_source(source, r, client)
            except Exception as e:
                msg = f"[{source.id}] {e}"
                log.error("source_error", source_id=source.id, error=str(e))
                r.errors.append(msg)
            r.sources_processed += 1
            return r

        with ThreadPoolExecutor(max_workers=self.workers) as executor:
            futures = {executor.submit(process_one, s): s for s in self.sources}
            for future in as_completed(futures):
                source = futures[future]
                try:
                    sub_result = future.result()
                    result.merge(sub_result)
                except Exception as e:
                    msg = f"[{source.id}] worker error: {e}"
                    log.error("worker_error", source_id=source.id, error=str(e))
                    result.errors.append(msg)
                    result.sources_processed += 1

        return result

    def _process_source(
        self,
        source: SourceConfig,
        result: PipelineResult,
        client,
    ) -> None:
        log.info("step_start", source_id=source.id, step="fetch")

        # 1. Fetch
        files = fetch(source, max_items=self.max_items)
        result.files_fetched += len(files)
        log.info("step_done", source_id=source.id, step="fetch", count=len(files))

        # 2. Parse
        log.info("step_start", source_id=source.id, step="parse")
        all_docs = []
        for file_path in files:
            docs = parse_file(file_path, source)
            all_docs.extend(docs)
        result.documents_parsed += len(all_docs)
        log.info("step_done", source_id=source.id, step="parse", count=len(all_docs))

        if not all_docs:
            log.warning("no_documents", source_id=source.id)
            return

        # 3. Chunk
        log.info("step_start", source_id=source.id, step="chunk")
        chunks = chunk_documents(all_docs)
        result.chunks_created += len(chunks)
        log.info("step_done", source_id=source.id, step="chunk", count=len(chunks))

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
            log.info("step_start", source_id=source.id, step="delete_old")
            delete_by_source(client, source.id)
            log.info("step_done", source_id=source.id, step="delete_old")

        if not chunks:
            log.info("no_new_chunks", source_id=source.id)
            return

        # 5. Embed (retry-safe: skip individual failures)
        log.info("step_start", source_id=source.id, step="embed", chunks=len(chunks))
        try:
            vectors = embed_chunks(chunks)
        except Exception as e:
            log.warning("embed_partial_retry", source_id=source.id, error=str(e))
            # Fall back to one-by-one embedding, skipping failures
            vectors = []
            failed_indices = []
            for idx, chunk in enumerate(chunks):
                try:
                    v = embed_chunks([chunk])
                    vectors.extend(v)
                except Exception:
                    log.warning("embed_chunk_skipped", source_id=source.id, chunk_index=idx)
                    failed_indices.append(idx)
            # Remove failed chunks from list
            for idx in reversed(failed_indices):
                chunks.pop(idx)
        result.chunks_embedded += len(vectors)
        log.info("step_done", source_id=source.id, step="embed", count=len(vectors))

        # 6. Upsert
        log.info("step_start", source_id=source.id, step="upsert", chunks=len(vectors))
        upserted = upsert_chunks(client, chunks, vectors)
        result.chunks_upserted += upserted
        log.info("step_done", source_id=source.id, step="upsert", count=upserted)
