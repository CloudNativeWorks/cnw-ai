"""RAG pipeline: retrieve relevant docs from Qdrant and query Ollama."""

import re
import time

from langchain_core.output_parsers import StrOutputParser
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.runnables import RunnablePassthrough
from langchain_ollama import ChatOllama, OllamaEmbeddings
from langchain_qdrant import QdrantVectorStore
from qdrant_client import QdrantClient

from clustereye.config import (
    COLLECTION_NAME,
    EMBEDDING_MODEL,
    LLM_MODEL,
    OLLAMA_BASE_URL,
    QDRANT_URL,
    TOP_K,
)

SYSTEM_PROMPT = """You are ClusterEye, an expert AI assistant for database monitoring and operations.
You specialize in PostgreSQL, MongoDB, MySQL, MSSQL, ClickHouse, Elasticsearch, Linux performance,
systemd services, and networking troubleshooting.

Rules:
- Answer directly and concisely based ONLY on the context below.
- Do NOT show your reasoning or thinking process.
- Do NOT speculate or make things up.
- If the context does not contain the answer, say "I don't have information about that."
- When applicable, include specific commands, queries, or configuration examples.

Context:
{context}

Question: {question}

Answer:"""

PROMPT = ChatPromptTemplate.from_template(SYSTEM_PROMPT)

# Regex to strip <think>...</think> from deepseek-r1 responses
_THINK_RE = re.compile(r"<think>.*?</think>", re.DOTALL)


def get_qdrant_client() -> QdrantClient:
    """Get a Qdrant client."""
    return QdrantClient(url=QDRANT_URL)


class _PrefixedEmbeddings(OllamaEmbeddings):
    """OllamaEmbeddings with nomic search_query prefix for queries."""

    def embed_query(self, text: str) -> list[float]:
        return super().embed_query(f"search_query: {text}")


def get_vectorstore() -> QdrantVectorStore:
    """Load existing Qdrant collection."""
    embeddings = _PrefixedEmbeddings(model=EMBEDDING_MODEL, base_url=OLLAMA_BASE_URL)
    return QdrantVectorStore.from_existing_collection(
        collection_name=COLLECTION_NAME,
        embedding=embeddings,
        url=QDRANT_URL,
        content_payload_key="text",
    )


def _format_docs(docs):
    return "\n\n".join(doc.page_content for doc in docs)


def _dedup_and_rerank(docs, boost_factor: float = 0.1):
    """Deduplicate by text_hash, then rerank by priority."""
    seen_hashes = set()
    unique_docs = []
    for doc in docs:
        h = doc.metadata.get("text_hash", "")
        if h and h in seen_hashes:
            continue
        if h:
            seen_hashes.add(h)
        unique_docs.append(doc)

    for doc in unique_docs:
        priority = doc.metadata.get("priority", 3)
        boost = 1 + boost_factor * (5 - priority)
        doc.metadata["_boosted_score"] = boost
    unique_docs.sort(key=lambda d: d.metadata.get("_boosted_score", 1), reverse=True)
    return unique_docs


def _strip_think_tags(text: str) -> str:
    """Strip <think>...</think> blocks from deepseek-r1 responses."""
    return _THINK_RE.sub("", text).strip()


def get_retriever():
    """Get the retriever from vectorstore with priority reranking."""
    vectorstore = get_vectorstore()
    return vectorstore.as_retriever(search_kwargs={"k": TOP_K})


def get_chain():
    """Build the RAG chain using LCEL."""
    retriever = get_retriever()
    llm = ChatOllama(
        model=LLM_MODEL,
        base_url=OLLAMA_BASE_URL,
    )

    chain = (
        {"context": retriever | _dedup_and_rerank | _format_docs, "question": RunnablePassthrough()}
        | PROMPT
        | llm
        | StrOutputParser()
    )
    return chain


def ask(question: str) -> dict:
    """Ask a question and return the answer with source documents and timing."""
    start = time.monotonic()

    retriever = get_retriever()
    chain = get_chain()

    docs = retriever.invoke(question)
    docs = _dedup_and_rerank(docs)
    raw_answer = chain.invoke(question)
    answer = _strip_think_tags(raw_answer)

    elapsed_ms = int((time.monotonic() - start) * 1000)

    sources = []
    for doc in docs:
        sources.append({
            "uri": doc.metadata.get("uri", ""),
            "title": doc.metadata.get("title", ""),
            "section": doc.metadata.get("section", ""),
            "db_engine": doc.metadata.get("db_engine", ""),
            "topic": doc.metadata.get("topic", ""),
        })

    return {
        "answer": answer,
        "sources": sources,
        "timing_ms": elapsed_ms,
    }
