"""RAG pipeline: retrieve relevant docs from Qdrant and query Ollama."""

from langchain_core.output_parsers import StrOutputParser
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.runnables import RunnablePassthrough
from langchain_ollama import ChatOllama, OllamaEmbeddings
from langchain_qdrant import QdrantVectorStore
from qdrant_client import QdrantClient

from cnw_ai.config import (
    COLLECTION_NAME,
    EMBEDDING_MODEL,
    LLM_MODEL,
    OLLAMA_BASE_URL,
    QDRANT_URL,
    TOP_K,
)

SYSTEM_PROMPT = """You are an expert AI assistant on Elchi and Envoy Proxy.
Elchi is an Envoy proxy management platform. It manages WAF, GSLB, Listeners, Routes, Clusters,
Certificates, Secrets and many other components.

Rules:
- Answer directly and concisely based ONLY on the context below.
- Do NOT show your reasoning or thinking process.
- Do NOT speculate or make things up.
- If the context does not contain the answer, say "I don't have information about that."

Context:
{context}

Question: {question}

Answer:"""

PROMPT = ChatPromptTemplate.from_template(SYSTEM_PROMPT)


def get_qdrant_client() -> QdrantClient:
    """Get a Qdrant client."""
    return QdrantClient(url=QDRANT_URL)


def get_vectorstore() -> QdrantVectorStore:
    """Load existing Qdrant collection."""
    embeddings = OllamaEmbeddings(model=EMBEDDING_MODEL, base_url=OLLAMA_BASE_URL)
    return QdrantVectorStore.from_existing_collection(
        collection_name=COLLECTION_NAME,
        embedding=embeddings,
        url=QDRANT_URL,
        content_payload_key="text",
    )


def _format_docs(docs):
    return "\n\n".join(doc.page_content for doc in docs)


def _rerank_by_priority(docs, boost_factor: float = 0.1):
    """Rerank documents by priority (lower priority number = higher boost)."""
    for doc in docs:
        priority = doc.metadata.get("priority", 3)
        # Priority 1 gets 1.4x, priority 2 gets 1.3x, etc.
        boost = 1 + boost_factor * (5 - priority)
        doc.metadata["_boosted_score"] = boost
    docs.sort(key=lambda d: d.metadata.get("_boosted_score", 1), reverse=True)
    return docs


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
        {"context": retriever | _rerank_by_priority | _format_docs, "question": RunnablePassthrough()}
        | PROMPT
        | llm
        | StrOutputParser()
    )
    return chain


def ask(question: str) -> dict:
    """Ask a question and return the answer with source documents."""
    retriever = get_retriever()
    chain = get_chain()

    docs = retriever.invoke(question)
    docs = _rerank_by_priority(docs)
    answer = chain.invoke(question)
    return {"answer": answer, "source_documents": docs}
