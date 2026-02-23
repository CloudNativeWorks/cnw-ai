"""Streamlit chat UI for Elchi/Envoy RAG assistant."""

import re
import time

import streamlit as st

from cnw_ai.config import COLLECTION_NAME, LLM_MODEL, QDRANT_URL
from cnw_ai.rag import get_chain, get_qdrant_client, get_retriever

st.set_page_config(page_title="Elchi AI Assistant", page_icon="🛡️", layout="wide")

# Sidebar
with st.sidebar:
    st.title("Elchi AI Assistant")
    st.markdown("Expert RAG system for Envoy/Elchi")
    st.divider()
    st.markdown(f"**Model:** `{LLM_MODEL}`")
    st.markdown(f"**Collection:** `{COLLECTION_NAME}`")

    try:
        qclient = get_qdrant_client()
        info = qclient.get_collection(COLLECTION_NAME)
        st.markdown(f"**Documents:** `{info.points_count}` chunks")

        with st.expander("Source info"):
            result = qclient.scroll(
                collection_name=COLLECTION_NAME,
                limit=1,
                with_payload=["domain", "source_id"],
                with_vectors=False,
            )
            if result[0]:
                st.markdown("Qdrant connected")
    except Exception:
        st.warning("Qdrant not available. Run `python -m cnw_ai` first.")

    st.divider()
    st.markdown("**Example questions:**")
    st.markdown("- What is WAF and how to configure it?")
    st.markdown("- How to create a Listener?")
    st.markdown("- How does GSLB work?")
    st.markdown("- How to configure Routes?")

# Chat state
if "messages" not in st.session_state:
    st.session_state.messages = []

if "chain" not in st.session_state:
    try:
        st.session_state.chain = get_chain()
        st.session_state.retriever = get_retriever()
    except Exception as e:
        st.error(f"Failed to initialize RAG chain: {e}")
        st.stop()

# Display chat history
for message in st.session_state.messages:
    with st.chat_message(message["role"]):
        st.markdown(message["content"])

# Chat input
if prompt := st.chat_input("Ask a question about Elchi/Envoy..."):
    st.session_state.messages.append({"role": "user", "content": prompt})
    with st.chat_message("user"):
        st.markdown(prompt)

    with st.chat_message("assistant"):
        # Retrieval
        with st.spinner("Retrieving..."):
            t0 = time.perf_counter()
            sources = st.session_state.retriever.invoke(prompt)
            t_retrieval = time.perf_counter() - t0

        # LLM generation
        with st.spinner("Generating..."):
            t0 = time.perf_counter()
            answer = st.session_state.chain.invoke(prompt)
            t_llm = time.perf_counter() - t0

        # Strip <think>...</think> blocks from deepseek-r1
        raw_answer = answer
        answer = re.sub(r"<think>.*?</think>", "", answer, flags=re.DOTALL).strip()

        st.markdown(answer)

        # Performance metrics
        with st.expander("Performance", expanded=False):
            col1, col2, col3 = st.columns(3)
            col1.metric("Retrieval", f"{t_retrieval:.2f}s")
            col2.metric("LLM", f"{t_llm:.1f}s")
            col3.metric("Total", f"{t_retrieval + t_llm:.1f}s")

            answer_tokens = len(answer.split())
            if t_llm > 0:
                st.caption(f"~{answer_tokens} words | ~{answer_tokens / t_llm:.1f} words/sec")

            # Show if thinking was stripped
            think_match = re.search(r"<think>(.*?)</think>", raw_answer, flags=re.DOTALL)
            if think_match:
                thinking_text = think_match.group(1).strip()
                if thinking_text:
                    st.markdown("**Model thinking (hidden from answer):**")
                    st.text(thinking_text[:500])

        # Sources
        if sources:
            with st.expander("Sources", expanded=False):
                for i, doc in enumerate(sources, 1):
                    domain = doc.metadata.get("domain", "?")
                    source_id = doc.metadata.get("source_id", "?")
                    component = doc.metadata.get("component", "")
                    section = doc.metadata.get("section", "")
                    priority = doc.metadata.get("priority", "?")
                    label = f"`{domain}` / `{source_id}`"
                    if component:
                        label += f" / `{component}`"
                    if section:
                        label += f" - {section}"
                    label += f" (p{priority})"
                    st.markdown(f"**{i}.** {label}")
                    st.caption(doc.page_content[:300] + "...")

            # Context quality indicator
            with st.expander("Context sent to LLM", expanded=False):
                total_chars = sum(len(d.page_content) for d in sources)
                unique_sources = len(set(d.metadata.get("source_id", "") for d in sources))
                st.caption(f"{len(sources)} chunks | {total_chars:,} chars | {unique_sources} unique sources")
                for i, doc in enumerate(sources, 1):
                    st.markdown(f"---\n**Chunk {i}** ({len(doc.page_content)} chars)")
                    st.code(doc.page_content[:500], language=None)

    st.session_state.messages.append({"role": "assistant", "content": answer})
