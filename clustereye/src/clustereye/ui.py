"""Streamlit chat UI for ClusterEye."""

import streamlit as st
import httpx

API_URL = "http://localhost:8000"

st.set_page_config(
    page_title="ClusterEye",
    page_icon="🔍",  # noqa
    layout="wide",
)


def render_stats(stats, timing_ms):
    """Render query statistics as a nice info box."""
    if not stats:
        st.caption(f"Total: {timing_ms}ms")
        return

    retrieval = stats.get("retrieval_ms", 0)
    rerank = stats.get("rerank_ms", 0)
    llm = stats.get("llm_ms", 0)
    docs = stats.get("docs_retrieved", 0)
    ctx_chars = stats.get("context_chars", 0)
    ctx_tokens = stats.get("context_tokens_est", 0)
    engines = stats.get("db_engines", [])
    model = stats.get("model", "")

    total_sec = timing_ms / 1000

    cols = st.columns(4)
    with cols[0]:
        st.metric("Total", f"{total_sec:.1f}s")
    with cols[1]:
        st.metric("LLM", f"{llm / 1000:.1f}s")
    with cols[2]:
        st.metric("Retrieval", f"{retrieval}ms")
    with cols[3]:
        st.metric("Docs", f"{docs}")

    detail = f"Model: `{model}` | Context: ~{ctx_tokens} tokens ({ctx_chars} chars) | Rerank: {rerank}ms"
    if engines:
        detail += f" | Engines: {', '.join(engines)}"
    st.caption(detail)


def render_sources(sources):
    """Render source documents."""
    if not sources:
        return
    with st.expander(f"Sources ({len(sources)})"):
        for src in sources:
            label = src.get("title") or src.get("uri", "")
            uri = src.get("uri", "")
            engine = src.get("db_engine", "")
            topic = src.get("topic", "")
            tag = f"`{engine}`" if engine else ""
            if topic:
                tag += f" `{topic}`"
            if uri.startswith("http"):
                st.markdown(f"- [{label}]({uri}) {tag}")
            elif label:
                st.markdown(f"- {label} {tag}")


# --- Sidebar ---
with st.sidebar:
    st.title("ClusterEye")
    st.caption("Offline AI assistant for database monitoring")

    # Health check
    try:
        health = httpx.get(f"{API_URL}/health", timeout=5).json()
        if health["status"] == "ok":
            st.success(f"Qdrant: {health['points_count']} vectors")
        else:
            st.warning(f"Qdrant: {health['qdrant']}")
    except Exception:
        st.error("API not reachable")

    # Sources
    try:
        resp = httpx.get(f"{API_URL}/sources", timeout=5).json()
        sources = resp.get("sources", [])
        if sources:
            st.markdown("**Indexed Sources**")
            engines = {}
            for s in sources:
                eng = s.get("db_engine") or s.get("domain", "other")
                engines.setdefault(eng, []).append(s["source_id"])
            for eng, ids in sorted(engines.items()):
                st.markdown(f"- **{eng}**: {', '.join(ids)}")
    except Exception:
        pass

    st.divider()
    if st.button("Clear Chat"):
        st.session_state.messages = []
        st.rerun()

# --- Chat ---
if "messages" not in st.session_state:
    st.session_state.messages = []

# Display chat history
for msg in st.session_state.messages:
    with st.chat_message(msg["role"]):
        st.markdown(msg["content"])
        if msg["role"] == "assistant":
            render_sources(msg.get("sources"))
            render_stats(msg.get("stats"), msg.get("timing_ms", 0))

# Chat input
if prompt := st.chat_input("Ask about database monitoring..."):
    st.session_state.messages.append({"role": "user", "content": prompt})
    with st.chat_message("user"):
        st.markdown(prompt)

    with st.chat_message("assistant"):
        with st.spinner("Thinking..."):
            try:
                resp = httpx.post(
                    f"{API_URL}/ask",
                    json={"question": prompt},
                    timeout=600,
                )
                resp.raise_for_status()
                data = resp.json()

                st.markdown(data["answer"])
                render_sources(data.get("sources"))
                render_stats(data.get("stats"), data.get("timing_ms", 0))

                st.session_state.messages.append({
                    "role": "assistant",
                    "content": data["answer"],
                    "sources": data.get("sources", []),
                    "timing_ms": data.get("timing_ms"),
                    "stats": data.get("stats"),
                })

            except httpx.ConnectError:
                st.error("Cannot connect to API. Is the server running?")
            except Exception as e:
                st.error(f"Error: {e}")
