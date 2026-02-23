"""Streamlit chat UI for Elchi/Envoy RAG assistant."""

import streamlit as st

from cnw_ai.config import COLLECTION_NAME, LLM_MODEL, QDRANT_URL
from cnw_ai.rag import get_chain, get_qdrant_client, get_retriever

st.set_page_config(page_title="Elchi AI Assistant", page_icon="🛡️", layout="wide")

# Sidebar
with st.sidebar:
    st.title("Elchi AI Assistant")
    st.markdown("Envoy/Elchi konusunda uzman RAG sistemi")
    st.divider()
    st.markdown(f"**Model:** `{LLM_MODEL}`")
    st.markdown(f"**Collection:** `{COLLECTION_NAME}`")

    try:
        qclient = get_qdrant_client()
        info = qclient.get_collection(COLLECTION_NAME)
        st.markdown(f"**Dokuman sayisi:** `{info.points_count}` chunk")

        # Show domain breakdown
        with st.expander("Kaynak bilgisi"):
            # Quick scroll to get unique domains
            result = qclient.scroll(
                collection_name=COLLECTION_NAME,
                limit=1,
                with_payload=["domain", "source_id"],
                with_vectors=False,
            )
            if result[0]:
                st.markdown("Qdrant connected")
    except Exception:
        st.warning("Qdrant baglantisi kurulamadi. Once `python -m cnw_ai` calistirin.")

    st.divider()
    st.markdown("**Ornek sorular:**")
    st.markdown("- WAF nedir ve nasil yapilandirilir?")
    st.markdown("- Listener nasil olusturulur?")
    st.markdown("- GSLB nasil calisir?")
    st.markdown("- Route konfigurasyonu nasil yapilir?")

# Chat state
if "messages" not in st.session_state:
    st.session_state.messages = []

if "chain" not in st.session_state:
    try:
        st.session_state.chain = get_chain()
        st.session_state.retriever = get_retriever()
    except Exception as e:
        st.error(f"RAG chain baslatılamadı: {e}")
        st.stop()

# Display chat history
for message in st.session_state.messages:
    with st.chat_message(message["role"]):
        st.markdown(message["content"])

# Chat input
if prompt := st.chat_input("Elchi/Envoy hakkinda bir soru sorun..."):
    st.session_state.messages.append({"role": "user", "content": prompt})
    with st.chat_message("user"):
        st.markdown(prompt)

    with st.chat_message("assistant"):
        with st.spinner("Dusunuyor..."):
            answer = st.session_state.chain.invoke(prompt)
            sources = st.session_state.retriever.invoke(prompt)

        st.markdown(answer)

        if sources:
            with st.expander("Kaynaklar", expanded=False):
                for i, doc in enumerate(sources, 1):
                    domain = doc.metadata.get("domain", "?")
                    source_id = doc.metadata.get("source_id", "?")
                    component = doc.metadata.get("component", "")
                    section = doc.metadata.get("section", "")
                    label = f"`{domain}` / `{source_id}`"
                    if component:
                        label += f" / `{component}`"
                    if section:
                        label += f" - {section}"
                    st.markdown(f"**{i}.** {label}")
                    st.caption(doc.page_content[:200] + "...")

    st.session_state.messages.append({"role": "assistant", "content": answer})
