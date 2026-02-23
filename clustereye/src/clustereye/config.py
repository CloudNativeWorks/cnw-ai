from pathlib import Path

# Paths
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
DATA_DIR = PROJECT_ROOT / "data"
WORKDIR = DATA_DIR / "workdir"
EXPORT_DIR = DATA_DIR / "export"
CONFIG_DIR = PROJECT_ROOT / "config"
DEFAULT_SOURCES_YAML = CONFIG_DIR / "sources.yaml"

# Ollama models
LLM_MODEL = "deepseek-r1:8b"
EMBEDDING_MODEL = "nomic-embed-text"
EMBEDDING_DIM = 768
OLLAMA_BASE_URL = "http://localhost:11434"

# Qdrant
QDRANT_URL = "http://localhost:6333"
COLLECTION_NAME = "clustereye_docs"

# Chunking
CHUNK_SIZE = 4000  # chars (~1000 tokens)
CHUNK_OVERLAP = 600  # chars (~150 tokens)
MIN_CHUNK_LENGTH = 200  # skip chunks shorter than this

# Embedding
EMBED_BATCH_SIZE = 64

# Upsert
UPSERT_BATCH_SIZE = 100

# Retrieval
TOP_K = 8

# API
API_HOST = "0.0.0.0"
API_PORT = 8000

# Workers
DEFAULT_WORKERS = 1

# Crawler
CRAWL_MAX_DEPTH = 1
CRAWL_RATE_LIMIT = 2.0
