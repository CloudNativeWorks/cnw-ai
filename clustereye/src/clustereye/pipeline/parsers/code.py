"""Code parser: extract doc comments, README-like content, YAML examples.

Skips test/vendor/generated files. Only ingests documentation-valuable content.
"""

from __future__ import annotations

from pathlib import Path

from clustereye.pipeline.models import ParsedDocument, SourceConfig

# Patterns to skip
_SKIP_PATTERNS = [
    "**/vendor/**",
    "**/*_test.go",
    "**/test_*.py",
    "**/*_test.py",
    "**/tests/**",
    "**/*.generated.*",
    "**/*.pb.go",
    "**/*.pb.*.go",
    "**/node_modules/**",
    "**/__pycache__/**",
]

# Max file size to parse (skip huge generated/vendor files)
_MAX_FILE_SIZE = 200_000  # 200KB


def _should_skip(file_path: Path) -> bool:
    """Check if file should be skipped based on patterns."""
    path_str = str(file_path)
    if "vendor" in path_str:
        return True
    if path_str.endswith("_test.go"):
        return True
    if path_str.endswith(".pb.go"):
        return True
    if "node_modules" in path_str:
        return True
    if "__pycache__" in path_str:
        return True
    return False


_GO_DECL_KEYWORDS = {"func", "type", "var", "const"}


def _extract_go_docs(text: str) -> list[tuple[str, str]]:
    """Extract Go doc comments with their symbol names (line-based, no regex)."""
    results = []
    lines = text.split("\n")
    comment_buf: list[str] = []

    for line in lines:
        stripped = line.strip()
        if stripped.startswith("//"):
            comment_buf.append(stripped.lstrip("/").strip())
            continue

        if comment_buf and stripped:
            first_word = stripped.split("(")[0].split(" ")[0]
            if first_word in _GO_DECL_KEYWORDS:
                parts = stripped.split()
                symbol = parts[1].split("(")[0].split("[")[0] if len(parts) > 1 else ""
                if symbol:
                    doc = "\n".join(comment_buf).strip()
                    if len(doc) > 30:
                        results.append((symbol, doc))

        comment_buf.clear()

    return results


def _extract_python_docs(text: str) -> list[tuple[str, str]]:
    """Extract Python docstrings (line-based scan)."""
    results = []
    lines = text.split("\n")
    i = 0
    while i < len(lines):
        stripped = lines[i].strip()
        if stripped.startswith(("def ", "class ")):
            keyword = "def" if stripped.startswith("def") else "class"
            symbol = stripped[len(keyword):].strip().split("(")[0].split(":")[0].strip()
            j = i + 1
            while j < len(lines) and not lines[j].strip():
                j += 1
            if j < len(lines) and '"""' in lines[j]:
                line_j = lines[j].strip()
                if line_j.count('"""') >= 2:
                    doc = line_j.strip('" ')
                    if len(doc) > 30:
                        results.append((symbol, doc))
                    i = j + 1
                    continue
                doc_lines = [line_j.replace('"""', "").strip()]
                j += 1
                while j < len(lines):
                    if '"""' in lines[j]:
                        doc_lines.append(lines[j].strip().replace('"""', "").strip())
                        break
                    doc_lines.append(lines[j].strip())
                    j += 1
                doc = "\n".join(doc_lines).strip()
                if len(doc) > 30:
                    results.append((symbol, doc))
                i = j + 1
                continue
        i += 1
    return results


def parse_code(file_path: Path, source: SourceConfig) -> list[ParsedDocument]:
    """Parse code files, extracting only documentation-valuable content."""
    if _should_skip(file_path):
        return []

    if file_path.stat().st_size > _MAX_FILE_SIZE:
        return []

    text = file_path.read_text(encoding="utf-8", errors="replace")
    if not text.strip():
        return []

    suffix = file_path.suffix.lower()
    docs = []

    # YAML/TOML/JSON: ingest whole file as config example if not too large
    if suffix in {".yaml", ".yml", ".toml", ".json"}:
        if len(text) < 10000:
            docs.append(
                ParsedDocument(
                    source_id=source.id,
                    domain=source.domain,
                    uri=str(file_path),
                    title=file_path.name,
                    section="config",
                    content=text,
                    source_type=source.source_type,
                    priority=source.priority,
                    version=source.version,
                    tags=list(source.tags),
                    component=source.component,
                    license=source.license,
                    origin=source.location,
                    db_engine=source.db_engine,
                    topic=source.topic,
                )
            )
        return docs

    # Go files: extract doc comments
    if suffix == ".go":
        for symbol, doc in _extract_go_docs(text):
            docs.append(
                ParsedDocument(
                    source_id=source.id,
                    domain=source.domain,
                    uri=str(file_path),
                    title=f"{file_path.stem}.{symbol}",
                    section=symbol,
                    content=doc,
                    source_type=source.source_type,
                    priority=source.priority,
                    version=source.version,
                    tags=list(source.tags),
                    component=source.component,
                    license=source.license,
                    origin=source.location,
                    db_engine=source.db_engine,
                    topic=source.topic,
                )
            )

    # Python files: extract docstrings
    elif suffix == ".py":
        for symbol, doc in _extract_python_docs(text):
            docs.append(
                ParsedDocument(
                    source_id=source.id,
                    domain=source.domain,
                    uri=str(file_path),
                    title=f"{file_path.stem}.{symbol}",
                    section=symbol,
                    content=doc,
                    source_type=source.source_type,
                    priority=source.priority,
                    version=source.version,
                    tags=list(source.tags),
                    component=source.component,
                    license=source.license,
                    origin=source.location,
                    db_engine=source.db_engine,
                    topic=source.topic,
                )
            )

    # Shell scripts: extract header comments
    elif suffix in {".sh", ".bash"}:
        lines = text.split("\n")
        header_lines = []
        for line in lines[1:]:  # skip shebang
            if line.startswith("#"):
                header_lines.append(line.lstrip("# "))
            elif line.strip():
                break
        header = "\n".join(header_lines).strip()
        if len(header) > 30:
            docs.append(
                ParsedDocument(
                    source_id=source.id,
                    domain=source.domain,
                    uri=str(file_path),
                    title=file_path.name,
                    section="header",
                    content=header,
                    source_type=source.source_type,
                    priority=source.priority,
                    version=source.version,
                    tags=list(source.tags),
                    component=source.component,
                    license=source.license,
                    origin=source.location,
                    db_engine=source.db_engine,
                    topic=source.topic,
                )
            )

    return docs
