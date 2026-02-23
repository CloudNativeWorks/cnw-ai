"""Code parser: extract doc comments, README-like content, YAML examples.

Skips test/vendor/generated files. Only ingests documentation-valuable content.
"""

from __future__ import annotations

import re
from pathlib import Path

from cnw_ai.pipeline.models import ParsedDocument, SourceConfig

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

# Go doc comment pattern
_GO_DOC_RE = re.compile(
    r"(?:^|\n)((?://\s?.*\n)+)(?:func|type|var|const)\s+(\w+)",
    re.MULTILINE,
)

# Python docstring pattern
_PY_DOC_RE = re.compile(
    r'(?:def|class)\s+(\w+).*?:\s*\n\s*"""(.*?)"""',
    re.DOTALL,
)


def _should_skip(file_path: Path) -> bool:
    """Check if file should be skipped based on patterns."""
    path_str = str(file_path)
    for pattern in _SKIP_PATTERNS:
        # Simple glob-like matching
        if "vendor" in path_str and "**/vendor/**" == pattern:
            return True
        if path_str.endswith("_test.go") and "**/*_test.go" == pattern:
            return True
        if path_str.endswith(".pb.go") and "**/*.pb.go" == pattern:
            return True
        if "node_modules" in path_str:
            return True
        if "__pycache__" in path_str:
            return True
    return False


def _extract_go_docs(text: str) -> list[tuple[str, str]]:
    """Extract Go doc comments with their symbol names."""
    results = []
    for match in _GO_DOC_RE.finditer(text):
        comment = match.group(1).strip()
        symbol = match.group(2)
        # Clean up // prefixes
        lines = [line.lstrip("/").strip() for line in comment.split("\n")]
        doc = "\n".join(lines).strip()
        if len(doc) > 30:
            results.append((symbol, doc))
    return results


def _extract_python_docs(text: str) -> list[tuple[str, str]]:
    """Extract Python docstrings."""
    results = []
    for match in _PY_DOC_RE.finditer(text):
        symbol = match.group(1)
        doc = match.group(2).strip()
        if len(doc) > 30:
            results.append((symbol, doc))
    return results


def parse_code(file_path: Path, source: SourceConfig) -> list[ParsedDocument]:
    """Parse code files, extracting only documentation-valuable content."""
    if _should_skip(file_path):
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
                )
            )

    return docs
