"""Parser registry for different file types."""

from __future__ import annotations

from pathlib import Path

from cnw_ai.pipeline.models import ParsedDocument, SourceConfig
from cnw_ai.pipeline.parsers.markdown import parse_markdown
from cnw_ai.pipeline.parsers.html import parse_html
from cnw_ai.pipeline.parsers.proto import parse_proto
from cnw_ai.pipeline.parsers.code import parse_code
from cnw_ai.pipeline.parsers.jsonl import parse_jsonl
from cnw_ai.pipeline.parsers.pdf import parse_pdf
from cnw_ai.utils.logging import get_logger

log = get_logger(__name__)

# Extension → parser mapping
_PARSER_MAP = {
    ".md": parse_markdown,
    ".rst": parse_markdown,  # RST treated similarly to markdown
    ".html": parse_html,
    ".htm": parse_html,
    ".proto": parse_proto,
    ".jsonl": parse_jsonl,
    ".pdf": parse_pdf,
    # Code files
    ".go": parse_code,
    ".py": parse_code,
    ".yaml": parse_code,
    ".yml": parse_code,
    ".toml": parse_code,
    ".json": parse_code,
    ".sh": parse_code,
    ".bash": parse_code,
}


def get_parser(file_path: Path, hint: str = "auto"):
    """Get the appropriate parser function for a file."""
    if hint != "auto":
        mapping = {
            "markdown": parse_markdown,
            "html": parse_html,
            "proto": parse_proto,
            "code": parse_code,
            "jsonl": parse_jsonl,
            "pdf": parse_pdf,
        }
        if hint in mapping:
            return mapping[hint]

    suffix = file_path.suffix.lower()
    return _PARSER_MAP.get(suffix)


def parse_file(
    file_path: Path, source: SourceConfig
) -> list[ParsedDocument]:
    """Parse a file using the appropriate parser."""
    parser = get_parser(file_path, source.parser_hint)
    if parser is None:
        log.debug("no_parser", path=str(file_path), suffix=file_path.suffix)
        return []

    try:
        return parser(file_path, source)
    except Exception as e:
        log.warning("parse_error", path=str(file_path), error=str(e))
        return []
