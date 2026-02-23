"""Proto parser: line-based state machine for .proto files.

Extracts message, enum, service blocks with metadata.
Handles nested messages, oneof, reserved, options, multi-line comments.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum, auto
from pathlib import Path

from cnw_ai.pipeline.models import ParsedDocument, SourceConfig


class _State(Enum):
    TOP = auto()
    BLOCK = auto()       # inside message/enum/service
    LINE_COMMENT = auto()
    BLOCK_COMMENT = auto()


@dataclass
class _Block:
    gtype: str  # message, enum, service
    name: str
    lines: list[str] = field(default_factory=list)
    leading_comments: list[str] = field(default_factory=list)
    fields: list[str] = field(default_factory=list)
    deprecated: bool = False
    oneofs: list[str] = field(default_factory=list)
    depth: int = 0  # brace nesting depth within this block


def _parse_proto_blocks(text: str) -> list[_Block]:
    """Parse proto file into blocks using a line-based state machine."""
    lines = text.split("\n")
    blocks: list[_Block] = []
    pending_comments: list[str] = []
    block_stack: list[_Block] = []
    in_block_comment = False

    for line in lines:
        stripped = line.strip()

        # Handle block comment boundaries
        if in_block_comment:
            pending_comments.append(stripped.lstrip("* "))
            if "*/" in stripped:
                in_block_comment = False
            continue

        if stripped.startswith("/*"):
            in_block_comment = "*/" not in stripped
            comment_text = stripped.lstrip("/* ").rstrip("*/ ")
            if comment_text:
                pending_comments.append(comment_text)
            continue

        # Single-line comment
        if stripped.startswith("//"):
            pending_comments.append(stripped.lstrip("/ "))
            continue

        # Block start: message, enum, service
        for keyword in ("message", "enum", "service"):
            if stripped.startswith(keyword + " "):
                rest = stripped[len(keyword) :].strip()
                name = rest.split("{")[0].strip().split()[0] if rest else "unknown"
                block = _Block(
                    gtype=keyword,
                    name=name,
                    leading_comments=list(pending_comments),
                    depth=0,
                )
                pending_comments.clear()

                if block_stack:
                    # Nested block: track it
                    block_stack[-1].lines.append(stripped)

                block_stack.append(block)
                # Count opening braces on this line
                block.depth += stripped.count("{") - stripped.count("}")
                if block.depth <= 0:
                    # Single-line block
                    block_stack.pop()
                    blocks.append(block)
                break
        else:
            # Not a block start line
            if not block_stack:
                # Top-level non-block line: clear pending comments unless meaningful
                if stripped and not stripped.startswith("syntax") and not stripped.startswith("package") and not stripped.startswith("import") and not stripped.startswith("option"):
                    pending_comments.clear()
                continue

            current = block_stack[-1]
            current.lines.append(stripped)

            # Track brace depth
            current.depth += stripped.count("{") - stripped.count("}")

            # Extract field info
            if "deprecated" in stripped.lower():
                current.deprecated = True

            if stripped.startswith("oneof "):
                oneof_name = stripped.split()[1].rstrip("{").strip()
                current.oneofs.append(oneof_name)

            # Named fields (simplified: lines with = and ;)
            if "=" in stripped and stripped.endswith(";"):
                field_name = stripped.split("=")[0].strip().split()
                if len(field_name) >= 2:
                    current.fields.append(field_name[-1])

            # Block end
            if current.depth <= 0:
                block_stack.pop()
                if not block_stack:
                    blocks.append(current)
                # else: nested block finished, parent continues

    return blocks


def parse_proto(file_path: Path, source: SourceConfig) -> list[ParsedDocument]:
    """Parse a .proto file into documents per block."""
    text = file_path.read_text(encoding="utf-8", errors="replace")
    if not text.strip():
        return []

    blocks = _parse_proto_blocks(text)
    docs = []

    for block in blocks:
        # Build content: leading comments + block source
        parts = []
        if block.leading_comments:
            parts.append("\n".join(block.leading_comments))
        block_source = f"{block.gtype} {block.name} {{\n"
        block_source += "\n".join(f"  {l}" for l in block.lines)
        block_source += "\n}"
        parts.append(block_source)
        content = "\n\n".join(parts)

        if len(content) < 20:
            continue

        docs.append(
            ParsedDocument(
                source_id=source.id,
                domain=source.domain,
                uri=str(file_path),
                title=f"{block.gtype} {block.name}",
                section=block.name,
                content=content,
                source_type=source.source_type,
                priority=source.priority,
                version=source.version,
                tags=list(source.tags),
                component=source.component,
                license=source.license,
                origin=source.location,
                gtype=block.gtype,
                message=block.name,
                proto_field=", ".join(block.fields[:20]),
                deprecated=block.deprecated,
                oneof=", ".join(block.oneofs),
            )
        )

    # If no blocks found, treat the whole file as a single document
    if not docs and len(text.strip()) > 50:
        docs.append(
            ParsedDocument(
                source_id=source.id,
                domain=source.domain,
                uri=str(file_path),
                title=file_path.stem,
                section="",
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
