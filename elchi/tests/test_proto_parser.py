"""Tests for the proto parser."""

from pathlib import Path
from textwrap import dedent

from cnw_ai.pipeline.models import SourceConfig
from cnw_ai.pipeline.parsers.proto import parse_proto


def _write_proto(tmp_path: Path, content: str) -> Path:
    p = tmp_path / "test.proto"
    p.write_text(dedent(content))
    return p


def _source() -> SourceConfig:
    return SourceConfig(
        id="test-proto",
        domain="test",
        priority=1,
        source_type="local",
        location="/tmp",
        tags=["proto"],
    )


def test_simple_message(tmp_path):
    p = _write_proto(tmp_path, """\
        syntax = "proto3";

        // A simple request message.
        message HelloRequest {
            string name = 1;
            int32 age = 2;
        }
    """)
    docs = parse_proto(p, _source())
    assert len(docs) == 1
    assert docs[0].gtype == "message"
    assert docs[0].message == "HelloRequest"
    assert "name" in docs[0].proto_field


def test_enum_block(tmp_path):
    p = _write_proto(tmp_path, """\
        syntax = "proto3";

        enum Status {
            UNKNOWN = 0;
            ACTIVE = 1;
            INACTIVE = 2;
        }
    """)
    docs = parse_proto(p, _source())
    assert len(docs) == 1
    assert docs[0].gtype == "enum"
    assert docs[0].message == "Status"


def test_service_block(tmp_path):
    p = _write_proto(tmp_path, """\
        syntax = "proto3";

        service Greeter {
            rpc SayHello (HelloRequest) returns (HelloReply);
        }
    """)
    docs = parse_proto(p, _source())
    assert len(docs) == 1
    assert docs[0].gtype == "service"


def test_nested_message(tmp_path):
    p = _write_proto(tmp_path, """\
        syntax = "proto3";

        message Outer {
            string id = 1;

            message Inner {
                string value = 1;
            }

            Inner inner = 2;
        }
    """)
    docs = parse_proto(p, _source())
    # Should have at least the Outer message
    assert any(d.message == "Outer" for d in docs)


def test_oneof_detection(tmp_path):
    p = _write_proto(tmp_path, """\
        syntax = "proto3";

        message Config {
            oneof backend {
                string url = 1;
                string path = 2;
            }
        }
    """)
    docs = parse_proto(p, _source())
    assert len(docs) >= 1
    assert "backend" in docs[0].oneof


def test_deprecated_detection(tmp_path):
    p = _write_proto(tmp_path, """\
        syntax = "proto3";

        message OldMessage {
            string name = 1 [deprecated = true];
        }
    """)
    docs = parse_proto(p, _source())
    assert docs[0].deprecated is True


def test_leading_comments(tmp_path):
    p = _write_proto(tmp_path, """\
        syntax = "proto3";

        // This is a doc comment
        // for the Foo message.
        message Foo {
            string bar = 1;
        }
    """)
    docs = parse_proto(p, _source())
    assert "doc comment" in docs[0].content
