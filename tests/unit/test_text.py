from dml.text import chunk_text, content_hash, normalize_text, parse_records, split_line_shards


def test_normalize_and_hash_are_stable() -> None:
    a = content_hash("Hello   world\r\n\r\n")
    b = content_hash("Hello world")
    assert a == b
    assert normalize_text("  a\n\n\n b  ") == "a\n\n b"


def test_split_line_shards_does_not_require_full_list() -> None:
    def gen():
        for i in range(2500):
            yield f"line {i}\n"

    shards = list(split_line_shards(gen(), lines_per_shard=1000))
    assert [len(lines) for _, lines in shards] == [1000, 1000, 500]
    assert shards[0][0] == 0
    assert shards[-1][0] == 2


def test_chunk_text_respects_max_chars() -> None:
    text = "alpha " * 400
    chunks = chunk_text(text, max_chars=80, overlap=10)
    assert chunks
    assert all(len(c.text) <= 80 for c in chunks)
    assert chunks[0].ordinal == 0


def test_parse_jsonl_skips_empty() -> None:
    body = '{"text":"hello"}\n\n{"text":"   "}\n{"content":"world"}\n'
    records = parse_records(body, content_type="jsonl")
    assert [r["text"] for r in records] == ["hello", "world"]
