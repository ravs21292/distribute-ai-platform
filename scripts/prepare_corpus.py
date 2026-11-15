"""Download English Wikipedia text from Hugging Face and write jsonl shards.

Default target is 1 GiB. Use --bytes 67108864 for a quicker laptop run.

  python scripts/prepare_corpus.py --out samples/wiki --bytes 1073741824
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


def _iter_articles():
    try:
        from datasets import load_dataset
    except ImportError as exc:
        raise SystemExit("pip install datasets  # required to download Wikipedia") from exc

    specs = [
        ("wikimedia/wikipedia", "20231101.en", "train"),
        ("wikipedia", "20220301.en", "train"),
        ("wikitext", "wikitext-103-raw-v1", "train"),
    ]
    last_error = None
    for name, config, split in specs:
        try:
            print(f"loading {name} ({config}) streaming…", flush=True)
            ds = load_dataset(name, config, split=split, streaming=True)
            for row in ds:
                title = str(row.get("title") or row.get("id") or "")
                text = str(row.get("text") or "")
                if len(text.strip()) < 80:
                    continue
                yield title, text
            return
        except Exception as exc:
            last_error = exc
            print(f"skip {name}: {exc}", flush=True)
    raise SystemExit(f"could not load a Hugging Face corpus: {last_error}")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--out", default="samples/wiki")
    parser.add_argument("--bytes", type=int, default=1024 * 1024 * 1024, help="target uncompressed UTF-8 bytes")
    parser.add_argument("--shard-bytes", type=int, default=64 * 1024 * 1024)
    args = parser.parse_args()
    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    written = 0
    shard_idx = 0
    shard_size = 0
    handle = None

    def open_shard():
        nonlocal handle, shard_idx, shard_size
        if handle:
            handle.close()
        shard_idx += 1
        shard_size = 0
        path = out / f"wiki-{shard_idx:04d}.jsonl"
        print(f"writing {path}", flush=True)
        handle = path.open("w", encoding="utf-8")

    open_shard()
    try:
        for i, (title, text) in enumerate(_iter_articles(), start=1):
            line = json.dumps({"id": f"doc-{i}", "title": title, "text": text}, ensure_ascii=False) + "\n"
            raw = line.encode("utf-8")
            if shard_size + len(raw) > args.shard_bytes:
                open_shard()
            handle.write(line)
            shard_size += len(raw)
            written += len(raw)
            if i % 500 == 0:
                print(f"{written / (1024 * 1024):.1f} MB", flush=True)
            if written >= args.bytes:
                break
    finally:
        if handle:
            handle.close()
    print(f"done: {written} bytes in {shard_idx} shards under {out}", flush=True)


if __name__ == "__main__":
    main()
    sys.exit(0)
