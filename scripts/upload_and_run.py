"""Upload shard files as a dataset version and start the full DAG through embeddings.

Complete flow for large corpora (e.g. samples/wiki4gb):
  1. Create dataset
  2. Open a draft version
  3. Append shards in small batches (avoids one 4GB multipart)
  4. Commit → ingest/ETL workers
  5. Wait until version ready
  6. POST /v1/pipelines/run (dedupe → embed → index)
  7. Watch React console at http://localhost:5173

  python scripts/upload_and_run.py --dir samples/wiki4gb --api http://localhost:8000
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import tempfile
import time
import urllib.error
import urllib.request
from pathlib import Path


def _json(method: str, url: str, payload: dict | None = None, headers: dict | None = None) -> dict:
    data = None if payload is None else json.dumps(payload).encode()
    req = urllib.request.Request(url, data=data, method=method)
    req.add_header("content-type", "application/json")
    for key, value in (headers or {}).items():
        req.add_header(key, value)
    with urllib.request.urlopen(req, timeout=120) as res:
        return json.loads(res.read().decode())


def _multipart(url: str, files: list[Path], fields: dict[str, str], headers: dict | None = None) -> dict:
    boundary = "----dmlboundary"
    tmp = tempfile.NamedTemporaryFile(delete=False)
    try:
        for key, value in fields.items():
            tmp.write(
                f"--{boundary}\r\nContent-Disposition: form-data; name=\"{key}\"\r\n\r\n{value}\r\n".encode()
            )
        for path in files:
            print(f"  packing {path.name} ({path.stat().st_size / (1024 * 1024):.1f} MB)", flush=True)
            tmp.write(
                (
                    f"--{boundary}\r\nContent-Disposition: form-data; name=\"files\"; filename=\"{path.name}\"\r\n"
                    "Content-Type: application/octet-stream\r\n\r\n"
                ).encode()
            )
            with path.open("rb") as handle:
                while True:
                    buf = handle.read(8 * 1024 * 1024)
                    if not buf:
                        break
                    tmp.write(buf)
            tmp.write(b"\r\n")
        tmp.write(f"--{boundary}--\r\n".encode())
        tmp.flush()
        size = tmp.tell()
        tmp.close()
        with open(tmp.name, "rb") as body:
            req = urllib.request.Request(url, data=body, method="POST")
            req.add_header("content-type", f"multipart/form-data; boundary={boundary}")
            req.add_header("content-length", str(size))
            for key, value in (headers or {}).items():
                req.add_header(key, value)
            with urllib.request.urlopen(req, timeout=3600) as res:
                return json.loads(res.read().decode())
    finally:
        try:
            os.unlink(tmp.name)
        except OSError:
            pass


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dir", default="samples/wiki4gb")
    parser.add_argument("--api", default="http://localhost:8000")
    parser.add_argument("--name", default="wikipedia-en-4gb")
    parser.add_argument("--include-training", action="store_true")
    parser.add_argument("--chunk-chars", default="1200")
    parser.add_argument("--batch-files", type=int, default=4, help="files per upload batch")
    parser.add_argument("--api-key", default=os.environ.get("DML_API_KEY") or "")
    args = parser.parse_args()
    headers = {"X-API-Key": args.api_key} if args.api_key else None
    files = sorted(Path(args.dir).glob("*.jsonl")) + sorted(Path(args.dir).glob("*.txt"))
    if not files:
        raise SystemExit(f"no .jsonl/.txt shards in {args.dir}")
    total_mb = sum(p.stat().st_size for p in files) / (1024 * 1024)
    print(f"complete flow: {len(files)} shards, {total_mb:.1f} MB -> ingest/ETL -> embed -> pgvector")

    try:
        dataset = _json(
            "POST",
            f"{args.api}/v1/datasets",
            {
                "name": args.name,
                "description": "Full Wikipedia corpus for pipeline + embedding analysis",
                "classification": "internal",
                "pii_policy": "redact",
            },
            headers=headers,
        )
    except urllib.error.HTTPError as exc:
        if exc.code != 409:
            raise
        datasets = _json("GET", f"{args.api}/v1/datasets", headers=headers)
        dataset = next((d for d in datasets if d["name"] == args.name), None)
        if dataset is None:
            raise

    print("dataset", dataset["id"])
    versions = _json("GET", f"{args.api}/v1/datasets/{dataset['id']}/versions", headers=headers)
    version = next((v for v in versions if v.get("status") == "draft"), None)
    if version is None:
        version = _multipart(
            f"{args.api}/v1/datasets/{dataset['id']}/versions",
            [],
            {
                "lines_per_shard": "200",
                "chunk_chars": args.chunk_chars,
                "chunk_overlap": "80",
                "commit": "false",
            },
            headers=headers,
        )
        print("draft version", version["id"], version["status"])
    else:
        print("resuming draft", version["id"], f"source_bytes={(version.get('source_bytes') or 0)/(1024*1024):.1f} MB")

    already = {
        s["filename"]
        for s in _json(
            "GET",
            f"{args.api}/v1/datasets/{dataset['id']}/versions/{version['id']}/sources",
            headers=headers,
        )
    }
    remaining = [p for p in files if p.name not in already]
    print(f"already uploaded={len(already)} remaining={len(remaining)}")

    batch = max(1, args.batch_files)
    for i in range(0, len(remaining), batch):
        group = remaining[i : i + batch]
        print(
            f"upload batch {i // batch + 1}/{(len(remaining) + batch - 1) // batch}: {[p.name for p in group]}"
        )
        version = _multipart(
            f"{args.api}/v1/datasets/{dataset['id']}/versions/{version['id']}/sources",
            group,
            {},
            headers=headers,
        )
        print(f"  source_bytes={(version.get('source_bytes') or 0) / (1024 * 1024):.1f} MB", flush=True)

    version = _json(
        "POST",
        f"{args.api}/v1/datasets/{dataset['id']}/versions/{version['id']}/commit",
        {},
        headers=headers,
    )
    print("committed", version["id"], version["status"])

    for _ in range(100_000):
        try:
            current = _json(
                "GET",
                f"{args.api}/v1/datasets/{dataset['id']}/versions/{version['id']}",
                headers=headers,
            )
        except (urllib.error.URLError, ConnectionResetError, TimeoutError, OSError) as exc:
            print(f"ingest/etl poll retry after {exc}", flush=True)
            time.sleep(5)
            continue
        print(
            "ingest/etl",
            current["status"],
            "chunks",
            current.get("chunk_count"),
            "docs",
            current.get("document_count"),
            flush=True,
        )
        if current["status"] in {"ready", "failed"}:
            version = current
            break
        time.sleep(5)
    if version["status"] != "ready":
        raise SystemExit("dataset version did not become ready")

    pipe = _json(
        "POST",
        f"{args.api}/v1/pipelines/run",
        {
            "dataset_version_id": version["id"],
            "include_training": args.include_training,
        },
        headers=headers,
    )
    print("pipeline", pipe["id"], pipe["status"])
    print("watch workers + ETA live at http://localhost:5173  (React) or API WS /v1/ops/ws")
    print("DAG: ingest → etl → dedupe → embed → index  (embedding-worker fills pgvector)")


if __name__ == "__main__":
    main()
    sys.exit(0)
