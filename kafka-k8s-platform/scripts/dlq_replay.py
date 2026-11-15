#!/usr/bin/env python3
"""Replay DLQ messages from Kafka back to their source topics (additive ops tool).

Usage (Compose network / host with port 19092):
  python scripts/dlq_replay.py --bootstrap host.docker.internal:19092 --max 10
  python scripts/dlq_replay.py --bootstrap localhost:19092 --dry-run

Does not delete DLQ offsets; commits after successful republish when --commit.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import sys


async def _run(args: argparse.Namespace) -> int:
    from aiokafka import AIOKafkaConsumer, AIOKafkaProducer

    consumer = AIOKafkaConsumer(
        "dap.dlq",
        bootstrap_servers=args.bootstrap,
        group_id=args.group,
        enable_auto_commit=False,
        auto_offset_reset="earliest",
        value_deserializer=lambda b: json.loads(b.decode("utf-8")),
    )
    producer = AIOKafkaProducer(
        bootstrap_servers=args.bootstrap,
        value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
    )
    await consumer.start()
    await producer.start()
    count = 0
    try:
        async for msg in consumer:
            payload = msg.value if isinstance(msg.value, dict) else {}
            source = payload.get("source_topic")
            body = payload.get("payload") or {}
            if not source:
                print(f"skip offset={msg.offset} missing source_topic", file=sys.stderr)
                continue
            key = None
            if isinstance(body, dict) and body.get("vehicle_id"):
                key = str(body["vehicle_id"])
            print(f"replay topic={source} offset={msg.offset} dry_run={args.dry_run}")
            if not args.dry_run:
                await producer.send_and_wait(source, value=body, key=key)
            count += 1
            if args.commit and not args.dry_run:
                await consumer.commit()
            if count >= args.max:
                break
    finally:
        await consumer.stop()
        await producer.stop()
    print(f"replayed={count}")
    return 0


def main() -> None:
    p = argparse.ArgumentParser(description="Replay dap.dlq messages")
    p.add_argument("--bootstrap", default="localhost:19092")
    p.add_argument("--group", default="dap-dlq-replay")
    p.add_argument("--max", type=int, default=20)
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--commit", action="store_true", help="Commit DLQ offsets after republish")
    args = p.parse_args()
    raise SystemExit(asyncio.run(_run(args)))


if __name__ == "__main__":
    main()
