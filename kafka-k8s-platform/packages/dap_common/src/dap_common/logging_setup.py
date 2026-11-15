"""Structured logging bootstrap."""

from __future__ import annotations

import logging
import sys

from dap_common.config import Settings


def setup_logging(settings: Settings) -> None:
    level = getattr(logging, settings.log_level.upper(), logging.INFO)
    handler = logging.StreamHandler(sys.stdout)
    if settings.log_json:
        fmt = '{"service":"%(name)s","level":"%(levelname)s","msg":"%(message)s"}'
    else:
        fmt = "%(asctime)s %(levelname)s [%(name)s] %(message)s"
    handler.setFormatter(logging.Formatter(fmt))
    root = logging.getLogger()
    root.handlers.clear()
    root.addHandler(handler)
    root.setLevel(level)
