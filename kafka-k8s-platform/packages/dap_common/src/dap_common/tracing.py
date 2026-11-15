"""Optional OpenTelemetry-style trace id propagation via message headers.

When DAP_OTEL_ENABLED=false (default), this is a no-op helper that still
stamps a lightweight trace_id on payloads for log correlation.
"""

from __future__ import annotations

import uuid
from typing import Any


TRACE_KEY = "_dap_trace_id"


def ensure_trace_id(payload: dict[str, Any]) -> str:
    tid = payload.get(TRACE_KEY)
    if isinstance(tid, str) and tid:
        return tid
    tid = uuid.uuid4().hex
    payload[TRACE_KEY] = tid
    return tid


def copy_trace(src: dict[str, Any], dest: dict[str, Any]) -> str:
    tid = ensure_trace_id(src)
    dest[TRACE_KEY] = tid
    return tid
