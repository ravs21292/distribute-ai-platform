"""PII scan/redact used by the ETL service before hashing and embedding."""

from __future__ import annotations

import re
from dataclasses import dataclass

_RULES: list[tuple[str, re.Pattern[str], str]] = [
    ("email", re.compile(r"\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}\b"), "[REDACTED_EMAIL]"),
    (
        "phone",
        re.compile(r"(?<!\d)(?:\+?1[-.\s]?)?(?:\(?\d{3}\)?[-.\s]?)\d{3}[-.\s]?\d{4}(?!\d)"),
        "[REDACTED_PHONE]",
    ),
    ("ssn", re.compile(r"\b\d{3}-\d{2}-\d{4}\b"), "[REDACTED_SSN]"),
    ("credit_card", re.compile(r"\b(?:\d[ -]*?){13,19}\b"), "[REDACTED_CARD]"),
    (
        "ip_address",
        re.compile(r"\b(?:(?:25[0-5]|2[0-4]\d|[01]?\d\d?)\.){3}(?:25[0-5]|2[0-4]\d|[01]?\d\d?)\b"),
        "[REDACTED_IP]",
    ),
]


@dataclass(frozen=True, slots=True)
class PiiResult:
    text: str
    categories: list[str]
    redacted: bool


def scan_and_redact(text: str, *, enabled: bool = True) -> PiiResult:
    if not enabled or not text:
        return PiiResult(text=text, categories=[], redacted=False)
    found: list[str] = []
    out = text
    for name, pattern, replacement in _RULES:
        if pattern.search(out):
            found.append(name)
            out = pattern.sub(replacement, out)
    return PiiResult(text=out, categories=sorted(set(found)), redacted=bool(found))
