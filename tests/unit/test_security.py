from types import SimpleNamespace
from uuid import uuid4

import pytest
from fastapi import HTTPException

from dml.api.ops_progress import resolve_searchable_progress
from dml.security.pii import scan_and_redact
from dml.security.rag import (
    authorize_rag_search,
    prepare_rag_query,
    sanitize_rag_hit,
)
from dml.security.rbac import (
    Role,
    parse_api_keys,
    require_classification_access,
    require_permission,
    resolve_principal,
)


def test_pii_redacts_email_and_phone():
    result = scan_and_redact("Contact jane@example.com or +1-415-555-0100 today.")
    assert result.redacted
    assert "email" in result.categories
    assert "phone" in result.categories
    assert "jane@example.com" not in result.text
    assert "[REDACTED_EMAIL]" in result.text


def test_pii_allow_disabled():
    result = scan_and_redact("ssn 123-45-6789", enabled=False)
    assert result.text == "ssn 123-45-6789"
    assert not result.redacted


def test_pii_reject_categories_detected():
    result = scan_and_redact("card 4111-1111-1111-1111 and ip 10.0.0.1")
    assert "credit_card" in result.categories
    assert "ip_address" in result.categories


def test_rbac_roles_and_keys():
    keys = parse_api_keys(
        '{"k-admin":{"subject":"alice","role":"admin"},"k-reader":{"subject":"bob","role":"reader"}}'
    )
    admin = resolve_principal(
        header_key="k-admin", api_keys=keys, legacy_key=None, auth_required=True
    )
    reader = resolve_principal(
        header_key="k-reader", api_keys=keys, legacy_key=None, auth_required=True
    )
    assert admin is not None and admin.role == Role.ADMIN
    assert reader is not None and reader.role == Role.READER
    require_permission(admin, "datasets:write")
    require_permission(admin, "jobs:write")
    require_permission(admin, "models:write")
    try:
        require_permission(reader, "datasets:write")
        assert False, "reader should not write datasets"
    except Exception as exc:
        assert getattr(exc, "status_code", None) == 403
    try:
        require_permission(reader, "jobs:write")
        assert False, "reader should not write jobs"
    except Exception as exc:
        assert getattr(exc, "status_code", None) == 403


def test_classification_gates_rag_access():
    keys = parse_api_keys(
        '{"k-admin":{"subject":"alice","role":"admin"},'
        '"k-op":{"subject":"ops","role":"operator"},'
        '"k-reader":{"subject":"bob","role":"reader"}}'
    )
    admin = resolve_principal(
        header_key="k-admin", api_keys=keys, legacy_key=None, auth_required=True
    )
    operator = resolve_principal(
        header_key="k-op", api_keys=keys, legacy_key=None, auth_required=True
    )
    reader = resolve_principal(
        header_key="k-reader", api_keys=keys, legacy_key=None, auth_required=True
    )
    require_classification_access(reader, "internal")
    require_classification_access(operator, "confidential")
    require_classification_access(admin, "restricted")
    try:
        require_classification_access(reader, "confidential")
        assert False, "reader cannot read confidential"
    except Exception as exc:
        assert getattr(exc, "status_code", None) == 403
    try:
        require_classification_access(operator, "restricted")
        assert False, "operator cannot read restricted"
    except Exception as exc:
        assert getattr(exc, "status_code", None) == 403


def test_dev_mode_open_when_no_keys():
    principal = resolve_principal(
        header_key=None, api_keys={}, legacy_key=None, auth_required=False
    )
    assert principal is not None
    assert principal.role == Role.ADMIN


def test_ops_progress_never_claims_searchable_on_failed_version():
    phase, message, _ = resolve_searchable_progress(
        phase="completed",
        latest_status="failed",
        version_chunks=124398,
        version_embeddings=0,
    )
    assert phase == "failed"
    assert "not searchable" in message
    assert "searchable in pgvector" not in message


def test_ops_progress_incomplete_when_embeds_missing():
    phase, message, percent = resolve_searchable_progress(
        phase="completed",
        latest_status="ready",
        version_chunks=100,
        version_embeddings=22,
    )
    assert phase == "incomplete"
    assert "incomplete" in message
    assert percent < 100


def test_ops_progress_searchable_only_when_ready_and_complete():
    phase, message, percent = resolve_searchable_progress(
        phase="completed",
        latest_status="ready",
        version_chunks=100,
        version_embeddings=100,
    )
    assert phase == "completed"
    assert message == "Pipeline finished. Embeddings are searchable in pgvector."
    assert percent == 100.0


def _rag_dataset(*, classification="internal", pii_policy="redact"):
    return SimpleNamespace(
        id=uuid4(),
        classification=classification,
        pii_policy=pii_policy,
    )


def _rag_version(*, status="ready"):
    return SimpleNamespace(id=uuid4(), status=status, dataset_id=uuid4())


def test_rag_authorize_requires_ready_and_classification():
    keys = parse_api_keys(
        '{"k-admin":{"subject":"alice","role":"admin"},'
        '"k-reader":{"subject":"bob","role":"reader"}}'
    )
    admin = resolve_principal(
        header_key="k-admin", api_keys=keys, legacy_key=None, auth_required=True
    )
    reader = resolve_principal(
        header_key="k-reader", api_keys=keys, legacy_key=None, auth_required=True
    )
    dataset = _rag_dataset(classification="restricted", pii_policy="redact")
    version = _rag_version(status="ready")
    ctx = authorize_rag_search(admin, dataset=dataset, version=version)
    assert ctx.classification == "restricted"
    assert ctx.pii_policy == "redact"
    with pytest.raises(HTTPException) as denied:
        authorize_rag_search(reader, dataset=dataset, version=version)
    assert denied.value.status_code == 403
    with pytest.raises(HTTPException) as not_ready:
        authorize_rag_search(
            admin, dataset=dataset, version=_rag_version(status="embedding")
        )
    assert not_ready.value.status_code == 409


def test_rag_query_pii_redact_and_reject():
    redacted = prepare_rag_query(
        "email me at jane@example.com",
        pii_policy="redact",
        pii_redaction_enabled=True,
    )
    assert redacted.pii.redacted
    assert "jane@example.com" not in redacted.text
    assert "[REDACTED_EMAIL]" in redacted.text

    with pytest.raises(HTTPException) as rejected:
        prepare_rag_query(
            "ssn 123-45-6789",
            pii_policy="reject",
            pii_redaction_enabled=True,
        )
    assert rejected.value.status_code == 400

    allowed = prepare_rag_query(
        "jane@example.com",
        pii_policy="allow",
        pii_redaction_enabled=True,
    )
    assert allowed.text == "jane@example.com"
    assert not allowed.pii.redacted


def test_rag_hit_sanitize_defense_in_depth():
    text, flag = sanitize_rag_hit(
        "reach bob@corp.com",
        pii_policy="redact",
        pii_redaction_enabled=True,
        already_redacted=False,
    )
    assert flag is True
    assert "bob@corp.com" not in text
    kept, kept_flag = sanitize_rag_hit(
        "reach bob@corp.com",
        pii_policy="allow",
        pii_redaction_enabled=True,
        already_redacted=False,
    )
    assert kept == "reach bob@corp.com"
    assert kept_flag is False
