"""Activity input/output types: ours, not the client's.

Activity payloads are persisted verbatim into workflow history, so the type that
crosses that boundary becomes a compatibility contract for every workflow that
has ever run. Owning the wire format lets ``xmemory-ai`` evolve underneath us
without breaking replay of completed workflows.

Dataclasses, not pydantic: Temporal's default converter reconstructs those with
no configuration, whereas pydantic models need a namespace-wide converter this
plugin refuses to impose. For the same reason this module uses real annotations
(no ``from __future__ import annotations``), since the converter resolves them via
``typing.get_type_hints``, which fails on stringized ones.

Outputs are flattened projections — only the fields a workflow can act on.
"""

from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True)
class ScopeObject:
    """One record a scoped read may touch, addressed by its primary key."""

    type: str
    key: dict[str, str | int | float | bool] = field(default_factory=dict)


@dataclass(frozen=True)
class ReadScope:
    """Restrict a read to specific records.

    Mirrors the client's ``ReadScope`` so a malformed scope is a type error at
    author time rather than a non-retryable ``XmemoryBadRequest`` at runtime.
    ``relations_scope`` is ``no_relations`` (objects only) by default;
    ``all_relations`` also exposes relations among the in-scope objects.
    """

    objects: list[ScopeObject] = field(default_factory=list)
    relations_scope: str = "no_relations"


@dataclass(frozen=True)
class ReadInput:
    query: str
    read_mode: str | None = None
    scope: ReadScope | None = None
    read_id: str | None = None


@dataclass(frozen=True)
class SubAnswer:
    """One decomposed sub-query and its own answer."""

    sub_query: str
    reader_result: Any = None
    error: str | None = None


@dataclass(frozen=True)
class ReadOutput:
    reader_result: Any = None
    sub_answers: list[SubAnswer] = field(default_factory=list)
    trace_id: str | None = None


@dataclass(frozen=True)
class WriteInput:
    text: str
    extraction_logic: str | None = None
    diff_engine: bool | None = None


@dataclass(frozen=True)
class WriteOutput:
    write_id: str
    trace_id: str | None = None
    changes: Any = None


@dataclass(frozen=True)
class WriteStartOutput:
    write_id: str


@dataclass(frozen=True)
class WriteStatusInput:
    write_id: str


@dataclass(frozen=True)
class WriteStatusOutput:
    write_id: str
    write_status: str
    error_detail: str | None = None
    completed_at: str | None = None
    # What the write applied. None until xmemory-ai surfaces it on write_status
    # (see project_write_status); kept for symmetry with WriteOutput.changes.
    changes: Any = None


# --- Projections from the client's models ----------------------------------
# `getattr` with defaults rather than attribute access: an older or newer client
# release may not carry every field, and a missing one should degrade to `None`
# rather than raise inside an activity.


def project_read(result: Any) -> ReadOutput:
    raw_sub = getattr(result, "reader_results", None) or []
    return ReadOutput(
        reader_result=getattr(result, "reader_result", None),
        sub_answers=[
            SubAnswer(
                sub_query=getattr(item, "sub_query", ""),
                reader_result=getattr(item, "reader_result", None),
                error=getattr(item, "error", None),
            )
            for item in raw_sub
        ],
        trace_id=getattr(result, "trace_id", None),
    )


def project_write(result: Any) -> WriteOutput:
    return WriteOutput(
        write_id=getattr(result, "write_id", ""),
        trace_id=getattr(result, "trace_id", None),
        changes=getattr(result, "changes", None),
    )


def project_write_start(result: Any) -> WriteStartOutput:
    return WriteStartOutput(write_id=getattr(result, "write_id", ""))


def project_write_status(result: Any) -> WriteStatusOutput:
    status = getattr(result, "write_status", None)
    completed_at = getattr(result, "completed_at", None)
    return WriteStatusOutput(
        write_id=getattr(result, "write_id", ""),
        # `WriteQueueStatus` is a `str` enum; normalize to its plain value so
        # history never embeds an enum class the workflow side must import.
        write_status=getattr(status, "value", status) or "",
        error_detail=getattr(result, "error_detail", None),
        completed_at=completed_at.isoformat() if completed_at is not None else None,
        # The server returns what the write applied, but xmemory-ai's
        # WriteStatusResult does not surface it yet — so `changes` is None here
        # (unlike sync `write`, which carries WriteResult.changes). Picked up via
        # getattr so it auto-populates if a future client exposes it. Surfacing it
        # is an upstream client follow-up.
        changes=getattr(result, "changes", None) or getattr(result, "result", None),
    )
