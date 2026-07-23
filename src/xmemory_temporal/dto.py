"""Activity input/output types.

These are **dataclasses on purpose**, and they are deliberately *ours* rather
than ``xmemory-ai``'s result models.

*Ours, not the client's:* activity arguments and results are persisted verbatim
into Temporal workflow history, so whatever type crosses that boundary becomes a
compatibility contract for every workflow that has ever run. A field renamed in
``xmemory-ai`` 0.11 would then break *replay of already-completed workflows* —
the one failure mode Temporal's review cares about most. We own the wire format
and map at the boundary; the client is free to evolve underneath us.

*Dataclasses, not pydantic:* Temporal's default data converter reconstructs
dataclasses from the activity's type hints with no configuration. Pydantic v2
models round-trip back as plain ``dict``\\ s unless a pydantic data converter is
installed namespace-wide — and this plugin refuses to impose one, since that
would rewrite every payload flowing through the user's worker, not just ours.
(``XmemoryConfig`` stays pydantic: it is never an activity argument, so it never
touches history.)

Outputs are flattened projections — only the fields a workflow can act on.

Note: like the rest of the package, this module uses real annotations (no
``from __future__ import annotations``). It matters most here: Temporal's data
converter reconstructs these dataclasses by calling ``typing.get_type_hints``
on them, which fails to resolve stringized annotations (``Any``,
``list[SubAnswer]``) from its evaluation context. ``X | None`` and ``list[...]``
evaluate natively on the 3.10 floor, so nothing is lost.
"""

from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True)
class ReadInput:
    query: str
    read_mode: str | None = None
    scope: dict[str, Any] | None = None
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
