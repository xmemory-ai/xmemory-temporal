"""The workflow facade must keep quacking like the real client.

The migration story — swap ``client.instance(id)`` for ``xmemory_for_workflow()``
and change nothing else — only holds if ``WorkflowXmemory.read`` / ``.write``
accept the same core arguments as ``xmemory.AsyncInstanceAPI``. This reflection
check fails loudly if a client release drifts the shared surface, rather than
letting the two silently diverge.
"""

import inspect
from typing import Callable

from xmemory._instance import AsyncInstanceAPI  # type: ignore[import-not-found]

from xmemory_temporal.workflow_api import WorkflowXmemory


def _params(fn: Callable[..., object]) -> set[str]:
    return {
        name
        for name, p in inspect.signature(fn).parameters.items()
        if name != "self" and p.kind in (p.POSITIONAL_OR_KEYWORD, p.KEYWORD_ONLY)
    }


def test_read_shares_core_params() -> None:
    client_params = _params(AsyncInstanceAPI.read)
    facade_params = _params(WorkflowXmemory.read)
    # The facade need not expose client-only knobs (e.g. `timeout`, which
    # Temporal governs), but everything it DOES share must exist on the client.
    shared = {"query", "read_mode", "scope", "read_id"}
    assert shared <= client_params, shared - client_params
    assert shared <= facade_params, shared - facade_params


def test_write_shares_core_params() -> None:
    client_params = _params(AsyncInstanceAPI.write)
    facade_params = _params(WorkflowXmemory.write)
    shared = {"text", "extraction_logic", "diff_engine"}
    assert shared <= client_params, shared - client_params
    assert shared <= facade_params, shared - facade_params


def test_facade_is_not_a_client_subclass() -> None:
    # Structural mimicry, not nominal: return types differ (our DTOs), so it must
    # not claim substitutability by inheritance.
    assert not issubclass(WorkflowXmemory, AsyncInstanceAPI)
