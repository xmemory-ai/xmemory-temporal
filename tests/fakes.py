"""A recording, scriptable stand-in for ``xmemory.AsyncInstanceAPI``.

Because the plugin injects the instance (rather than looking one up), the whole
suite runs with no backend, no network, and no monkeypatching. This fake is the
side-effect ledger the replay test asserts against, so its behavior is kept
dead simple and fully observable: every call appends a ``CallRecord``.
"""

from dataclasses import dataclass, field
from typing import Any

from xmemory._exceptions import XmemoryAPIError  # type: ignore[import-not-found]
from xmemory._models import WriteQueueStatus  # type: ignore[import-not-found]


@dataclass
class CallRecord:
    method: str
    text_or_query: str
    kwargs: dict[str, Any]


@dataclass
class _Read:
    reader_result: Any = None
    reader_results: list[Any] = field(default_factory=list)
    trace_id: str | None = "trace-read"


@dataclass
class _Write:
    write_id: str
    trace_id: str | None = "trace-write"
    changes: Any = None


@dataclass
class _WriteStart:
    write_id: str


@dataclass
class _WriteStatus:
    # `WriteQueueStatus | str`: tests also feed raw strings to model a status the
    # client enum does not know yet (a future server state).
    write_id: str
    write_status: Any
    error_detail: str | None = None
    completed_at: Any = None


class FakeXmemoryInstance:
    """Implements ``XmemoryInstanceProtocol`` and records every call.

    Scriptable:
      * ``read_answer`` — what ``read`` returns.
      * ``fail_write_times(n, exc)`` — the next ``n`` writes/starts raise ``exc``.
      * ``status_sequence([...])`` — the ``write_status`` values to yield in order
        (the last one repeats once exhausted).
    """

    def __init__(self, read_answer: Any = "the answer") -> None:
        self.calls: list[CallRecord] = []
        self._read_answer = read_answer
        self._write_counter = 0
        self._fail_writes = 0
        self._fail_exc: BaseException | None = None
        self._status_values: list[Any] = [WriteQueueStatus.COMPLETED]
        self._status_index = 0
        self._status_error_detail: str | None = None

    # --- scripting ----------------------------------------------------------

    def fail_write_times(self, n: int, exc: BaseException) -> None:
        self._fail_writes = n
        self._fail_exc = exc

    def status_sequence(self, values: list[Any], *, error_detail: str | None = None) -> None:
        self._status_values = list(values)
        self._status_index = 0
        self._status_error_detail = error_detail

    # --- protocol -----------------------------------------------------------

    async def read(self, query: str, **kwargs: Any) -> _Read:
        self.calls.append(CallRecord("read", query, kwargs))
        return _Read(reader_result=self._read_answer)

    async def write(self, text: str, **kwargs: Any) -> _Write:
        self.calls.append(CallRecord("write", text, kwargs))
        self._maybe_fail()
        self._write_counter += 1
        return _Write(write_id=f"w{self._write_counter}")

    async def write_async(self, text: str, **kwargs: Any) -> _WriteStart:
        self.calls.append(CallRecord("write_async", text, kwargs))
        self._maybe_fail()
        self._write_counter += 1
        return _WriteStart(write_id=f"w{self._write_counter}")

    async def write_status(self, write_id: str, **kwargs: Any) -> _WriteStatus:
        self.calls.append(CallRecord("write_status", write_id, kwargs))
        value = self._status_values[min(self._status_index, len(self._status_values) - 1)]
        self._status_index += 1
        detail = self._status_error_detail if value == WriteQueueStatus.FAILED else None
        return _WriteStatus(write_id=write_id, write_status=value, error_detail=detail)

    # --- helpers ------------------------------------------------------------

    def _maybe_fail(self) -> None:
        if self._fail_writes > 0 and self._fail_exc is not None:
            self._fail_writes -= 1
            raise self._fail_exc

    def count(self, method: str) -> int:
        return sum(1 for c in self.calls if c.method == method)


def api_error(*, status: int | None = None, code: str | None = None, **kwargs: Any) -> XmemoryAPIError:
    """Build an ``XmemoryAPIError`` the way the real client would."""
    return XmemoryAPIError("boom", status=status, code=code, **kwargs)
