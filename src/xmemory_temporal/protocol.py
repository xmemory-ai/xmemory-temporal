"""The narrow slice of the xmemory client this plugin depends on.

``xmemory.AsyncInstanceAPI`` exposes fourteen methods; we use four. Depending on
a structural ``Protocol`` over just those four means schema-evolution API churn
in the client cannot break this package, and lets the test suite substitute a
recording fake without patching anything.
"""

from typing import Any, Protocol, runtime_checkable


@runtime_checkable
class XmemoryInstanceProtocol(Protocol):
    """Structurally satisfied by ``xmemory.AsyncInstanceAPI``.

    Return types are intentionally ``Any``: the concrete pydantic models belong
    to ``xmemory-ai`` and are mapped onto this package's own DTOs at the
    activity boundary (see ``dto.py`` for why they must not cross the wire).
    """

    async def read(
        self,
        query: str,
        *,
        read_mode: Any = ...,
        scope: Any = ...,
        read_id: str | None = ...,
        timeout: float | None = ...,
    ) -> Any: ...

    async def write(
        self,
        text: str,
        *,
        extraction_logic: Any = ...,
        diff_engine: bool | None = ...,
        timeout: float | None = ...,
    ) -> Any: ...

    async def write_async(
        self,
        text: str,
        *,
        extraction_logic: Any = ...,
        diff_engine: bool | None = ...,
        timeout: float | None = ...,
    ) -> Any: ...

    async def write_status(self, write_id: str, *, timeout: float | None = ...) -> Any: ...
