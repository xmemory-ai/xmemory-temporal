"""The four client methods this plugin uses, as a structural Protocol.

Depending on four of ``AsyncInstanceAPI``'s fourteen keeps client API churn from
breaking us, and lets tests substitute a fake with no patching.
"""

from typing import Any, Protocol, runtime_checkable


@runtime_checkable
class XmemoryInstanceProtocol(Protocol):
    """Structurally satisfied by ``xmemory.AsyncInstanceAPI``.

    Returns are ``Any``: vendor models are mapped to our DTOs at the activity
    boundary (see ``dto.py``).
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
