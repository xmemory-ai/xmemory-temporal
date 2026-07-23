"""Temporal plugin for xmemory — durable agent memory as Temporal Activities.

Register ``XmemoryPlugin`` on your Temporal ``Client`` (the Worker inherits its
client's plugins) — or on the ``Worker`` — but **never both**: registering twice
adds the activities twice and the worker crashes at boot with "More than one
activity named xmemory_read". Then call ``xmemory_for_workflow`` inside a
workflow to read and write memory through replay-safe activities.
"""

from xmemory_temporal.activities import (
    ACTIVITY_READ,
    ACTIVITY_WRITE,
    ACTIVITY_WRITE_START,
    ACTIVITY_WRITE_STATUS,
    XmemoryActivities,
)
from xmemory_temporal.config import XmemoryConfig, XmemoryTimeouts
from xmemory_temporal.dto import (
    ReadInput,
    ReadOutput,
    SubAnswer,
    WriteInput,
    WriteOutput,
    WriteStartOutput,
    WriteStatusInput,
    WriteStatusOutput,
)
from xmemory_temporal.errors import NON_RETRYABLE_TYPES, to_application_error
from xmemory_temporal.interceptor import AutoCaptureConfig
from xmemory_temporal.plugin import XmemoryPlugin
from xmemory_temporal.protocol import XmemoryInstanceProtocol
from xmemory_temporal.workflow_api import WorkflowXmemory, xmemory_for_workflow

__all__ = [
    "XmemoryPlugin",
    "XmemoryConfig",
    "XmemoryTimeouts",
    "AutoCaptureConfig",
    "WorkflowXmemory",
    "xmemory_for_workflow",
    "XmemoryActivities",
    "XmemoryInstanceProtocol",
    "ReadInput",
    "ReadOutput",
    "SubAnswer",
    "WriteInput",
    "WriteOutput",
    "WriteStartOutput",
    "WriteStatusInput",
    "WriteStatusOutput",
    "NON_RETRYABLE_TYPES",
    "to_application_error",
    "ACTIVITY_READ",
    "ACTIVITY_WRITE",
    "ACTIVITY_WRITE_START",
    "ACTIVITY_WRITE_STATUS",
]
