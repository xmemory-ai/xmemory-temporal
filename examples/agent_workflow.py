"""A tiny agent-shaped workflow that remembers and recalls through xmemory.

This is the migration story the partner guide asks for: the workflow reads and
writes memory with the same calls a non-Temporal agent would make against
``xmemory.AsyncInstanceAPI`` — only the handle differs.
"""

from temporalio import workflow

with workflow.unsafe.imports_passed_through():
    from xmemory_temporal import xmemory_for_workflow

TASK_QUEUE = "xmemory-example"


@workflow.defn
class SupportAgentWorkflow:
    @workflow.run
    async def run(self, user_name: str, user_message: str) -> str:
        mem = xmemory_for_workflow()

        # Durably remember the fact, attributed to the user by name. A memory
        # store has no ambient "current user" or session — xmemory extracts
        # entities from the text, so the text itself must say whom the fact is
        # about. Survives worker restarts.
        await mem.write_durable(f"{user_name} says: {user_message}")

        # Recall by that same name — "this user" would mean nothing to xmemory;
        # you query for the entity you wrote. (For precise binding to a known
        # record, pass `scope=` to restrict the read to specific objects.)
        #
        # `reader_result` is Any — a natural-language string or a structured
        # object depending on read mode and schema — so coerce it to str to match
        # this workflow's declared return type.
        recalled = await mem.read(f"What do we know about {user_name}?")
        answer = recalled.reader_result
        if answer is None:
            return "(nothing remembered yet)"
        return answer if isinstance(answer, str) else str(answer)
