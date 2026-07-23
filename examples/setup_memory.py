"""Create an xmemory instance with a stable-PK schema for the example workflow.

Run this ONCE, before the worker:

    XMEM_API_KEY=xmem_...  uv run python examples/setup_memory.py

It prints the new instance id — export it for the worker:

    export XMEM_INSTANCE_ID="$(uv run python examples/setup_memory.py)"

The schema keys people by their ``name``, so the example's recall finds the
data (without a schema like this it comes back "not tracked"). NOTE: a name is
an LLM-normalized key, which can fork on re-extraction — so writes to it stay
at-most-once (the default); do not opt into write retries for a name-keyed
schema. See the README's idempotency section.
"""

import json
import os

from xmemory import SchemaType, XmemoryClient


def main() -> None:
    with XmemoryClient(api_key=os.environ["XMEM_API_KEY"], url=os.environ.get("XMEM_API_URL")) as client:
        clusters = client.admin.list_clusters()
        if not clusters:
            raise RuntimeError("no xmemory cluster is available for this account")
        cluster_id = clusters[0].id

        # Generate a schema from a description. NOTE the primary-key instruction:
        # `name` as PK is what makes repeated writes about a person idempotent.
        schema = client.admin.generate_schema(
            cluster_id,
            (
                "Track people the agent talks to. A person is identified by their full "
                "name. Record facts and preferences stated about each person — for "
                "example a preferred contact channel or role. Make name the primary key "
                "so repeated writes about the same person update the same record."
            ),
        )

        instance = client.admin.create_instance(
            cluster_id=cluster_id,
            name="temporal-agent-memory",
            schema_text=json.dumps(schema.data_schema),
            schema_type=SchemaType.JSON,
        )
        print(instance.id)


if __name__ == "__main__":
    main()
