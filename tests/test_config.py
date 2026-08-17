"""Config: credential sourcing and the no-secret-in-history guarantee."""

import pytest

from xmemory_temporal import XmemoryConfig, XmemoryTimeouts
from xmemory_temporal.config import client_timeout_seconds


def test_api_key_never_serialized() -> None:
    # The config carries the env var NAME, never the key. Nothing that could
    # leak a secret into Temporal history should appear in a dump.
    cfg = XmemoryConfig(instance_id="inst-1", api_key_env="MY_XMEM_KEY")
    dumped = cfg.model_dump_json()
    assert "MY_XMEM_KEY" in dumped  # the var name is fine
    assert "xmem_" not in dumped
    assert "api_key" not in cfg.model_dump()


def test_resolve_api_key_reads_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("XMEM_API_KEY", "xmem_secret")
    cfg = XmemoryConfig(instance_id="inst-1")
    assert cfg.resolve_api_key() == "xmem_secret"


def test_resolve_api_key_custom_var(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("OTHER_KEY", "xmem_other")
    cfg = XmemoryConfig(instance_id="inst-1", api_key_env="OTHER_KEY")
    assert cfg.resolve_api_key() == "xmem_other"


def test_missing_key_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("XMEM_API_KEY", raising=False)
    cfg = XmemoryConfig(instance_id="inst-1")
    with pytest.raises(ValueError, match="XMEM_API_KEY"):
        cfg.resolve_api_key()


def test_frozen() -> None:
    cfg = XmemoryConfig(instance_id="inst-1")
    with pytest.raises(Exception):
        cfg.instance_id = "other"  # type: ignore[misc]


def test_client_timeout_below_every_activity_budget() -> None:
    # The client (httpx) must give up before Temporal for EVERY budget a workflow
    # could choose, not just the comfortable defaults. Budgets at or under the
    # margin get a proportional one, so there is no input that inverts the order.
    t = XmemoryTimeouts()
    defaults = [t.read_seconds, t.write_seconds, t.write_start_seconds, t.write_status_seconds]
    for budget in [*defaults, 0.5, 1, 2, 5, 6, 10, 3600]:
        assert client_timeout_seconds(budget) < budget


def test_client_timeout_stays_below_every_activity_budget() -> None:
    # The whole point of the margin is that the client fails first. A budget at
    # or under the margin, and a nonsensical margin, must not invert that.
    for activity, margin in [(0.05, 5), (5, 0), (30, -1), (120, 5), (0.2, 5)]:
        used = client_timeout_seconds(activity, margin)
        assert 0 < used < activity, f"activity={activity} margin={margin} gave {used}"

    with pytest.raises(ValueError):
        client_timeout_seconds(0, 5)


def test_a_supplied_url_must_be_https_or_loopback() -> None:
    # The key travels as a bearer token, so a blank url must not fall through to
    # the default endpoint, and plaintext must not carry the credential.
    assert XmemoryConfig(instance_id="i").resolve_url() is None
    assert XmemoryConfig(instance_id="i", url="https://api.example.com").resolve_url() == "https://api.example.com"
    # Loopback stays usable for local development.
    assert XmemoryConfig(instance_id="i", url="http://localhost:8080").resolve_url() == "http://localhost:8080"

    for bad in ("", "   ", "not-a-url", "http://api.example.com"):
        with pytest.raises(ValueError):
            XmemoryConfig(instance_id="i", url=bad).resolve_url()


def test_the_environment_endpoint_is_validated_too(monkeypatch: pytest.MonkeyPatch) -> None:
    # Validating only `config.url` left the client free to fall back to
    # XMEM_API_URL, which then carried the bearer token to whatever it named.
    monkeypatch.setenv("XMEM_API_URL", "http://attacker.invalid")
    with pytest.raises(ValueError, match="XMEM_API_URL must use https"):
        XmemoryConfig(instance_id="i").resolve_url()

    monkeypatch.setenv("XMEM_API_URL", "https://api.example.com")
    assert XmemoryConfig(instance_id="i").resolve_url() == "https://api.example.com"
    # An explicit url wins over the environment, and is still checked.
    assert XmemoryConfig(instance_id="i", url="https://other.example").resolve_url() == "https://other.example"
    with pytest.raises(ValueError):
        XmemoryConfig(instance_id="i", url="http://attacker.invalid").resolve_url()

    monkeypatch.delenv("XMEM_API_URL")
    assert XmemoryConfig(instance_id="i").resolve_url() is None


def test_an_endpoint_may_not_use_an_odd_scheme_or_embed_credentials() -> None:
    # Loopback relaxes https, not the scheme list, and this config is meant to be
    # safe to log -- so credentials belong in the API key, not the URL.
    assert XmemoryConfig(instance_id="i", url="http://127.0.0.1:8080").resolve_url() == "http://127.0.0.1:8080"
    for bad in ("ftp://localhost/x", "file:///tmp/x", "https://user:pw@api.example.com"):
        with pytest.raises(ValueError):
            XmemoryConfig(instance_id="i", url=bad).resolve_url()


async def test_a_custom_transport_endpoint_is_validated_and_an_empty_key_refused() -> None:
    # Both bypassed validation before: a caller-supplied client carried the key to
    # its own base_url unchecked, and an empty explicit key silently fell back to
    # whatever XMEM_API_KEY held -- a production credential, potentially, sent to a
    # staging endpoint.
    import httpx

    from xmemory_temporal.client_factory import open_instance

    config = XmemoryConfig(instance_id="i")
    async with httpx.AsyncClient(base_url="http://attacker.invalid") as transport:
        with pytest.raises(ValueError, match="must use https"):
            async with open_instance(config, api_key="k", http_client=transport):
                pass

    async with httpx.AsyncClient(base_url="https://staging.example") as transport:
        with pytest.raises(ValueError, match="supplied but empty"):
            async with open_instance(config, api_key="", http_client=transport):
                pass
