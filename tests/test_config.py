"""Config: credential sourcing and the no-secret-in-history guarantee."""

import pytest

from xmemory_temporal import XmemoryConfig


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
    # F2: the client (httpx) must give up before Temporal for EVERY activity,
    # including the short write_start / write_status ones — no inversion.
    from xmemory_temporal import XmemoryTimeouts

    t = XmemoryTimeouts()
    for budget in (t.read_seconds, t.write_seconds, t.write_start_seconds, t.write_status_seconds):
        assert t.client_timeout(budget) < budget
