"""Local patch: pre-visible overload failures on the direct stream path fail over.

Upstream relays ``response.created`` immediately, so a following
``response.failed(server_is_overloaded)`` is treated as downstream-visible and
surfaced without trying another account. With
``stream_previsible_transient_failover_enabled`` the lifecycle frames are held
until the first real frame, and an overload failure before that point fails
over transparently to another account.
"""

from __future__ import annotations

import base64
import json

import pytest

import app.modules.proxy.service as proxy_module
from app.core.auth import generate_unique_account_id
from app.core.config.settings import get_settings

pytestmark = pytest.mark.integration


@pytest.fixture(autouse=True)
async def _force_usage_weighted_routing(async_client) -> None:
    current = await async_client.get("/api/settings")
    assert current.status_code == 200
    payload = current.json()
    payload["routingStrategy"] = "usage_weighted"
    response = await async_client.put("/api/settings", json=payload)
    assert response.status_code == 200


def _encode_jwt(payload: dict) -> str:
    raw = json.dumps(payload, separators=(",", ":")).encode("utf-8")
    body = base64.urlsafe_b64encode(raw).rstrip(b"=").decode("ascii")
    return f"header.{body}.sig"


async def _import_account(async_client, account_id: str, email: str) -> str:
    payload = {
        "email": email,
        "chatgpt_account_id": account_id,
        "https://api.openai.com/auth": {"chatgpt_plan_type": "plus"},
    }
    auth_json = {
        "tokens": {
            "idToken": _encode_jwt(payload),
            "accessToken": "access-token",
            "refreshToken": "refresh-token",
            "accountId": account_id,
        },
    }
    files = {"auth_json": ("auth.json", json.dumps(auth_json), "application/json")}
    response = await async_client.post("/api/accounts/import", files=files)
    assert response.status_code == 200
    return generate_unique_account_id(account_id, email)


def _sse_event(payload: dict) -> str:
    return f"data: {json.dumps(payload)}\n\n"


def _created(response_id: str) -> str:
    return _sse_event({"type": "response.created", "response": {"id": response_id, "status": "in_progress"}})


def _in_progress(response_id: str) -> str:
    return _sse_event({"type": "response.in_progress", "response": {"id": response_id, "status": "in_progress"}})


def _metadata(response_id: str) -> str:
    return _sse_event({"type": "response.metadata", "response_id": response_id, "turn_state": "ts_" + response_id})


def _text_delta() -> str:
    return _sse_event({"type": "response.output_text.delta", "delta": "hi"})


def _overloaded(response_id: str, code: str = "server_is_overloaded") -> str:
    return _sse_event(
        {
            "type": "response.failed",
            "response": {
                "id": response_id,
                "error": {"code": code, "message": "Our servers are currently overloaded. Please try again later."},
            },
        }
    )


def _completed(response_id: str) -> str:
    return _sse_event(
        {
            "type": "response.completed",
            "response": {"id": response_id, "usage": {"input_tokens": 1, "output_tokens": 1}},
        }
    )


def _extract_events(lines: list[str]) -> list[dict]:
    """Decode data frames, dropping proxy keepalives so assertions see only response events."""
    events = []
    for line in lines:
        if line.startswith("data: "):
            data = line[6:]
            if data == "[DONE]":
                continue
            event = json.loads(data)
            if event.get("type") == "codex.keepalive":
                continue
            events.append(event)
    return events


async def _run(async_client, monkeypatch, *, enabled: bool, first_account_frames: list[str]):
    """First account called yields ``first_account_frames``; every other account succeeds."""
    monkeypatch.setattr(get_settings(), "stream_previsible_transient_failover_enabled", enabled)
    await _import_account(async_client, "acc_pv_a", "pv_a@example.com")
    await _import_account(async_client, "acc_pv_b", "pv_b@example.com")
    seen_account_ids: list[str | None] = []

    async def fake_stream(payload, headers, access_token, account_id, base_url=None, raise_for_status=False):
        seen_account_ids.append(account_id)
        if account_id == seen_account_ids[0] and len(seen_account_ids) == 1:
            for frame in first_account_frames:
                yield frame
            return
        yield _created("resp_ok")
        yield _completed("resp_ok")

    monkeypatch.setattr(proxy_module, "core_stream_responses", fake_stream)
    payload = {"model": "gpt-5.1", "instructions": "hi", "input": [], "stream": True}
    async with async_client.stream("POST", "/backend-api/codex/responses", json=payload) as resp:
        assert resp.status_code == 200
        lines = [line async for line in resp.aiter_lines() if line]
    return _extract_events(lines), seen_account_ids


@pytest.mark.asyncio
@pytest.mark.parametrize("error_code", ["server_is_overloaded", "overloaded_error"])
async def test_created_then_overloaded_fails_over_without_leaking_lifecycle(async_client, monkeypatch, error_code):
    events, seen = await _run(
        async_client,
        monkeypatch,
        enabled=True,
        first_account_frames=[
            _created("resp_bad"),
            _in_progress("resp_bad"),
            _metadata("resp_bad"),
            _overloaded("resp_bad", error_code),
        ],
    )
    types = [e.get("type") for e in events]
    assert "response.failed" not in types
    created = [e for e in events if e.get("type") == "response.created"]
    assert [e["response"]["id"] for e in created] == ["resp_ok"]
    assert types.count("response.completed") == 1
    assert len(seen) == 2 and seen[0] != seen[1]


@pytest.mark.asyncio
async def test_first_event_overloaded_fails_over(async_client, monkeypatch):
    events, seen = await _run(
        async_client,
        monkeypatch,
        enabled=True,
        first_account_frames=[_overloaded("resp_bad")],
    )
    types = [e.get("type") for e in events]
    assert "response.failed" not in types
    assert types.count("response.completed") == 1
    assert len(seen) == 2 and seen[0] != seen[1]


@pytest.mark.asyncio
async def test_overloaded_after_visible_output_still_surfaces(async_client, monkeypatch):
    events, seen = await _run(
        async_client,
        monkeypatch,
        enabled=True,
        first_account_frames=[_created("resp_bad"), _text_delta(), _overloaded("resp_bad")],
    )
    types = [e.get("type") for e in events]
    # Once output reached the client the held frames were flushed in order and the failure surfaces.
    assert types.index("response.created") < types.index("response.output_text.delta")
    failed = [e for e in events if e.get("type") == "response.failed"]
    assert len(failed) == 1
    assert failed[0]["response"]["error"]["code"] == "server_is_overloaded"
    assert len(seen) == 1


@pytest.mark.asyncio
async def test_toggle_off_keeps_upstream_behavior(async_client, monkeypatch):
    events, seen = await _run(
        async_client,
        monkeypatch,
        enabled=False,
        first_account_frames=[_created("resp_bad"), _in_progress("resp_bad"), _overloaded("resp_bad")],
    )
    types = [e.get("type") for e in events]
    assert types[:2] == ["response.created", "response.in_progress"]
    failed = [e for e in events if e.get("type") == "response.failed"]
    assert len(failed) == 1
    assert failed[0]["response"]["error"]["code"] == "server_is_overloaded"
    assert len(seen) == 1


@pytest.mark.asyncio
async def test_success_path_relays_lifecycle_in_order(async_client, monkeypatch):
    events, seen = await _run(
        async_client,
        monkeypatch,
        enabled=True,
        first_account_frames=[
            _created("resp_a"),
            _in_progress("resp_a"),
            _metadata("resp_a"),
            _text_delta(),
            _completed("resp_a"),
        ],
    )
    types = [e.get("type") for e in events]
    assert types == [
        "response.created",
        "response.in_progress",
        "response.metadata",
        "response.output_text.delta",
        "response.completed",
    ]
    assert len(seen) == 1


@pytest.mark.asyncio
async def test_pinned_continuation_retries_same_account(async_client, monkeypatch):
    """A previous_response_id continuation is pinned to its owner, so overload retries stay on that account."""
    monkeypatch.setattr(get_settings(), "stream_previsible_transient_failover_enabled", True)
    await _import_account(async_client, "acc_pin_a", "pin_a@example.com")
    await _import_account(async_client, "acc_pin_b", "pin_b@example.com")
    anchor_calls: list[str | None] = []
    followup_calls: list[str | None] = []

    async def fake_stream(payload, headers, access_token, account_id, base_url=None, raise_for_status=False):
        if payload.previous_response_id is None:
            anchor_calls.append(account_id)
            yield _created("resp_pin_anchor")
            yield _completed("resp_pin_anchor")
            return
        followup_calls.append(account_id)
        if len(followup_calls) == 1:
            yield _created("resp_pin_bad")
            yield _in_progress("resp_pin_bad")
            yield _overloaded("resp_pin_bad")
            return
        yield _created("resp_pin_ok")
        yield _completed("resp_pin_ok")

    monkeypatch.setattr(proxy_module, "core_stream_responses", fake_stream)
    base = {"model": "gpt-5.1", "instructions": "hi", "input": [], "stream": True}
    async with async_client.stream("POST", "/backend-api/codex/responses", json=base) as resp:
        assert resp.status_code == 200
        _ = [line async for line in resp.aiter_lines() if line]
    followup = {**base, "previous_response_id": "resp_pin_anchor"}
    async with async_client.stream("POST", "/backend-api/codex/responses", json=followup) as resp:
        assert resp.status_code == 200
        lines = [line async for line in resp.aiter_lines() if line]
    events = _extract_events(lines)
    types = [e.get("type") for e in events]
    assert "response.failed" not in types
    assert [e["response"]["id"] for e in events if e.get("type") == "response.created"] == ["resp_pin_ok"]
    assert types.count("response.completed") == 1
    assert len(anchor_calls) == 1
    assert followup_calls == [anchor_calls[0], anchor_calls[0]]
