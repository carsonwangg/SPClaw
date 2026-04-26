from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import pytest

from spclaw import dev_buzz
from spclaw.slack_dev_buzz_intent import parse_dev_buzz_intent


def _setup_env(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SPCLAW_DATA_ROOT", str(tmp_path))
    monkeypatch.setenv("SPCLAW_DEV_BUZZ_DB_PATH", str(tmp_path / "db/dev_buzz.sqlite"))
    monkeypatch.setenv("SPCLAW_DEV_BUZZ_ARTIFACT_DIR", str(tmp_path / "artifacts/dev-buzz"))
    monkeypatch.setenv("SPCLAW_X_BEARER_TOKEN", "test-token")
    monkeypatch.delenv("OPENAI_API_KEY", raising=False)


def _fake_payload() -> dict:
    return {
        "data": [
            {
                "id": "1",
                "author_id": "u1",
                "text": "We launched New Runtime for developers today https://t.co/a",
                "created_at": "2026-04-20T16:00:00Z",
                "public_metrics": {"like_count": 100, "retweet_count": 20, "reply_count": 8, "quote_count": 4},
                "entities": {"urls": [{"url": "https://t.co/a", "expanded_url": "https://example.com/new-runtime"}]},
            },
            {
                "id": "2",
                "author_id": "u2",
                "text": "New Runtime looks like a meaningful platform shift for infra teams https://t.co/b",
                "created_at": "2026-04-20T17:00:00Z",
                "public_metrics": {"like_count": 50, "retweet_count": 10, "reply_count": 2, "quote_count": 1},
                "entities": {"urls": [{"url": "https://t.co/b", "expanded_url": "https://example.com/new-runtime"}]},
            },
        ],
        "includes": {
            "users": [
                {"id": "u1", "username": "toolco", "name": "Tool Co"},
                {"id": "u2", "username": "engineer", "name": "Engineer"},
            ]
        },
    }


def test_parse_posts_and_canonicalizes_linked_release() -> None:
    posts = dev_buzz._parse_posts(_fake_payload())
    assert len(posts) == 2
    assert posts[0].url == "https://x.com/toolco/status/1"
    assert posts[0].engagement == 132
    assert dev_buzz._canonical_key(posts[0]) == dev_buzz._canonical_key(posts[1])


def test_collect_dedupes_and_applies_llm_editor(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _setup_env(tmp_path, monkeypatch)

    def fake_fetch_json(*, url: str, headers: dict[str, str], params: dict[str, str]) -> dict:
        assert url.endswith("/2/tweets/search/recent")
        assert headers["Authorization"] == "Bearer test-token"
        assert "-is:retweet" in params["query"]
        return _fake_payload()

    def fake_editor(messages):
        text = messages[-1]["content"]
        assert "New Runtime" in text
        item_id = dev_buzz._item_id("url:example.com/new-runtime")
        return {
            "shortlist": [
                {
                    "item_id": item_id,
                    "rank": 1,
                    "headline": "New Runtime launches for infra developers",
                    "category": "Developer infrastructure",
                    "why_matters": "It changes how infra teams ship runtime workloads.",
                    "rationale": "Multiple engineers discussed the launch and linked the same release.",
                    "confidence": "high",
                    "friday_worthy": True,
                    "reject_reason": "",
                }
            ]
        }

    result = dev_buzz.collect(
        manual=True,
        now_utc=datetime(2026, 4, 20, 19, 0, tzinfo=UTC),
        fetch_json=fake_fetch_json,
        llm_editor=fake_editor,
    )

    assert result["editor_mode"] == "llm"
    rows = dev_buzz.DevBuzzStore().shortlist()
    assert len(rows) == 1
    assert rows[0]["headline"] == "New Runtime launches for infra developers"
    assert rows[0]["observation_count"] == 2


def test_malformed_llm_response_falls_back(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _setup_env(tmp_path, monkeypatch)

    result = dev_buzz.collect(
        manual=True,
        now_utc=datetime(2026, 4, 20, 19, 0, tzinfo=UTC),
        fetch_json=lambda **_: _fake_payload(),
        llm_editor=lambda _messages: {"not_shortlist": []},
    )

    assert result["editor_mode"] == "fallback"
    assert result["fallback_reason"] == "test_editor_returned_invalid_payload"
    assert dev_buzz.DevBuzzStore().shortlist(limit=5)


def test_pin_drop_and_explain(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _setup_env(tmp_path, monkeypatch)
    dev_buzz.collect(
        manual=True,
        now_utc=datetime(2026, 4, 20, 19, 0, tzinfo=UTC),
        fetch_json=lambda **_: _fake_payload(),
        llm_editor=lambda _messages: {"shortlist": []},
    )
    item_id = dev_buzz._item_id("url:example.com/new-runtime")
    assert dev_buzz.pin(item_id)["pinned"] is True
    assert dev_buzz.explain(item_id)["item"]["pinned"] == 1
    assert dev_buzz.drop(item_id)["dropped"] is True
    assert dev_buzz.DevBuzzStore().shortlist(limit=5) == []


def test_publish_dry_run_writes_artifact_without_slack(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _setup_env(tmp_path, monkeypatch)
    item_id = dev_buzz._item_id("url:example.com/new-runtime")
    dev_buzz.collect(
        manual=True,
        now_utc=datetime(2026, 4, 20, 19, 0, tzinfo=UTC),
        fetch_json=lambda **_: _fake_payload(),
        llm_editor=lambda _messages: {
            "shortlist": [
                {
                    "item_id": item_id,
                    "rank": 1,
                    "headline": "New Runtime launches for infra developers",
                    "category": "Developer infrastructure",
                    "why_matters": "It changes how infra teams ship runtime workloads.",
                    "rationale": "Strong engineer discussion.",
                    "confidence": "high",
                    "friday_worthy": True,
                    "reject_reason": "",
                }
            ]
        },
    )

    result = dev_buzz.publish(
        dry_run=True,
        force=True,
        now_utc=datetime(2026, 4, 24, 23, 0, tzinfo=UTC),
    )

    assert result["status"] == "dry_run"
    assert result["message_ts"] is None
    artifact = Path(result["artifact_path"])
    assert artifact.exists()
    assert "New Runtime launches" in artifact.read_text(encoding="utf-8")


def test_slack_dev_buzz_intents() -> None:
    assert parse_dev_buzz_intent("dev buzz status").kind == "status"
    assert parse_dev_buzz_intent("dev buzz collect now").kind == "collect"
    assert parse_dev_buzz_intent("dev buzz dry run").kind == "publish_dry_run"
    assert parse_dev_buzz_intent("dev buzz add source @vercel").kind == "add_source"
    assert parse_dev_buzz_intent("dev buzz add keyword AI coding launch").value == "AI coding launch"
    assert parse_dev_buzz_intent("dev buzz pin dbz_abc123").kind == "pin"
