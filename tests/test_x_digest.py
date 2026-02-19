from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import pytest

from coatue_claw.x_digest import XDigestError, build_x_digest


def test_build_x_digest_writes_markdown(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("COATUE_CLAW_DATA_ROOT", str(tmp_path))
    monkeypatch.setenv("COATUE_CLAW_X_BEARER_TOKEN", "test-token")

    captured: dict[str, object] = {}

    def _fake_fetch_json(*, url: str, headers: dict[str, str], params: dict[str, str]) -> dict:
        captured["url"] = url
        captured["headers"] = headers
        captured["params"] = params
        return {
            "data": [
                {
                    "id": "1",
                    "author_id": "u1",
                    "text": "Snowflake posted strong enterprise demand.",
                    "created_at": "2026-02-19T02:00:00Z",
                    "public_metrics": {"like_count": 30, "retweet_count": 20, "reply_count": 5, "quote_count": 2},
                },
                {
                    "id": "2",
                    "author_id": "u2",
                    "text": "Databricks vs Snowflake debate continues in data cloud.",
                    "created_at": "2026-02-19T01:00:00Z",
                    "public_metrics": {"like_count": 5, "retweet_count": 3, "reply_count": 1, "quote_count": 0},
                },
            ],
            "includes": {
                "users": [
                    {"id": "u1", "username": "analyst1", "name": "Analyst One"},
                    {"id": "u2", "username": "analyst2", "name": "Analyst Two"},
                ]
            },
        }

    now = datetime(2026, 2, 19, 12, 0, 0, tzinfo=UTC)
    result = build_x_digest(
        "snowflake",
        hours=24,
        max_results=25,
        now_utc=now,
        fetch_json=_fake_fetch_json,
    )

    assert str(captured["url"]).endswith("/2/tweets/search/recent")
    assert "Authorization" in (captured["headers"] or {})
    params = captured["params"] or {}
    assert params["query"] == "snowflake -is:retweet -is:reply"
    assert params["max_results"] == "25"
    assert result.post_count == 2
    assert result.top_post_url == "https://x.com/analyst1/status/1"
    assert result.output_path.exists()

    content = result.output_path.read_text(encoding="utf-8")
    assert "# X Digest: snowflake" in content
    assert "## Key Takeaways" in content
    assert "https://x.com/analyst1/status/1" in content
    assert "Posts analyzed: `2`" in content


def test_missing_bearer_token_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("COATUE_CLAW_X_BEARER_TOKEN", raising=False)
    monkeypatch.delenv("X_BEARER_TOKEN", raising=False)
    monkeypatch.delenv("COATUE_CLAW_TWITTER_BEARER_TOKEN", raising=False)
    with pytest.raises(XDigestError):
        build_x_digest("snowflake")
