from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
import sys
import types

from coatue_claw.x_chart_daily import (
    Candidate,
    XChartStore,
    _build_x_title,
    _extract_rebuilt_bars,
    _extract_rebuilt_series,
    _infer_chart_mode,
    _is_us_relevant_post,
    _normalize_render_text,
    _parse_windows,
    _parse_x_candidates,
    _post_winner_to_slack,
    _select_style_draft,
    _shorten_without_ellipsis,
    _slack_tokens,
    run_chart_scout_once,
)


def test_parse_windows_defaults_and_custom() -> None:
    assert _parse_windows("09:00,12:00,18:00") == [(9, 0), (12, 0), (18, 0)]
    assert _parse_windows("bad") == [(9, 0), (12, 0), (18, 0)]
    assert _parse_windows("8:30, 21:15") == [(8, 30), (21, 15)]


def test_store_seeds_fiscal_ai(tmp_path: Path, monkeypatch) -> None:
    db = tmp_path / "x_chart.sqlite"
    monkeypatch.setenv("COATUE_CLAW_X_CHART_DB_PATH", str(db))
    store = XChartStore()
    handles = {item["handle"] for item in store.list_sources(limit=200)}
    assert "fiscal_AI".lower() in {h.lower() for h in handles}


def test_run_chart_scout_dry_run(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setenv("COATUE_CLAW_DATA_ROOT", str(tmp_path))
    monkeypatch.setenv("COATUE_CLAW_X_CHART_DB_PATH", str(tmp_path / "db/x_chart.sqlite"))
    monkeypatch.setenv("COATUE_CLAW_X_BEARER_TOKEN", "test-token")
    monkeypatch.setenv("COATUE_CLAW_X_CHART_WINDOWS", "09:00,12:00,18:00")
    monkeypatch.setenv("COATUE_CLAW_X_CHART_TIMEZONE", "UTC")

    monkeypatch.setattr("coatue_claw.x_chart_daily._discover_new_sources", lambda **kwargs: [])
    monkeypatch.setattr("coatue_claw.x_chart_daily._fetch_visualcapitalist_candidates", lambda **kwargs: [])
    monkeypatch.setattr(
        "coatue_claw.x_chart_daily._fetch_x_candidates_from_sources",
        lambda **kwargs: [
            Candidate(
                candidate_key="x:1",
                source_type="x",
                source_id="fiscal_AI",
                author="@fiscal_AI",
                title="Fiscal AI trend chart",
                text="US AI software demand trend chart",
                url="https://x.com/fiscal_AI/status/1",
                image_url="https://example.com/chart.png",
                created_at=datetime.now(UTC).isoformat(),
                engagement=500,
                source_priority=1.6,
                score=95.0,
            )
        ],
    )

    class Frozen(datetime):
        @classmethod
        def now(cls, tz=None):
            base = datetime(2026, 2, 19, 9, 0, 0, tzinfo=UTC)
            if tz is None:
                return base
            return base.astimezone(tz)

    monkeypatch.setattr("coatue_claw.x_chart_daily.datetime", Frozen)

    result = run_chart_scout_once(manual=False, dry_run=True)
    assert result["ok"] is True
    assert result["reason"] == "dry_run"
    assert result["winner"]["source"] == "x:fiscal_AI"


def test_slack_token_falls_back_to_openclaw_config(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.delenv("SLACK_BOT_TOKEN", raising=False)
    monkeypatch.setenv("HOME", str(tmp_path))
    cfg_dir = tmp_path / ".openclaw"
    cfg_dir.mkdir(parents=True, exist_ok=True)
    cfg = cfg_dir / "openclaw.json"
    cfg.write_text(
        '{"channels":{"slack":{"botToken":"xoxb-fallback-token"}}}',
        encoding="utf-8",
    )
    assert _slack_tokens() == ["xoxb-fallback-token"]


def test_slack_tokens_include_env_then_config(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setenv("SLACK_BOT_TOKEN", "xoxb-env-token")
    monkeypatch.setenv("HOME", str(tmp_path))
    cfg_dir = tmp_path / ".openclaw"
    cfg_dir.mkdir(parents=True, exist_ok=True)
    cfg = cfg_dir / "openclaw.json"
    cfg.write_text(
        '{"channels":{"slack":{"botToken":"xoxb-config-token"}}}',
        encoding="utf-8",
    )
    assert _slack_tokens() == ["xoxb-env-token", "xoxb-config-token"]


def test_parse_x_candidates_filters_non_chart_text() -> None:
    payload = {
        "data": [
            {
                "id": "t1",
                "author_id": "u1",
                "text": "BREAKING: leadership change expected soon.",
                "created_at": "2026-02-19T00:00:00Z",
                "public_metrics": {"like_count": 100, "retweet_count": 50, "reply_count": 20, "quote_count": 10},
                "attachments": {"media_keys": ["m1"]},
            }
        ],
        "includes": {
            "users": [{"id": "u1", "username": "KobeissiLetter"}],
            "media": [{"media_key": "m1", "type": "photo", "url": "https://example.com/image.png"}],
        },
    }
    parsed = _parse_x_candidates(payload, priority_by_handle={"kobeissiletter": 1.3})
    assert parsed == []


def test_parse_x_candidates_accepts_chart_signal_text() -> None:
    payload = {
        "data": [
            {
                "id": "t2",
                "author_id": "u2",
                "text": "New US chart: S&P software revenue growth hit 42% YoY.",
                "created_at": "2026-02-19T00:00:00Z",
                "public_metrics": {"like_count": 10, "retweet_count": 5, "reply_count": 2, "quote_count": 1},
                "attachments": {"media_keys": ["m2"]},
            }
        ],
        "includes": {
            "users": [{"id": "u2", "username": "fiscal_AI"}],
            "media": [{"media_key": "m2", "type": "photo", "url": "https://example.com/chart.png"}],
        },
    }
    parsed = _parse_x_candidates(payload, priority_by_handle={"fiscal_ai": 1.6})
    assert len(parsed) == 1
    assert parsed[0].source_id == "fiscal_AI"


def test_parse_x_candidates_rejects_non_us_forex_posts() -> None:
    payload = {
        "data": [
            {
                "id": "t3",
                "author_id": "u3",
                "text": "Chart: Turkish Lira vs U.S. Dollar now down 97% since 2010.",
                "created_at": "2026-02-19T00:00:00Z",
                "public_metrics": {"like_count": 500, "retweet_count": 200, "reply_count": 90, "quote_count": 50},
                "attachments": {"media_keys": ["m3"]},
            }
        ],
        "includes": {
            "users": [{"id": "u3", "username": "Barchart"}],
            "media": [{"media_key": "m3", "type": "photo", "url": "https://example.com/forex.png"}],
        },
    }
    parsed = _parse_x_candidates(payload, priority_by_handle={"barchart": 1.2})
    assert parsed == []


def test_us_relevance_classifier_prefers_us_topics() -> None:
    assert _is_us_relevant_post("US CPI cools while S&P 500 makes a new high chart") is True
    assert _is_us_relevant_post("EUR/USD forex trend update with no US equity angle") is False


def test_render_text_normalization_removes_garbled_characters() -> None:
    raw = "BREAKING 🚨 Turkey Lira falls 97% � https://x.com/example"
    normalized = _normalize_render_text(raw)
    assert "🚨" not in normalized
    assert "�" not in normalized
    assert "https://" not in normalized
    assert "Turkey Lira falls 97%" in normalized


def test_style_draft_prefers_simple_feed_like_copy() -> None:
    candidate = Candidate(
        candidate_key="x:99",
        source_type="x",
        source_id="fiscal_AI",
        author="@fiscal_AI",
        title="@fiscal_AI: US cloud software growth re-accelerates to 29% YoY.",
        text="US cloud software growth re-accelerates to 29% YoY and valuations follow.",
        url="https://x.com/fiscal_AI/status/99",
        image_url="https://example.com/chart.png",
        created_at=datetime.now(UTC).isoformat(),
        engagement=700,
        source_priority=1.6,
        score=101.0,
    )
    draft = _select_style_draft(candidate)
    assert draft.checks["us_relevant"] is True
    assert draft.checks["trend_explicit"] is True
    assert draft.checks["graph_first_copy"] is True
    assert len(draft.headline) <= 72
    assert len(draft.takeaway) <= 96
    assert draft.score >= 6.0


def test_post_winner_uploads_file_in_initial_message(monkeypatch, tmp_path: Path) -> None:
    candidate = Candidate(
        candidate_key="x:88",
        source_type="x",
        source_id="fiscal_AI",
        author="@fiscal_AI",
        title="@fiscal_AI: US software growth re-accelerates to 29% YoY.",
        text="US software growth re-accelerates to 29% YoY while enterprise spending remains resilient.",
        url="https://x.com/fiscal_AI/status/88",
        image_url="https://example.com/chart.png",
        created_at=datetime.now(UTC).isoformat(),
        engagement=450,
        source_priority=1.6,
        score=98.0,
    )
    styled = tmp_path / "styled.png"
    styled.write_bytes(b"fake")

    upload_calls: list[dict[str, object]] = []

    class FakeWebClient:
        def __init__(self, token: str) -> None:
            self.token = token

        def files_upload_v2(self, **kwargs):
            upload_calls.append(kwargs)
            return {"ok": True, "file": {"id": "F123"}}

    class FakeSlackApiError(Exception):
        def __init__(self, error: str) -> None:
            self.response = {"error": error}
            super().__init__(error)

    monkeypatch.setitem(sys.modules, "slack_sdk", types.SimpleNamespace(WebClient=FakeWebClient))
    monkeypatch.setitem(sys.modules, "slack_sdk.errors", types.SimpleNamespace(SlackApiError=FakeSlackApiError))
    monkeypatch.setattr("coatue_claw.x_chart_daily._slack_tokens", lambda: ["xoxb-test"])
    monkeypatch.setattr("coatue_claw.x_chart_daily._render_chart_of_day_style", lambda **kwargs: styled)

    result = _post_winner_to_slack(candidate=candidate, channel="C123", slot_key="manual-1", windows_text="09:00,12:00,18:00")
    assert result["ok"] is True
    assert result["channel"] == "C123"
    assert result["file_id"] == "F123"
    assert len(upload_calls) == 1
    assert upload_calls[0]["channel"] == "C123"
    assert upload_calls[0]["file"] == str(styled)
    assert "initial_comment" in upload_calls[0]
    assert "thread_ts" not in upload_calls[0]


def test_shorten_without_ellipsis_removes_three_dots() -> None:
    text = "Non-asset owners are being left behind: US consumer sentiment among non-stockholders keeps sliding"
    shortened = _shorten_without_ellipsis(text, max_chars=58)
    assert "..." not in shortened
    assert len(shortened) <= 58


def test_build_x_title_has_no_ellipsis() -> None:
    title = _build_x_title(handle="fiscal_AI", text="This is a very long sentence " * 12)
    assert "..." not in title


def test_extract_rebuilt_series_from_synthetic_line_chart() -> None:
    try:
        import numpy as np
    except Exception:
        return

    image = np.ones((420, 720, 3), dtype=float)
    x_start, x_end = 90, 680
    y_top, y_bottom = 80, 360
    image[y_top:y_bottom, x_start : x_start + 2, :] = 0.0
    image[y_bottom - 2 : y_bottom, x_start:x_end, :] = 0.0

    width = x_end - x_start
    for i in range(width):
        x = x_start + i
        y1 = int(300 - (i * 0.25))
        y2 = int(260 - (i * 0.18))
        y1 = max(y_top + 5, min(y_bottom - 5, y1))
        y2 = max(y_top + 5, min(y_bottom - 5, y2))
        image[max(0, y1 - 1) : y1 + 1, max(0, x - 1) : x + 1, :] = [0.18, 0.42, 0.92]
        image[max(0, y2 - 1) : y2 + 1, max(0, x - 1) : x + 1, :] = [0.30, 0.70, 0.52]

    candidate = Candidate(
        candidate_key="x:synthetic",
        source_type="x",
        source_id="fiscal_AI",
        author="@fiscal_AI",
        title="@fiscal_AI: US synthetic series",
        text="US chart synthetic trend",
        url="https://x.com/fiscal_AI/status/synthetic",
        image_url="https://example.com/synthetic.png",
        created_at=datetime.now(UTC).isoformat(),
        engagement=100,
        source_priority=1.6,
        score=95.0,
    )
    series = _extract_rebuilt_series(candidate=candidate, image=image)
    assert len(series) >= 1
    assert all(len(s.x) == len(s.y) for s in series)


def test_infer_chart_mode_prefers_bar_when_text_says_bar_chart() -> None:
    candidate = Candidate(
        candidate_key="x:bar",
        source_type="x",
        source_id="fiscal_AI",
        author="@fiscal_AI",
        title="@fiscal_AI: New bar chart on US enrollment by cohort",
        text="Bar chart shows US enrollment by cohort.",
        url="https://x.com/fiscal_AI/status/bar",
        image_url="https://example.com/bar.png",
        created_at=datetime.now(UTC).isoformat(),
        engagement=120,
        source_priority=1.6,
        score=90.0,
    )
    assert _infer_chart_mode(candidate=candidate, image=None) == "bar"


def test_extract_rebuilt_bars_from_synthetic_bars() -> None:
    try:
        import numpy as np
    except Exception:
        return
    image = np.ones((420, 720, 3), dtype=float)
    x0, x1 = 120, 640
    y0, y1 = 90, 360
    image[y0:y1, x0 : x0 + 2, :] = 0.0
    image[y1 - 2 : y1, x0:x1, :] = 0.0
    bars = [(150, 12), (230, 45), (310, 30), (390, 70), (470, 55), (550, 35)]
    for center, height in bars:
        left, right = max(x0 + 5, center - 12), min(x1 - 5, center + 12)
        top = max(y0 + 8, y1 - height * 3)
        image[top:y1, left:right, :] = [0.2, 0.44, 0.86]
    rebuilt = _extract_rebuilt_bars(image=image)
    assert rebuilt is not None
    assert len(rebuilt.values) >= 3
