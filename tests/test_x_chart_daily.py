from __future__ import annotations

from datetime import UTC, datetime
import json
from pathlib import Path
import sys
import types

from coatue_claw.x_chart_daily import (
    Candidate,
    RebuiltBars,
    XChartStore,
    XChartError,
    _build_x_title,
    _compute_y_ticks,
    _convention_name,
    _fallback_bar_labels,
    _extract_rebuilt_bars_via_vision,
    _extract_rebuilt_bars,
    _extract_rebuilt_series,
    _infer_bar_labels_from_text,
    _infer_chart_mode,
    _is_us_relevant_post,
    _normalize_render_text,
    _parse_windows,
    _parse_x_candidates,
    _pick_winner,
    _post_publish_checklist,
    _post_winner_to_slack,
    _render_chart_of_day_style,
    _select_style_draft,
    _shorten_without_ellipsis,
    _slack_tokens,
    main,
    run_chart_for_post_url,
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


def test_run_chart_scout_outside_window_updates_pool(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setenv("COATUE_CLAW_DATA_ROOT", str(tmp_path))
    monkeypatch.setenv("COATUE_CLAW_X_CHART_DB_PATH", str(tmp_path / "db/x_chart.sqlite"))
    monkeypatch.setenv("COATUE_CLAW_X_BEARER_TOKEN", "test-token")
    monkeypatch.setenv("COATUE_CLAW_X_CHART_WINDOWS", "09:00,12:00,18:00")
    monkeypatch.setenv("COATUE_CLAW_X_CHART_TIMEZONE", "UTC")

    candidate = Candidate(
        candidate_key="x:pool-1",
        source_type="x",
        source_id="fiscal_AI",
        author="@fiscal_AI",
        title="US software momentum rises",
        text="US software momentum rises",
        url="https://x.com/fiscal_AI/status/pool-1",
        image_url="https://example.com/pool-1.png",
        created_at=datetime.now(UTC).isoformat(),
        engagement=120,
        source_priority=1.6,
        score=91.0,
    )

    monkeypatch.setattr("coatue_claw.x_chart_daily._discover_new_sources", lambda **kwargs: [])
    monkeypatch.setattr("coatue_claw.x_chart_daily._fetch_visualcapitalist_candidates", lambda **kwargs: [])
    monkeypatch.setattr("coatue_claw.x_chart_daily._fetch_x_candidates_from_sources", lambda **kwargs: [candidate])

    class Frozen(datetime):
        @classmethod
        def now(cls, tz=None):
            base = datetime(2026, 2, 19, 10, 31, 0, tzinfo=UTC)
            if tz is None:
                return base
            return base.astimezone(tz)

    monkeypatch.setattr("coatue_claw.x_chart_daily.datetime", Frozen)

    result = run_chart_scout_once(manual=False, dry_run=False)
    assert result["ok"] is True
    assert result["posted"] is False
    assert result["reason"] == "scouted_pool_updated"
    assert result["candidates_observed"] == 1

    store = XChartStore()
    pooled = store.observed_candidates_since(since_utc=None, limit=20)
    assert len(pooled) == 1
    assert pooled[0].candidate_key == "x:pool-1"


def test_run_chart_scout_window_uses_hourly_pool_since_last_slot(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setenv("COATUE_CLAW_DATA_ROOT", str(tmp_path))
    monkeypatch.setenv("COATUE_CLAW_X_CHART_DB_PATH", str(tmp_path / "db/x_chart.sqlite"))
    monkeypatch.setenv("COATUE_CLAW_X_BEARER_TOKEN", "test-token")
    monkeypatch.setenv("COATUE_CLAW_X_CHART_WINDOWS", "09:00,12:00,18:00")
    monkeypatch.setenv("COATUE_CLAW_X_CHART_TIMEZONE", "UTC")

    high = Candidate(
        candidate_key="x:high",
        source_type="x",
        source_id="fiscal_AI",
        author="@fiscal_AI",
        title="High score trend",
        text="High score trend",
        url="https://x.com/fiscal_AI/status/high",
        image_url="https://example.com/high.png",
        created_at=datetime.now(UTC).isoformat(),
        engagement=300,
        source_priority=1.6,
        score=99.0,
    )
    lower = Candidate(
        candidate_key="x:low",
        source_type="x",
        source_id="KobeissiLetter",
        author="@KobeissiLetter",
        title="Lower score trend",
        text="Lower score trend",
        url="https://x.com/KobeissiLetter/status/low",
        image_url="https://example.com/low.png",
        created_at=datetime.now(UTC).isoformat(),
        engagement=140,
        source_priority=1.3,
        score=81.0,
    )

    class Frozen(datetime):
        current = datetime(2026, 2, 19, 10, 5, 0, tzinfo=UTC)

        @classmethod
        def now(cls, tz=None):
            base = cls.current
            if tz is None:
                return base
            return base.astimezone(tz)

    def _fetch_candidates(**kwargs):
        if Frozen.current.hour == 10:
            return [high]
        return [lower]

    monkeypatch.setattr("coatue_claw.x_chart_daily.datetime", Frozen)
    monkeypatch.setattr("coatue_claw.x_chart_daily._discover_new_sources", lambda **kwargs: [])
    monkeypatch.setattr("coatue_claw.x_chart_daily._fetch_visualcapitalist_candidates", lambda **kwargs: [])
    monkeypatch.setattr("coatue_claw.x_chart_daily._fetch_x_candidates_from_sources", _fetch_candidates)

    posted: dict[str, str] = {}

    def _fake_post(**kwargs):
        candidate = kwargs["candidate"]
        posted["candidate_url"] = candidate.url
        return {"ok": True, "channel": kwargs["channel"], "file_id": "FTEST"}

    monkeypatch.setattr("coatue_claw.x_chart_daily._post_winner_to_slack", _fake_post)

    first = run_chart_scout_once(manual=False, dry_run=False, channel_override="C123")
    assert first["posted"] is False
    assert first["reason"] == "scouted_pool_updated"

    Frozen.current = datetime(2026, 2, 19, 12, 0, 0, tzinfo=UTC)
    second = run_chart_scout_once(manual=False, dry_run=False, channel_override="C123")
    assert second["posted"] is True
    assert posted["candidate_url"] == "https://x.com/fiscal_AI/status/high"
    assert second["convention"] == "Coatue Chart of the Afternoon"


def test_convention_name_uses_morning_afternoon_evening_windows() -> None:
    now = datetime(2026, 2, 19, 12, 0, 0, tzinfo=UTC)
    windows = [(9, 0), (12, 0), (18, 0)]
    assert _convention_name(slot_key="2026-02-19-09:00", now_local=now, windows=windows) == "Coatue Chart of the Morning"
    assert _convention_name(slot_key="2026-02-19-12:00", now_local=now, windows=windows) == "Coatue Chart of the Afternoon"
    assert _convention_name(slot_key="2026-02-19-18:00", now_local=now, windows=windows) == "Coatue Chart of the Evening"


def test_pick_winner_prefers_variety_within_score_floor(monkeypatch) -> None:
    monkeypatch.setenv("COATUE_CLAW_X_CHART_SOURCE_VARIETY_LOOKBACK", "6")
    monkeypatch.setenv("COATUE_CLAW_X_CHART_SOURCE_VARIETY_SCORE_FLOOR", "0.90")

    class _Store:
        def was_item_posted_recently(self, candidate_key: str, *, days: int = 30) -> bool:
            return False

        def latest_posts(self, *, limit: int = 10):
            return [
                {"source": "x:KobeissiLetter"},
                {"source": "x:KobeissiLetter"},
                {"source": "x:KobeissiLetter"},
                {"source": "x:fiscal_AI"},
            ][:limit]

    kobeissi = Candidate(
        candidate_key="x:top",
        source_type="x",
        source_id="KobeissiLetter",
        author="@KobeissiLetter",
        title="Top",
        text="Top",
        url="https://x.com/KobeissiLetter/status/top",
        image_url="https://example.com/top.png",
        created_at=datetime.now(UTC).isoformat(),
        engagement=1000,
        source_priority=1.3,
        score=100.0,
    )
    fiscal = Candidate(
        candidate_key="x:alt",
        source_type="x",
        source_id="fiscal_AI",
        author="@fiscal_AI",
        title="Alt",
        text="Alt",
        url="https://x.com/fiscal_AI/status/alt",
        image_url="https://example.com/alt.png",
        created_at=datetime.now(UTC).isoformat(),
        engagement=950,
        source_priority=1.6,
        score=93.0,
    )
    picked = _pick_winner(store=_Store(), candidates=[kobeissi, fiscal])
    assert picked is not None
    assert picked.source_id == "fiscal_AI"


def test_pick_winner_keeps_top_when_alternative_too_low(monkeypatch) -> None:
    monkeypatch.setenv("COATUE_CLAW_X_CHART_SOURCE_VARIETY_LOOKBACK", "6")
    monkeypatch.setenv("COATUE_CLAW_X_CHART_SOURCE_VARIETY_SCORE_FLOOR", "0.90")

    class _Store:
        def was_item_posted_recently(self, candidate_key: str, *, days: int = 30) -> bool:
            return False

        def latest_posts(self, *, limit: int = 10):
            return [{"source": "x:KobeissiLetter"}][:limit]

    kobeissi = Candidate(
        candidate_key="x:top2",
        source_type="x",
        source_id="KobeissiLetter",
        author="@KobeissiLetter",
        title="Top2",
        text="Top2",
        url="https://x.com/KobeissiLetter/status/top2",
        image_url="https://example.com/top2.png",
        created_at=datetime.now(UTC).isoformat(),
        engagement=1000,
        source_priority=1.3,
        score=100.0,
    )
    fiscal = Candidate(
        candidate_key="x:low-alt",
        source_type="x",
        source_id="fiscal_AI",
        author="@fiscal_AI",
        title="Low",
        text="Low",
        url="https://x.com/fiscal_AI/status/low",
        image_url="https://example.com/low.png",
        created_at=datetime.now(UTC).isoformat(),
        engagement=300,
        source_priority=1.6,
        score=80.0,
    )
    picked = _pick_winner(store=_Store(), candidates=[kobeissi, fiscal])
    assert picked is not None
    assert picked.source_id == "KobeissiLetter"


def test_cli_run_post_url_command(monkeypatch, capsys) -> None:
    called: dict[str, object] = {}

    def _fake_run(*, post_url: str, channel_override: str | None = None) -> dict[str, object]:
        called["post_url"] = post_url
        called["channel_override"] = channel_override
        return {"ok": True, "posted": True, "winner": {"url": post_url}, "channel": channel_override or "default"}

    monkeypatch.setattr("coatue_claw.x_chart_daily.run_chart_for_post_url", _fake_run)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "coatue-claw-x-chart-daily",
            "run-post-url",
            "https://x.com/oguzerkan/status/2024447368137994460",
            "--channel",
            "C123",
        ],
    )
    main()
    output = capsys.readouterr().out
    payload = json.loads(output)
    assert payload["ok"] is True
    assert called["post_url"] == "https://x.com/oguzerkan/status/2024447368137994460"
    assert called["channel_override"] == "C123"


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
    assert "breaking" not in draft.headline.lower()
    assert draft.chart_label
    assert len(draft.headline) <= 72
    assert len(draft.takeaway) <= 68
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
    monkeypatch.setattr("coatue_claw.x_chart_daily._write_source_chart_image", lambda **kwargs: styled)
    monkeypatch.setattr("coatue_claw.x_chart_daily._render_source_snip_card", lambda **kwargs: styled)

    result = _post_winner_to_slack(candidate=candidate, channel="C123", slot_key="manual-1", windows_text="09:00,12:00,18:00")
    assert result["ok"] is True
    assert result["channel"] == "C123"
    assert result["file_id"] == "F123"
    assert len(upload_calls) == 1
    assert upload_calls[0]["channel"] == "C123"
    assert upload_calls[0]["file"] == str(styled)
    assert "initial_comment" in upload_calls[0]
    assert "Chart label:" not in str(upload_calls[0]["initial_comment"])
    assert "thread_ts" not in upload_calls[0]
    assert "post_publish_review" in result


def test_shorten_without_ellipsis_removes_three_dots() -> None:
    text = "Non-asset owners are being left behind: US consumer sentiment among non-stockholders keeps sliding"
    shortened = _shorten_without_ellipsis(text, max_chars=58)
    assert "..." not in shortened
    assert len(shortened) <= 58


def test_compute_y_ticks_non_normalized_has_multiple_ticks() -> None:
    ticks = _compute_y_ticks(y_min=0.0, y_max=1700.0, normalized=False)
    assert len(ticks) >= 4
    assert ticks[0] <= 0.0
    assert ticks[-1] >= 1700.0


def test_post_publish_checklist_passes_for_clean_rebuilt_chart(tmp_path: Path) -> None:
    candidate = Candidate(
        candidate_key="x:checklist",
        source_type="x",
        source_id="fiscal_AI",
        author="@fiscal_AI",
        title="US software growth re-accelerates",
        text="US software growth re-accelerates to 29% YoY.",
        url="https://x.com/fiscal_AI/status/checklist",
        image_url="https://example.com/chart.png",
        created_at=datetime.now(UTC).isoformat(),
        engagement=200,
        source_priority=1.6,
        score=90.0,
    )
    draft = _select_style_draft(candidate)
    styled = tmp_path / "styled.png"
    styled.write_bytes(b"x" * 30000)
    review = _post_publish_checklist(
        candidate=candidate,
        style_draft=draft,
        styled_path=styled,
        render_qa={
            "reconstruction_mode": "bar",
            "x_axis_labels_present": True,
            "y_axis_labels_present": True,
            "grouped_two_series": False,
        },
    )
    assert review["passed"] is True
    assert review["failed"] == []


def test_review_feedback_penalizes_failing_source(tmp_path: Path, monkeypatch) -> None:
    db = tmp_path / "x_chart.sqlite"
    monkeypatch.setenv("COATUE_CLAW_X_CHART_DB_PATH", str(db))
    store = XChartStore()
    store.upsert_source("badsource", priority=1.0, manual=True)
    before = {row["handle"]: float(row["priority"]) for row in store.list_sources(limit=200)}
    store.apply_review_feedback(source_id="badsource", passed=False, failed_checks=["x_axis_labels_present", "y_axis_labels_present"])
    after = {row["handle"]: float(row["priority"]) for row in store.list_sources(limit=200)}
    assert after["badsource"] < before["badsource"]


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
    assert len(rebuilt.labels) == len(rebuilt.values)
    assert not any(label.startswith("G") for label in rebuilt.labels)


def test_infer_bar_labels_from_text_uses_year_range() -> None:
    candidate = Candidate(
        candidate_key="x:years",
        source_type="x",
        source_id="KobeissiLetter",
        author="@KobeissiLetter",
        title="US ETF flows in first six weeks of year historically (2013-2026)",
        text="US ETF inflows surged in first six weeks of 2026. Versus 2025 and 2024.",
        url="https://x.com/KobeissiLetter/status/years",
        image_url="https://example.com/years.png",
        created_at=datetime.now(UTC).isoformat(),
        engagement=100,
        source_priority=1.2,
        score=90.0,
    )
    labels = _infer_bar_labels_from_text(candidate=candidate, count=14)
    assert labels[0] == "2013"
    assert labels[-1] == "2026"
    assert len(labels) == 14


def test_fallback_bar_labels_uses_created_at_year_when_no_explicit_year_range() -> None:
    candidate = Candidate(
        candidate_key="x:fallback-years",
        source_type="x",
        source_id="fiscal_AI",
        author="@fiscal_AI",
        title="US ETF inflows in first six weeks historically",
        text="US ETF inflows surged to a new record.",
        url="https://x.com/fiscal_AI/status/fallback-years",
        image_url="https://example.com/fallback-years.png",
        created_at="2026-02-19T00:00:00Z",
        engagement=80,
        source_priority=1.2,
        score=88.0,
    )
    labels = _fallback_bar_labels(candidate=candidate, count=10)
    assert labels[0] == "2017"
    assert labels[-1] == "2026"
    assert len(labels) == 10


def test_extract_rebuilt_bars_via_vision_parses_json(monkeypatch) -> None:
    candidate = Candidate(
        candidate_key="x:vision",
        source_type="x",
        source_id="fiscal_AI",
        author="@fiscal_AI",
        title="US ETF inflows",
        text="US ETF inflows in first six weeks of 2026",
        url="https://x.com/fiscal_AI/status/vision",
        image_url="https://example.com/vision.png",
        created_at=datetime.now(UTC).isoformat(),
        engagement=100,
        source_priority=1.6,
        score=90.0,
    )

    class FakeMessage:
        def __init__(self, content: str) -> None:
            self.content = content

    class FakeChoice:
        def __init__(self, content: str) -> None:
            self.message = FakeMessage(content)

    class FakeResponse:
        def __init__(self, content: str) -> None:
            self.choices = [FakeChoice(content)]

    class FakeCompletions:
        def create(self, **kwargs):
            assert kwargs["response_format"]["type"] == "json_object"
            payload = {
                "chart_type": "bar",
                "x_labels": ["2023", "2024", "2025", "2026"],
                "series": [
                    {"name": "Employees", "values": [1260, 1541, 1525, 1556]},
                    {"name": "Robots", "values": [500, 750, 750, 1000]},
                ],
                "y_label": "US$ Billions",
                "normalized": False,
                "confidence": 0.88,
            }
            import json

            return FakeResponse(json.dumps(payload))

    class FakeChat:
        def __init__(self) -> None:
            self.completions = FakeCompletions()

    class FakeOpenAI:
        def __init__(self, api_key: str) -> None:
            self.api_key = api_key
            self.chat = FakeChat()

    monkeypatch.setenv("OPENAI_API_KEY", "test-key")
    monkeypatch.setattr("coatue_claw.x_chart_daily.OpenAI", FakeOpenAI)
    monkeypatch.setattr("coatue_claw.x_chart_daily._fetch_image_bytes", lambda url: (b"img-bytes", "image/png"))
    rebuilt = _extract_rebuilt_bars_via_vision(candidate=candidate)
    assert rebuilt is not None
    assert rebuilt.source == "vision"
    assert rebuilt.labels[-1] == "2026"
    assert rebuilt.values[-1] == 1556
    assert rebuilt.secondary_values is not None
    assert rebuilt.secondary_values[-1] == 1000
    assert rebuilt.normalized is False


def test_extract_rebuilt_bars_via_vision_requires_grouped_for_employees_robots(monkeypatch) -> None:
    monkeypatch.setenv("COATUE_CLAW_X_CHART_VISION_ENABLED", "1")
    monkeypatch.setenv("OPENAI_API_KEY", "test-key")

    class _FakeChoice:
        def __init__(self, content: str) -> None:
            self.message = types.SimpleNamespace(content=content)

    class _FakeResponse:
        def __init__(self, content: str) -> None:
            self.choices = [_FakeChoice(content)]

    class _FakeCompletions:
        @staticmethod
        def create(**kwargs):  # noqa: ANN003
            payload = {
                "chart_type": "bar",
                "x_labels": ["2022", "2023", "2024", "2025"],
                "values": [520, 750, 750, 1000],
                "y_label": "Number in Thousands",
                "normalized": False,
                "confidence": 0.9,
            }
            return _FakeResponse(json.dumps(payload))

    class _FakeChat:
        completions = _FakeCompletions()

    class _FakeOpenAI:
        def __init__(self, api_key: str) -> None:
            self.chat = _FakeChat()

    monkeypatch.setattr("coatue_claw.x_chart_daily.OpenAI", _FakeOpenAI)
    monkeypatch.setattr("coatue_claw.x_chart_daily._fetch_image_bytes", lambda _url: (b"png", "image/png"))

    candidate = Candidate(
        candidate_key="x:robots-single",
        source_type="x",
        source_id="oguzerkan",
        author="@oguzerkan",
        title="$AMZN employees and robots",
        text="$AMZN has 1.5 million employees and deployed 1 million robots.",
        url="https://x.com/oguzerkan/status/1",
        image_url="https://example.com/chart.png",
        created_at=datetime.now(UTC).isoformat(),
        engagement=400,
        source_priority=1.0,
        score=80.0,
    )
    rebuilt = _extract_rebuilt_bars_via_vision(candidate=candidate)
    assert rebuilt is None


def test_extract_rebuilt_bars_prefers_grouped_cv_for_employee_robot_chart(monkeypatch) -> None:
    import numpy as np

    candidate = Candidate(
        candidate_key="x:robots-cv",
        source_type="x",
        source_id="oguzerkan",
        author="@oguzerkan",
        title="$AMZN has 1.5 million employees and deployed 1 million robots.",
        text="$AMZN has 1.5 million employees and deployed 1 million robots.",
        url="https://x.com/oguzerkan/status/1",
        image_url="https://example.com/chart.png",
        created_at=datetime.now(UTC).isoformat(),
        engagement=400,
        source_priority=1.0,
        score=80.0,
    )
    grouped = RebuiltBars(
        labels=["2022", "2023", "2024", "2025"],
        values=[1608.0, 1541.0, 1525.0, 1556.0],
        color="#1F2452",
        y_label="Number (thousands)",
        normalized=False,
        source="cv",
        confidence=0.64,
        primary_label="Employees",
        secondary_values=[520.0, 750.0, 750.0, 1000.0],
        secondary_color="#6D63E7",
        secondary_label="Robots",
    )
    monkeypatch.setattr("coatue_claw.x_chart_daily._extract_employees_robots_bars_cv", lambda **kwargs: grouped)
    image = np.zeros((600, 1000, 3), dtype=float)
    rebuilt = _extract_rebuilt_bars(image=image, candidate=candidate, allow_vision=False)
    assert rebuilt is grouped


def test_run_chart_for_post_url_posts_specific_tweet(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setenv("COATUE_CLAW_X_CHART_DB_PATH", str(tmp_path / "db.sqlite"))
    monkeypatch.setenv("COATUE_CLAW_X_CHART_SLACK_CHANNEL", "C123")
    monkeypatch.setattr("coatue_claw.x_chart_daily._resolve_bearer_token", lambda: "test-token")

    payload = {
        "data": [
            {
                "id": "2024543034734768600",
                "author_id": "u1",
                "text": "BREAKING: US ETF inflows surged +94% YoY in first six weeks of 2026.",
                "created_at": "2026-02-19T00:00:00Z",
                "public_metrics": {"like_count": 10, "retweet_count": 5, "reply_count": 2, "quote_count": 1},
                "attachments": {"media_keys": ["m1"]},
            }
        ],
        "includes": {
            "users": [{"id": "u1", "username": "KobeissiLetter"}],
            "media": [{"media_key": "m1", "type": "photo", "url": "https://example.com/chart.png"}],
        },
    }

    monkeypatch.setattr("coatue_claw.x_chart_daily._http_json", lambda **kwargs: payload)
    monkeypatch.setattr(
        "coatue_claw.x_chart_daily._extract_rebuilt_bars_via_vision",
        lambda **kwargs: RebuiltBars(
            labels=["2023", "2024", "2025", "2026"],
            values=[44.0, 55.0, 126.0, 245.0],
            color="#2F6ABF",
            y_label="US$ Billions",
            normalized=False,
            source="vision",
            confidence=0.9,
        ),
    )

    captured: dict[str, object] = {}

    def _fake_post(**kwargs):
        captured["candidate_url"] = kwargs["candidate"].url
        captured["channel"] = kwargs["channel"]
        return {"ok": True, "channel": kwargs["channel"], "styled_artifact": str(tmp_path / "styled.png")}

    monkeypatch.setattr("coatue_claw.x_chart_daily._post_winner_to_slack", _fake_post)
    result = run_chart_for_post_url(
        post_url="https://x.com/KobeissiLetter/status/2024543034734768600",
        channel_override="C123",
    )
    assert result["ok"] is True
    assert result["posted"] is True
    assert captured["candidate_url"] == "https://x.com/KobeissiLetter/status/2024543034734768600"
    assert captured["channel"] == "C123"


def test_run_chart_for_post_url_uses_vxtwitter_fallback(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setenv("COATUE_CLAW_X_CHART_DB_PATH", str(tmp_path / "db.sqlite"))
    monkeypatch.setenv("COATUE_CLAW_X_CHART_SLACK_CHANNEL", "C123")
    monkeypatch.setattr("coatue_claw.x_chart_daily._resolve_bearer_token", lambda: "test-token")
    monkeypatch.setattr("coatue_claw.x_chart_daily._http_json", lambda **kwargs: {"data": [], "includes": {}})
    fallback_candidate = Candidate(
        candidate_key="x:2024447368137994460",
        source_type="x",
        source_id="oguzerkan",
        author="@oguzerkan",
        title="Amazon employees and robots",
        text="$AMZN has 1.5 million employees and deployed 1 million robots.",
        url="https://x.com/oguzerkan/status/2024447368137994460",
        image_url="https://pbs.twimg.com/media/HBhIJlNXQAE4nDw.jpg",
        created_at="2026-02-19T00:00:00Z",
        engagement=500,
        source_priority=1.0,
        score=80.0,
    )
    monkeypatch.setattr(
        "coatue_claw.x_chart_daily._fetch_vxtwitter_post_candidate",
        lambda **kwargs: fallback_candidate,
    )
    monkeypatch.setattr(
        "coatue_claw.x_chart_daily._extract_rebuilt_bars_via_vision",
        lambda **kwargs: RebuiltBars(
            labels=["2018", "2019", "2020", "2021", "2022", "2023", "2024", "2025"],
            values=[100.0, 154.0, 644.0, 798.0, 1298.0, 1608.0, 1541.0, 1556.0],
            color="#2F6ABF",
            y_label="Employees (k)",
            normalized=False,
            source="vision",
            confidence=0.88,
        ),
    )

    captured: dict[str, object] = {}

    def _fake_post(**kwargs):
        captured["candidate_url"] = kwargs["candidate"].url
        captured["source_id"] = kwargs["candidate"].source_id
        return {"ok": True, "channel": kwargs["channel"], "styled_artifact": str(tmp_path / "styled.png")}

    monkeypatch.setattr("coatue_claw.x_chart_daily._post_winner_to_slack", _fake_post)
    result = run_chart_for_post_url(
        post_url="https://x.com/oguzerkan/status/2024447368137994460",
        channel_override="C123",
    )
    assert result["ok"] is True
    assert result["posted"] is True
    assert captured["candidate_url"] == "https://x.com/oguzerkan/status/2024447368137994460"
    assert captured["source_id"] == "oguzerkan"


def test_post_winner_does_not_require_rebuild(monkeypatch, tmp_path: Path) -> None:
    candidate = Candidate(
        candidate_key="x:no-rebuild",
        source_type="x",
        source_id="fiscal_AI",
        author="@fiscal_AI",
        title="US trend chart",
        text="US trend chart",
        url="https://x.com/fiscal_AI/status/no-rebuild",
        image_url="https://example.com/no-image.png",
        created_at=datetime.now(UTC).isoformat(),
        engagement=10,
        source_priority=1.0,
        score=50.0,
    )

    source_path = tmp_path / "source.png"
    source_path.write_bytes(b"chart")
    upload_calls: list[dict[str, object]] = []

    class FakeWebClient:
        def __init__(self, token: str) -> None:
            self.token = token

        def files_upload_v2(self, **kwargs):
            upload_calls.append(kwargs)
            return {"ok": True, "file": {"id": "F111"}}

    class FakeSlackApiError(Exception):
        def __init__(self, error: str) -> None:
            self.response = {"error": error}
            super().__init__(error)

    monkeypatch.setitem(sys.modules, "slack_sdk", types.SimpleNamespace(WebClient=FakeWebClient))
    monkeypatch.setitem(sys.modules, "slack_sdk.errors", types.SimpleNamespace(SlackApiError=FakeSlackApiError))
    monkeypatch.setattr("coatue_claw.x_chart_daily._slack_tokens", lambda: ["xoxb-test"])
    monkeypatch.setattr("coatue_claw.x_chart_daily._write_source_chart_image", lambda **kwargs: source_path)
    monkeypatch.setattr("coatue_claw.x_chart_daily._render_source_snip_card", lambda **kwargs: source_path)

    result = _post_winner_to_slack(candidate=candidate, channel="C123", slot_key="manual-2", windows_text="09:00,12:00,18:00")
    assert result["ok"] is True
    assert result["file_id"] == "F111"
    assert len(upload_calls) == 1


def test_render_chart_rejects_screenshot_fallback_even_if_env_disabled(monkeypatch, tmp_path: Path) -> None:
    import numpy as np
    import pytest

    monkeypatch.setenv("COATUE_CLAW_X_CHART_REQUIRE_REBUILD", "0")
    monkeypatch.setenv("COATUE_CLAW_DATA_ROOT", str(tmp_path))

    candidate = Candidate(
        candidate_key="x:no-fallback",
        source_type="x",
        source_id="fiscal_AI",
        author="@fiscal_AI",
        title="US trend chart",
        text="US trend chart",
        url="https://x.com/fiscal_AI/status/no-fallback",
        image_url="https://example.com/chart.png",
        created_at=datetime.now(UTC).isoformat(),
        engagement=10,
        source_priority=1.0,
        score=50.0,
    )
    style = _select_style_draft(candidate)
    monkeypatch.setattr("coatue_claw.x_chart_daily._safe_image_from_url", lambda _url: np.zeros((120, 200, 3), dtype=float))
    monkeypatch.setattr("coatue_claw.x_chart_daily._extract_rebuilt_bars_via_vision", lambda **kwargs: None)
    monkeypatch.setattr("coatue_claw.x_chart_daily._infer_chart_mode", lambda **kwargs: "line")
    monkeypatch.setattr("coatue_claw.x_chart_daily._extract_rebuilt_series", lambda **kwargs: [])

    with pytest.raises(XChartError):
        _render_chart_of_day_style(
            candidate=candidate,
            slot_key="manual-no-fallback",
            windows_text="09:00,12:00,18:00",
            style_draft=style,
        )


def test_render_chart_rejects_single_series_for_employees_robots(monkeypatch, tmp_path: Path) -> None:
    import numpy as np
    import pytest

    monkeypatch.setenv("COATUE_CLAW_DATA_ROOT", str(tmp_path))
    candidate = Candidate(
        candidate_key="x:robots-single-series",
        source_type="x",
        source_id="oguzerkan",
        author="@oguzerkan",
        title="$AMZN has 1.5 million employees and deployed 1 million robots.",
        text="$AMZN has 1.5 million employees and deployed 1 million robots.",
        url="https://x.com/oguzerkan/status/2024447368137994460",
        image_url="https://example.com/chart.png",
        created_at=datetime.now(UTC).isoformat(),
        engagement=10,
        source_priority=1.0,
        score=50.0,
    )
    style = _select_style_draft(candidate)
    monkeypatch.setattr("coatue_claw.x_chart_daily._safe_image_from_url", lambda _url: np.zeros((500, 900, 3), dtype=float))
    monkeypatch.setattr(
        "coatue_claw.x_chart_daily._extract_rebuilt_bars_via_vision",
        lambda **kwargs: RebuiltBars(
            labels=["2022", "2023", "2024", "2025"],
            values=[520.0, 750.0, 750.0, 1000.0],
            color="#2F6ABF",
            y_label="Number (thousands)",
            normalized=False,
            source="vision",
            confidence=0.9,
        ),
    )
    with pytest.raises(XChartError):
        _render_chart_of_day_style(
            candidate=candidate,
            slot_key="manual-robots-single",
            windows_text="09:00,12:00,18:00",
            style_draft=style,
        )


def test_render_chart_rejects_missing_y_axis_tick_labels(monkeypatch, tmp_path: Path) -> None:
    import numpy as np
    import pytest

    monkeypatch.setenv("COATUE_CLAW_DATA_ROOT", str(tmp_path))
    candidate = Candidate(
        candidate_key="x:robots-no-yticks",
        source_type="x",
        source_id="oguzerkan",
        author="@oguzerkan",
        title="$AMZN has 1.5 million employees and deployed 1 million robots.",
        text="$AMZN has 1.5 million employees and deployed 1 million robots.",
        url="https://x.com/oguzerkan/status/2024447368137994460",
        image_url="https://example.com/chart.png",
        created_at=datetime.now(UTC).isoformat(),
        engagement=10,
        source_priority=1.0,
        score=50.0,
    )
    style = _select_style_draft(candidate)
    monkeypatch.setattr("coatue_claw.x_chart_daily._safe_image_from_url", lambda _url: np.zeros((500, 900, 3), dtype=float))
    monkeypatch.setattr(
        "coatue_claw.x_chart_daily._extract_rebuilt_bars_via_vision",
        lambda **kwargs: RebuiltBars(
            labels=["2016", "2017", "2018", "2019", "2020", "2021", "2022", "2023", "2024", "2025"],
            values=[117.0, 154.0, 341.0, 648.0, 798.0, 1298.0, 1608.0, 1541.0, 1525.0, 1556.0],
            color="#1F2452",
            y_label="Number (thousands)",
            normalized=False,
            source="vision",
            confidence=0.9,
            primary_label="Employees",
            secondary_values=[15.0, 30.0, 100.0, 200.0, 265.0, 350.0, 520.0, 750.0, 750.0, 1000.0],
            secondary_color="#6D63E7",
            secondary_label="Robots",
        ),
    )
    monkeypatch.setattr("coatue_claw.x_chart_daily._compute_y_ticks", lambda **kwargs: [])

    with pytest.raises(XChartError):
        _render_chart_of_day_style(
            candidate=candidate,
            slot_key="manual-robots-no-yticks",
            windows_text="09:00,12:00,18:00",
            style_draft=style,
        )


def test_style_draft_generates_narrative_title_and_small_label_for_etf_flow() -> None:
    candidate = Candidate(
        candidate_key="x:etf",
        source_type="x",
        source_id="KobeissiLetter",
        author="@KobeissiLetter",
        title="@KobeissiLetter: BREAKING: US ETF inflows surged +94% YoY in first six weeks of 2026",
        text="BREAKING: US ETF inflows surged +94% YoY in first six weeks of 2026 to a record $245 billion.",
        url="https://x.com/KobeissiLetter/status/2024543034734768600",
        image_url="https://pbs.twimg.com/media/HBheTMkWwAA7APy.jpg",
        created_at=datetime.now(UTC).isoformat(),
        engagement=1000,
        source_priority=1.2,
        score=90.0,
    )
    draft = _select_style_draft(candidate)
    assert "breaking" not in draft.headline.lower()
    assert "etf" in draft.chart_label.lower()
    assert "..." not in draft.headline


def test_style_draft_employees_vs_robots_titles_are_narrative() -> None:
    candidate = Candidate(
        candidate_key="x:robots",
        source_type="x",
        source_id="oguzerkan",
        author="@oguzerkan",
        title="$AMZN has 1.5 million employees and deployed 1 million robots.",
        text=(
            "$AMZN has 1.5 million employees and deployed 1 million robots. "
            "It's actively replacing humans with robots as human/robot ratio declined from 3 in 2020 to 1.5 in 2025."
        ),
        url="https://x.com/oguzerkan/status/2024447368137994460",
        image_url="https://pbs.twimg.com/media/HBhIJlNXQAE4nDw.jpg",
        created_at=datetime.now(UTC).isoformat(),
        engagement=700,
        source_priority=1.0,
        score=80.0,
    )
    draft = _select_style_draft(candidate)
    assert "robots" in draft.headline.lower() or "automation" in draft.headline.lower()
    assert "employees vs robots" in draft.chart_label.lower()
    assert "robots deployed" in draft.takeaway.lower()
    assert "..." not in draft.headline


def test_style_draft_rewrites_low_signal_tariff_title() -> None:
    candidate = Candidate(
        candidate_key="x:tariff",
        source_type="x",
        source_id="KobeissiLetter",
        author="@KobeissiLetter",
        title="@KobeissiLetter: It's official: In one of the most anticipated rulings in decades...",
        text=(
            "It's official: In one of the most anticipated rulings in decades, "
            "the Supreme Court ruling impacts tariff refunds while US customs duties are at records."
        ),
        url="https://x.com/KobeissiLetter/status/2024887690093572404",
        image_url="https://pbs.twimg.com/media/tariff.png",
        created_at=datetime.now(UTC).isoformat(),
        engagement=900,
        source_priority=1.2,
        score=90.0,
    )
    draft = _select_style_draft(candidate)
    assert "it's official" not in draft.headline.lower()
    assert "anticipated rulings" not in draft.headline.lower()
    assert draft.headline.lower().startswith("us tariff")
    assert len(draft.headline) <= 48
    assert draft.headline.split(" ")[-1].lower() not in {"in", "of", "the", "to"}


def test_style_draft_uses_chart_hint_for_low_signal_copy(monkeypatch) -> None:
    candidate = Candidate(
        candidate_key="x:tariff-hint",
        source_type="x",
        source_id="KobeissiLetter",
        author="@KobeissiLetter",
        title="@KobeissiLetter: It's official: In one of the most anticipated rulings in decades...",
        text="It's official: In one of the most anticipated rulings in decades...",
        url="https://x.com/KobeissiLetter/status/2024887690093572404",
        image_url="https://pbs.twimg.com/media/tariff.png",
        created_at=datetime.now(UTC).isoformat(),
        engagement=900,
        source_priority=1.2,
        score=90.0,
    )
    monkeypatch.setattr(
        "coatue_claw.x_chart_daily._extract_chart_title_hint_via_vision",
        lambda _candidate: "The US Tariff Take Has Surged",
    )
    draft = _select_style_draft(candidate)
    assert draft.headline == "US tariff receipts are surging"
    assert draft.chart_label == "Monthly US customs duties (US$B)"
    assert draft.takeaway == "US customs-duty collections just hit a new high."


def test_style_draft_rewrites_low_signal_takeaway_even_if_headline_is_good(monkeypatch) -> None:
    candidate = Candidate(
        candidate_key="x:tariff-llm",
        source_type="x",
        source_id="KobeissiLetter",
        author="@KobeissiLetter",
        title="@KobeissiLetter: It's official: In one of the most anticipated rulings in decades...",
        text="It's official: In one of the most anticipated rulings in decades...",
        url="https://x.com/KobeissiLetter/status/2024887690093572404",
        image_url="https://pbs.twimg.com/media/tariff.png",
        created_at=datetime.now(UTC).isoformat(),
        engagement=900,
        source_priority=1.2,
        score=90.0,
    )
    monkeypatch.setattr(
        "coatue_claw.x_chart_daily._synthesize_style_via_llm",
        lambda _candidate: {
            "headline": "US tariff receipts are surging",
            "chart_label": "Monthly US customs duties (US$B)",
            "takeaway": "It's official: In one of the most anticipated rulings",
        },
    )
    monkeypatch.setattr(
        "coatue_claw.x_chart_daily._extract_chart_title_hint_via_vision",
        lambda _candidate: "The US Tariff Take Has Surged",
    )
    draft = _select_style_draft(candidate)
    assert draft.headline == "US tariff receipts are surging"
    assert draft.takeaway == "US customs-duty collections just hit a new high."


def test_style_draft_rewrites_low_signal_takeaway_from_headline_context(monkeypatch) -> None:
    candidate = Candidate(
        candidate_key="x:tariff-ctx",
        source_type="x",
        source_id="KobeissiLetter",
        author="@KobeissiLetter",
        title="@KobeissiLetter: It's official: In one of the most anticipated rulings in decades...",
        text="It's official: In one of the most anticipated rulings in decades...",
        url="https://x.com/KobeissiLetter/status/2024887690093572404",
        image_url="https://pbs.twimg.com/media/tariff.png",
        created_at=datetime.now(UTC).isoformat(),
        engagement=900,
        source_priority=1.2,
        score=90.0,
    )
    monkeypatch.setattr(
        "coatue_claw.x_chart_daily._extract_chart_title_hint_via_vision",
        lambda _candidate: None,
    )
    monkeypatch.setattr(
        "coatue_claw.x_chart_daily._synthesize_style_via_llm",
        lambda _candidate: {
            "headline": "US tariff receipts are surging",
            "chart_label": "Monthly US customs duties (US$B)",
            "takeaway": "It's official: In one of the most anticipated rulings",
        },
    )
    draft = _select_style_draft(candidate)
    assert draft.takeaway == "US customs-duty collections just hit a new high."


def test_style_draft_rewrites_incoherent_institutional_selling_headline(monkeypatch) -> None:
    candidate = Candidate(
        candidate_key="x:inst-sellers",
        source_type="x",
        source_id="KobeissiLetter",
        author="@KobeissiLetter",
        title="@KobeissiLetter: Institutional investors sold a net -$8.3 billion of US equities last week",
        text="Exhibit 12: Institutional clients were the biggest net sellers last week.",
        url="https://x.com/KobeissiLetter/status/2024990952742682803",
        image_url="https://pbs.twimg.com/media/inst-sellers.png",
        created_at=datetime.now(UTC).isoformat(),
        engagement=900,
        source_priority=1.2,
        score=90.0,
    )
    monkeypatch.setattr(
        "coatue_claw.x_chart_daily._synthesize_style_via_llm",
        lambda _candidate: {
            "headline": "Institutional investors sold a are at an extreme",
            "chart_label": "US institutional net buying (selling) (US$M)",
            "takeaway": "Institutional investors sold a net -$8.3 billion of US equities.",
        },
    )
    draft = _select_style_draft(candidate)
    assert draft.headline == "Institutional selling is at an extreme"
    assert "sold a are" not in draft.headline.lower()
    assert draft.checks["headline_grammar"] is True
