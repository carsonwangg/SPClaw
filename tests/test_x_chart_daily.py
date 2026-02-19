from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

from coatue_claw.x_chart_daily import Candidate, XChartStore, _parse_windows, _slack_tokens, run_chart_scout_once


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
                text="AI software demand trend chart",
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
