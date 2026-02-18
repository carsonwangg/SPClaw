from __future__ import annotations

from pathlib import Path

from coatue_claw.runtime_settings import (
    RuntimeSettings,
    format_settings_summary,
    load_runtime_settings,
    runtime_settings_path,
    update_runtime_setting,
)


def _configure_tmp_paths(tmp_path: Path, monkeypatch):
    data_root = tmp_path / "data"
    repo_root = tmp_path / "repo"
    repo_root.mkdir(parents=True)
    monkeypatch.setenv("COATUE_CLAW_DATA_ROOT", str(data_root))
    monkeypatch.setenv("COATUE_CLAW_REPO_PATH", str(repo_root))
    monkeypatch.setenv("COATUE_CLAW_REPO_DEFAULTS_PATH", str(repo_root / "config/runtime-defaults.json"))


def test_load_runtime_settings_defaults(tmp_path: Path, monkeypatch):
    _configure_tmp_paths(tmp_path, monkeypatch)
    settings = load_runtime_settings()
    assert isinstance(settings, RuntimeSettings)
    assert settings.default_x_metric == "ev_ltm_revenue"
    assert settings.default_y_metric == "yoy_revenue_growth_pct"
    assert settings.peer_discovery_limit == 8


def test_update_runtime_setting_persists_and_audits(tmp_path: Path, monkeypatch):
    _configure_tmp_paths(tmp_path, monkeypatch)

    settings, audit_path = update_runtime_setting(
        key="peer_discovery_limit",
        value=12,
        actor="U123",
        source_text="going forward please look for 12 peers",
    )

    assert settings.peer_discovery_limit == 12
    assert runtime_settings_path().exists()
    assert audit_path.exists()
    audit_text = audit_path.read_text(encoding="utf-8")
    assert "peer_discovery_limit" in audit_text


def test_format_settings_summary(tmp_path: Path, monkeypatch):
    _configure_tmp_paths(tmp_path, monkeypatch)
    settings, _ = update_runtime_setting(
        key="default_x_metric",
        value="market_cap",
        actor="U123",
        source_text="use market cap as default x-axis",
    )
    summary = format_settings_summary(settings)
    assert "Default x-axis" in summary
    assert "market_cap" in summary
