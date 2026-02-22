from __future__ import annotations

from pathlib import Path
import plistlib

from coatue_claw import launchd_runtime


def test_service_specs_build_expected_commands(tmp_path: Path, monkeypatch) -> None:
    repo = tmp_path / "repo"
    data = tmp_path / "data"
    repo.mkdir()
    data.mkdir()
    monkeypatch.setenv("COATUE_CLAW_REPO_ROOT", str(repo))
    monkeypatch.setenv("COATUE_CLAW_DATA_ROOT", str(data))
    monkeypatch.setenv("COATUE_CLAW_PYTHON_BIN", "/tmp/python")
    monkeypatch.setenv("COATUE_CLAW_MEMORY_PRUNE_INTERVAL_SECONDS", "1800")

    specs = launchd_runtime._service_specs()
    email = specs[launchd_runtime.EMAIL_LABEL]
    assert email["ProgramArguments"] == ["/tmp/python", "-m", "coatue_claw.email_gateway", "serve"]
    assert email["KeepAlive"] is True
    assert email["RunAtLoad"] is True
    assert email["EnvironmentVariables"]["PYTHONPATH"] == str(repo / "src")

    prune = specs[launchd_runtime.MEMORY_PRUNE_LABEL]
    assert prune["ProgramArguments"] == ["/tmp/python", "-m", "coatue_claw.cli", "memory", "prune"]
    assert prune["StartInterval"] == 1800
    assert prune["RunAtLoad"] is True

    x_chart = specs[launchd_runtime.X_CHART_LABEL]
    assert x_chart["ProgramArguments"] == ["/tmp/python", "-m", "coatue_claw.x_chart_daily", "run-once"]
    assert x_chart["RunAtLoad"] is True
    assert x_chart["StartInterval"] == 3600

    spencer = specs[launchd_runtime.SPENCER_CHANGE_DIGEST_LABEL]
    assert spencer["ProgramArguments"] == ["/tmp/python", "-m", "coatue_claw.spencer_change_digest", "run-once"]
    assert spencer["RunAtLoad"] is False
    assert spencer["StartCalendarInterval"] == [{"Hour": 18, "Minute": 0}]

    board = specs[launchd_runtime.BOARD_SEAT_DAILY_LABEL]
    assert board["ProgramArguments"] == ["/tmp/python", "-m", "coatue_claw.board_seat_daily", "run-once"]
    assert board["RunAtLoad"] is False
    assert board["StartCalendarInterval"] == [{"Hour": 8, "Minute": 30}]

    md = specs[launchd_runtime.MARKET_DAILY_LABEL]
    assert md["ProgramArguments"] == ["/tmp/python", "-m", "coatue_claw.market_daily", "run-once"]
    assert md["RunAtLoad"] is False
    assert md["StartCalendarInterval"] == [
        {"Weekday": 1, "Hour": 7, "Minute": 0},
        {"Weekday": 1, "Hour": 14, "Minute": 15},
        {"Weekday": 2, "Hour": 7, "Minute": 0},
        {"Weekday": 2, "Hour": 14, "Minute": 15},
        {"Weekday": 3, "Hour": 7, "Minute": 0},
        {"Weekday": 3, "Hour": 14, "Minute": 15},
        {"Weekday": 4, "Hour": 7, "Minute": 0},
        {"Weekday": 4, "Hour": 14, "Minute": 15},
        {"Weekday": 5, "Hour": 7, "Minute": 0},
        {"Weekday": 5, "Hour": 14, "Minute": 15},
    ]


def test_write_service_plists(tmp_path: Path, monkeypatch) -> None:
    repo = tmp_path / "repo"
    data = tmp_path / "data"
    launch_agents = tmp_path / "launch-agents"
    repo.mkdir()
    data.mkdir()
    monkeypatch.setenv("COATUE_CLAW_REPO_ROOT", str(repo))
    monkeypatch.setenv("COATUE_CLAW_DATA_ROOT", str(data))
    monkeypatch.setenv("COATUE_CLAW_LAUNCHAGENTS_DIR", str(launch_agents))

    written = launchd_runtime.write_service_plists()
    assert set(written.keys()) == {
        launchd_runtime.EMAIL_LABEL,
        launchd_runtime.MEMORY_PRUNE_LABEL,
        launchd_runtime.X_CHART_LABEL,
        launchd_runtime.SPENCER_CHANGE_DIGEST_LABEL,
        launchd_runtime.BOARD_SEAT_DAILY_LABEL,
        launchd_runtime.MARKET_DAILY_LABEL,
    }
    for label, plist_path in written.items():
        path = Path(plist_path)
        assert path.exists()
        with path.open("rb") as f:
            payload = plistlib.load(f)
        assert payload["Label"] == label


def test_resolve_services() -> None:
    assert launchd_runtime._resolve_services("all") == [
        launchd_runtime.EMAIL_LABEL,
        launchd_runtime.MEMORY_PRUNE_LABEL,
        launchd_runtime.X_CHART_LABEL,
        launchd_runtime.SPENCER_CHANGE_DIGEST_LABEL,
        launchd_runtime.BOARD_SEAT_DAILY_LABEL,
        launchd_runtime.MARKET_DAILY_LABEL,
    ]
    assert launchd_runtime._resolve_services("email") == [launchd_runtime.EMAIL_LABEL]
    assert launchd_runtime._resolve_services("memory") == [launchd_runtime.MEMORY_PRUNE_LABEL]
    assert launchd_runtime._resolve_services("xchart") == [launchd_runtime.X_CHART_LABEL]
    assert launchd_runtime._resolve_services("spencer") == [launchd_runtime.SPENCER_CHANGE_DIGEST_LABEL]
    assert launchd_runtime._resolve_services("boardseat") == [launchd_runtime.BOARD_SEAT_DAILY_LABEL]
    assert launchd_runtime._resolve_services("marketdaily") == [launchd_runtime.MARKET_DAILY_LABEL]


def test_market_daily_schedule_env_override(monkeypatch) -> None:
    monkeypatch.setenv("COATUE_CLAW_MD_TIMES", "08:05,15:40")
    assert launchd_runtime._market_daily_schedule() == [
        {"Weekday": 1, "Hour": 8, "Minute": 5},
        {"Weekday": 1, "Hour": 15, "Minute": 40},
        {"Weekday": 2, "Hour": 8, "Minute": 5},
        {"Weekday": 2, "Hour": 15, "Minute": 40},
        {"Weekday": 3, "Hour": 8, "Minute": 5},
        {"Weekday": 3, "Hour": 15, "Minute": 40},
        {"Weekday": 4, "Hour": 8, "Minute": 5},
        {"Weekday": 4, "Hour": 15, "Minute": 40},
        {"Weekday": 5, "Hour": 8, "Minute": 5},
        {"Weekday": 5, "Hour": 15, "Minute": 40},
    ]


def test_launchctl_domains(monkeypatch) -> None:
    monkeypatch.delenv("COATUE_CLAW_LAUNCHCTL_DOMAIN", raising=False)
    domains = launchd_runtime._launchctl_domains()
    assert len(domains) == 2
    assert domains[0].startswith("gui/")
    assert domains[1].startswith("user/")

    monkeypatch.setenv("COATUE_CLAW_LAUNCHCTL_DOMAIN", "user/501")
    assert launchd_runtime._launchctl_domains() == ["user/501"]
