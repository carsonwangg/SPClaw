from __future__ import annotations

from pathlib import Path
import plistlib
import subprocess

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

    memory_reconcile = specs[launchd_runtime.MEMORY_RECONCILE_LABEL]
    assert memory_reconcile["ProgramArguments"] == [
        "/tmp/python",
        "-m",
        "coatue_claw.cli",
        "memory",
        "reconcile-export",
        "--limit",
        "200",
    ]
    assert memory_reconcile["StartInterval"] == 900
    assert memory_reconcile["RunAtLoad"] is True

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

    recap = specs[launchd_runtime.MARKET_DAILY_EARNINGS_RECAP_LABEL]
    assert recap["ProgramArguments"] == ["/tmp/python", "-m", "coatue_claw.market_daily", "run-earnings-recap"]
    assert recap["RunAtLoad"] is False
    assert recap["StartCalendarInterval"] == [
        {"Weekday": 1, "Hour": 19, "Minute": 0},
        {"Weekday": 2, "Hour": 19, "Minute": 0},
        {"Weekday": 3, "Hour": 19, "Minute": 0},
        {"Weekday": 4, "Hour": 19, "Minute": 0},
        {"Weekday": 5, "Hour": 19, "Minute": 0},
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
        launchd_runtime.MEMORY_RECONCILE_LABEL,
        launchd_runtime.X_CHART_LABEL,
        launchd_runtime.SPENCER_CHANGE_DIGEST_LABEL,
        launchd_runtime.BOARD_SEAT_DAILY_LABEL,
        launchd_runtime.MARKET_DAILY_LABEL,
        launchd_runtime.MARKET_DAILY_EARNINGS_RECAP_LABEL,
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
        launchd_runtime.MEMORY_RECONCILE_LABEL,
        launchd_runtime.X_CHART_LABEL,
        launchd_runtime.SPENCER_CHANGE_DIGEST_LABEL,
        launchd_runtime.BOARD_SEAT_DAILY_LABEL,
        launchd_runtime.MARKET_DAILY_LABEL,
        launchd_runtime.MARKET_DAILY_EARNINGS_RECAP_LABEL,
    ]
    assert launchd_runtime._resolve_services("email") == [launchd_runtime.EMAIL_LABEL]
    assert launchd_runtime._resolve_services("memory") == [launchd_runtime.MEMORY_PRUNE_LABEL]
    assert launchd_runtime._resolve_services("memoryreconcile") == [launchd_runtime.MEMORY_RECONCILE_LABEL]
    assert launchd_runtime._resolve_services("xchart") == [launchd_runtime.X_CHART_LABEL]
    assert launchd_runtime._resolve_services("spencer") == [launchd_runtime.SPENCER_CHANGE_DIGEST_LABEL]
    assert launchd_runtime._resolve_services("boardseat") == [launchd_runtime.BOARD_SEAT_DAILY_LABEL]
    assert launchd_runtime._resolve_services("marketdaily") == [
        launchd_runtime.MARKET_DAILY_LABEL,
        launchd_runtime.MARKET_DAILY_EARNINGS_RECAP_LABEL,
    ]


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


def test_market_daily_earnings_recap_schedule_env_override(monkeypatch) -> None:
    monkeypatch.setenv("COATUE_CLAW_MD_EARNINGS_RECAP_TIME", "20:10")
    assert launchd_runtime._market_daily_earnings_recap_schedule() == [
        {"Weekday": 1, "Hour": 20, "Minute": 10},
        {"Weekday": 2, "Hour": 20, "Minute": 10},
        {"Weekday": 3, "Hour": 20, "Minute": 10},
        {"Weekday": 4, "Hour": 20, "Minute": 10},
        {"Weekday": 5, "Hour": 20, "Minute": 10},
    ]


def test_launchctl_domains(monkeypatch) -> None:
    monkeypatch.delenv("COATUE_CLAW_LAUNCHCTL_DOMAIN", raising=False)
    domains = launchd_runtime._launchctl_domains()
    assert len(domains) == 2
    assert domains[0].startswith("gui/")
    assert domains[1].startswith("user/")

    monkeypatch.setenv("COATUE_CLAW_LAUNCHCTL_DOMAIN", "user/501")
    assert launchd_runtime._launchctl_domains() == ["user/501"]


def test_bootstrap_retries_transient_io_error(monkeypatch) -> None:
    calls: list[int] = []

    monkeypatch.setenv("COATUE_CLAW_LAUNCHCTL_DOMAIN", "gui/501")
    monkeypatch.setenv("COATUE_CLAW_LAUNCHCTL_BOOTSTRAP_RETRIES", "3")
    monkeypatch.setattr(launchd_runtime.time, "sleep", lambda _: None)

    def fake_run(cmd: list[str], *, check: bool = True) -> subprocess.CompletedProcess[str]:
        assert cmd[:2] == ["launchctl", "bootstrap"]
        calls.append(1)
        if len(calls) == 1:
            return subprocess.CompletedProcess(cmd, 5, "", "Bootstrap failed: 5: Input/output error")
        return subprocess.CompletedProcess(cmd, 0, "", "")

    monkeypatch.setattr(launchd_runtime, "_run", fake_run)
    domain = launchd_runtime._bootstrap("/tmp/fake.plist")
    assert domain == "gui/501"
    assert len(calls) == 2


def test_enable_services_error_includes_label(monkeypatch) -> None:
    monkeypatch.setattr(launchd_runtime, "write_service_plists", lambda: {launchd_runtime.EMAIL_LABEL: "/tmp/a.plist"})
    monkeypatch.setattr(launchd_runtime, "_bootout", lambda _: None)

    def fail_bootstrap(_: str) -> str:
        raise RuntimeError("bootstrap failed across launchctl domains: gui/501: boom")

    monkeypatch.setattr(launchd_runtime, "_bootstrap", fail_bootstrap)
    try:
        launchd_runtime.enable_services(services=[launchd_runtime.EMAIL_LABEL])
        raise AssertionError("expected RuntimeError")
    except RuntimeError as exc:
        assert "failed enabling com.coatueclaw.email-gateway" in str(exc)
