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
    assert x_chart["RunAtLoad"] is False
    assert x_chart["StartCalendarInterval"] == [{"Hour": 9, "Minute": 0}, {"Hour": 12, "Minute": 0}, {"Hour": 18, "Minute": 0}]

    spencer = specs[launchd_runtime.SPENCER_CHANGE_DIGEST_LABEL]
    assert spencer["ProgramArguments"] == ["/tmp/python", "-m", "coatue_claw.spencer_change_digest", "run-once"]
    assert spencer["RunAtLoad"] is False
    assert spencer["StartCalendarInterval"] == [{"Hour": 18, "Minute": 0}]


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
    ]
    assert launchd_runtime._resolve_services("email") == [launchd_runtime.EMAIL_LABEL]
    assert launchd_runtime._resolve_services("memory") == [launchd_runtime.MEMORY_PRUNE_LABEL]
    assert launchd_runtime._resolve_services("xchart") == [launchd_runtime.X_CHART_LABEL]
    assert launchd_runtime._resolve_services("spencer") == [launchd_runtime.SPENCER_CHANGE_DIGEST_LABEL]


def test_launchctl_domains(monkeypatch) -> None:
    monkeypatch.delenv("COATUE_CLAW_LAUNCHCTL_DOMAIN", raising=False)
    domains = launchd_runtime._launchctl_domains()
    assert len(domains) == 2
    assert domains[0].startswith("gui/")
    assert domains[1].startswith("user/")

    monkeypatch.setenv("COATUE_CLAW_LAUNCHCTL_DOMAIN", "user/501")
    assert launchd_runtime._launchctl_domains() == ["user/501"]
